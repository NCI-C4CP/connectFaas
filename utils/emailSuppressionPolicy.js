/**
 * Email-address normalization used everywhere `emailAddressStatus` docs are read or written.
 * The result is the Firestore document id, so any read path (`isEmailSuppressed`, `getEmailSuppressions`)
 * must normalize the same way as the write path (`addEmailSuppression`, `buildEmailSuppressionDoc`, `scripts/backfillEmailSuppressions.js`).
 * Changing this function changes every existing doc id, so caution on changes here.
 *
 * Webhook processing, SendGrid API backfills, and send-time checks all need the same rules — keep the suppression policy table centralized.
 *
 * @param {string} email - The email address to normalize.
 * @returns {string} The normalized email address (trimmed, lowercased).
 */
const normalizeEmailAddress = (email = "") => (
  typeof email === "string" ? email.trim().toLowerCase() : ""
);

const normalizeSuppressionKey = (value = "") => (
  typeof value === "string" ? value.trim().toLowerCase().replace(/[\s-]+/g, "_") : ""
);

const EXACT_FILTERED_DOMAINS = new Set([
  "example.com",
  "example.org",
  "example.net",
  "episphere.github.io",
]);

// Some repeated TLDs found in production data
const REPEATED_TLD_PATTERNS = [
  ".com.com",
  ".org.org",
  ".net.net",
  ".gov.gov",
];

const normalizeLocalPartForFiltering = (localPart = "") => localPart.replace(/[^a-z0-9]/g, "");

const shouldFilterEmailAddress = (email = "") => {
  const normalizedEmail = normalizeEmailAddress(email);
  if (!normalizedEmail || !normalizedEmail.includes("@")) return true;

  const [localPart = "", domain = ""] = normalizedEmail.split("@");
  const normalizedLocalPart = normalizeLocalPartForFiltering(localPart);

  if (normalizedLocalPart.includes("noreply") || normalizedLocalPart.includes("donotreply")) {
    return true;
  }

  if (
    EXACT_FILTERED_DOMAINS.has(domain) ||
    domain === "localhost" ||
    domain.endsWith(".localhost") ||
    domain.endsWith(".local")
  ) {
    return true;
  }

  return REPEATED_TLD_PATTERNS.some((pattern) => domain.includes(pattern));
};

const EMAIL_SUPPRESSION_POLICIES_BY_REASON = Object.freeze({
  hard_bounce: Object.freeze({ reason: "hard_bounce", suppressBulk: true, suppressTransactional: true }),
  spam_report: Object.freeze({ reason: "spam_report", suppressBulk: true, suppressTransactional: true }),
  invalid_email: Object.freeze({ reason: "invalid_email", suppressBulk: true, suppressTransactional: true }),
  unsubscribed: Object.freeze({ reason: "unsubscribed", suppressBulk: true, suppressTransactional: false }),
  global_unsubscribe: Object.freeze({ reason: "global_unsubscribe", suppressBulk: true, suppressTransactional: true }),
  legacy_global_unsubscribe: Object.freeze({ reason: "legacy_global_unsubscribe", suppressBulk: true, suppressTransactional: true }),
  blocked: Object.freeze({ reason: "blocked", suppressBulk: false, suppressTransactional: false }),
});

const EMAIL_SUPPRESSION_IMPORT_TYPE_TO_REASON = Object.freeze({
  legacy_global_unsubscribes: "legacy_global_unsubscribe",
  global_unsubscribes: "global_unsubscribe",
  group_unsubscribes: "unsubscribed",
  bounces: "hard_bounce",
  spam_reports: "spam_report",
  invalid_emails: "invalid_email",
});

const normalizeSuppressionImportType = (type = "") => (
  typeof type === "string" ? type.trim().toLowerCase() : ""
);

const getEmailSuppressionPolicyByReason = (reason = "") => {
  return EMAIL_SUPPRESSION_POLICIES_BY_REASON[normalizeSuppressionKey(reason)] || null;
};

const getEmailSuppressionPolicyForSendGridEvent = (event = {}) => {
  const eventName = normalizeSuppressionKey(event.event);
  const eventType = normalizeSuppressionKey(event.type);

  // Interim rule: Before bulk sends are wired to SendGrid `asm.group_id`, global unsubscribe behavior still applies at the SendGrid account level.
  // Keep global unsubscribes as all-email suppressions. The manual HMAC route and future group-level unsubscribes are bulk-only.
  if (eventName === "bounce") {
    // Soft bounces with type "blocked" are telemetry-only. All other bounce events (including those with type missing, "bounce", or any unknown
    // future value SendGrid may add) are treated as hard bounces. Erring toward suppression here is safer than silently letting a real bounce fall through unrecorded.
    if (eventType === "blocked") {
      return EMAIL_SUPPRESSION_POLICIES_BY_REASON.blocked;
    }
    return EMAIL_SUPPRESSION_POLICIES_BY_REASON.hard_bounce;
  }
  if (eventName === "spamreport" || eventName === "spam_report") {
    return EMAIL_SUPPRESSION_POLICIES_BY_REASON.spam_report;
  }
  if (eventName === "dropped") {
    const reason = normalizeSuppressionKey(event.reason);
    // Precedence is intentional: ordered most-specific to least-specific:
    // A future SendGrid drop reason like "Bounced address - invalid format" contains both "bounce" and "invalid"; we want it to map to hard_bounce
    // (the actionable suppression) rather than invalid_email. Same logic for "Spam Reporting / Unsubscribed Address" combinations.
    if (reason.includes("bounce")) {
      return EMAIL_SUPPRESSION_POLICIES_BY_REASON.hard_bounce;
    }
    if (reason.includes("spam")) {
      return EMAIL_SUPPRESSION_POLICIES_BY_REASON.spam_report;
    }
    if (reason.includes("unsubscribe")) {
      return EMAIL_SUPPRESSION_POLICIES_BY_REASON.global_unsubscribe;
    }
    if (reason.includes("invalid")) {
      return EMAIL_SUPPRESSION_POLICIES_BY_REASON.invalid_email;
    }
    if (reason.includes("block")) {
      return EMAIL_SUPPRESSION_POLICIES_BY_REASON.blocked;
    }
  }

  if (eventName === "unsubscribe") {
    return EMAIL_SUPPRESSION_POLICIES_BY_REASON.global_unsubscribe;
  }

  if (eventName === "group_unsubscribe") {
    return EMAIL_SUPPRESSION_POLICIES_BY_REASON.unsubscribed;
  }
  if (eventName === "blocked") {
    return EMAIL_SUPPRESSION_POLICIES_BY_REASON.blocked;
  }

  return null;
};

const getEmailSuppressionPolicyForImportType = (type = "") => {
  const reason = EMAIL_SUPPRESSION_IMPORT_TYPE_TO_REASON[normalizeSuppressionImportType(type)];
  return reason ? EMAIL_SUPPRESSION_POLICIES_BY_REASON[reason] : null;
};

const buildEmailSuppressionDoc = ({
  email,
  normalizedEmail = normalizeEmailAddress(email),
  policy = null,
  reason = "",
  notificationId = null,
  token = "",
  suppressBulk = false,
  suppressTransactional = false,
  lastEventAt = new Date().toISOString(),
  manualOverride = false,
  status = "suppressed",
} = {}) => {
  const resolvedPolicy = policy || getEmailSuppressionPolicyByReason(reason) || {
    reason,
    suppressBulk: !!suppressBulk,
    suppressTransactional: !!suppressTransactional,
  };

  if (!normalizedEmail || !resolvedPolicy.reason || shouldFilterEmailAddress(normalizedEmail)) return null;

  const doc = {
    normalizedEmail,
    status,
    reason: resolvedPolicy.reason,
    lastEventAt,
    lastNotificationId: notificationId,
    manualOverride,
  };
  if (token) doc.token = token;
  if (resolvedPolicy.suppressBulk) doc.suppressBulk = true;
  if (resolvedPolicy.suppressTransactional) doc.suppressTransactional = true;

  return doc;
};

module.exports = {
  normalizeEmailAddress,
  normalizeSuppressionKey,
  shouldFilterEmailAddress,
  getEmailSuppressionPolicyByReason,
  getEmailSuppressionPolicyForSendGridEvent,
  getEmailSuppressionPolicyForImportType,
  buildEmailSuppressionDoc,
};
