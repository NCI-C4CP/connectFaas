const { v4: uuid } = require("uuid");
const crypto = require("crypto");
const sgMail = require("@sendgrid/mail");
const showdown = require("showdown");
const twilio = require("twilio");
const {SecretManagerServiceClient} = require('@google-cloud/secret-manager');
const sharedUtils = require("./shared");
const {getResponseJSON, setHeadersDomainRestricted, setHeaders, logIPAddress, redactEmailLoginInfo, redactPhoneLoginInfo, validEmailFormat, getTemplateForEmailLink, nihMailbox, getSecret, cidToLangMapper, unsubscribeTextObj, getAdjustedTime, getEasternDateKey, parseResponseJson, parseRequestBody, delay, backoffMs} = sharedUtils;
const { htmlToPlaintext } = require("./htmlToPlaintext");
const {getNotificationSpecById, getNotificationSpecByCategoryAndAttempt, getNotificationSpecsByScheduleOncePerDay, markNotificationSpecsQueuedForRun, clearNotificationSpecsQueuedRun, markNotificationSpecsLastRun, saveNotificationBatch, getNotificationRecordId, reserveNotificationBatch, markNotificationBatchProviderSendStarted, markNotificationBatchProviderAcceptanceUnknown, markNotificationBatchAccepted, markNotificationBatchFailed, generateSignInWithEmailLink, checkIsNotificationSent, updateSmsPermission, storeIncomingSmsData, getEmailSuppressions, isEmailSuppressed, getAppSettings, getBulkNotificationRun, saveBulkNotificationRunPlan, getBulkNotificationBatch, getBulkNotificationRunBatches, markBulkNotificationBatchEnqueued, markBulkNotificationRunEnqueueFailed, markBulkNotificationRunQueued, markBulkNotificationBatchRunning, markBulkNotificationBatchComplete, markBulkNotificationBatchFailed, finalizeBulkNotificationRunIfTerminal } = require("./firestore");
const {getParticipantsForNotificationsBQ, countParticipantsForNotificationsBQ, getTokensAndPreferredLanguageByPhone} = require("./bigquery");
const { normalizeEmailAddress, shouldFilterEmailAddress } = require("./emailSuppressionPolicy");
const { isProviderSendStartedState } = require("./notificationState");
const conceptIds = require("./fieldToConceptIdMapping");

const converter = new showdown.Converter();
const langArray = ["english", "spanish"];
let twilioClient, messagingServiceSid;
let isSendGridSetup = false;
let isTwilioSetup = false;
let twilioAuthToken = "";
// Per-instance guard. Cloud Functions Gen2 may spawn multiple instances, each with its own copy of this flag.
// Coordination across instances relies on three layers:
//    - (1) `sendScheduledNotifications` is deployed with `--ingress=internal` so only Cloud Scheduler can invoke it.
//    - (2) Cloud Scheduler is configured to fire once per day.
//    - (3) the per-recipient `notifications` doc state machine and the per-batch `notificationBulkRuns` queued markers prevent duplicate sends even if a second invocation slips through.
// per-recipient `notifications` doc state machine and the per-batch `notificationBulkRuns` queued markers prevent duplicate sends even if a
// second invocation slips through. This flag is just a fast-path no-op for duplicate invocations on the same instance.
let isSendingNotifications = false;
const BULK_LANE_DEFAULT = "default";        // Default bulk lane for non-Microsoft email domains.
const BULK_LANE_MICROSOFT = "microsoft";    // Bulk lane for Microsoft email domains (Outlook, Hotmail, Live, MSN). Separate lane due to deliverability issues.
const BULK_LANE_CONFIG = Object.freeze({
  [BULK_LANE_DEFAULT]: Object.freeze({
    queueName: "processNotificationBatchBulkDefault",
    batchSizeSetting: "bulkDefaultBatchSize",
  }),
  [BULK_LANE_MICROSOFT]: Object.freeze({
    queueName: "processNotificationBatchBulkMicrosoft",
    batchSizeSetting: "bulkMicrosoftBatchSize",
  }),
});
const BULK_TASK_DISPATCH_DEADLINE_SECONDS = 1800;
const BULK_WORKER_URI_SETTINGS_KEYS = Object.freeze({
  [BULK_LANE_DEFAULT]: "bulkWorkerUriDefault",
  [BULK_LANE_MICROSOFT]: "bulkWorkerUriMicrosoft",
});

// Validates a bulk worker URI before it's passed to Cloud Tasks.
const validateBulkWorkerUri = (uri, lane, source) => {
  if (typeof uri !== "string") {
    throw new Error(
      `Bulk worker URI for lane "${lane}" from ${source} must be a string. Got: ${typeof uri} (${JSON.stringify(uri)}).`,
    );
  }
  const trimmed = uri.trim();
  if (!trimmed) {
    throw new Error(`Bulk worker URI for lane "${lane}" from ${source} is empty.`);
  }
  let parsed;
  try {
    parsed = new URL(trimmed);
  } catch (err) {
    throw new Error(`Bulk worker URI for lane "${lane}" from ${source} is not a valid URL: "${uri}". ${err.message}`);
  }
  if (parsed.protocol !== "https:") {
    throw new Error(`Bulk worker URI for lane "${lane}" from ${source} must use https. Got: "${uri}".`);
  }
  if (!parsed.hostname.endsWith(".run.app")) {
    console.warn(`Bulk worker URI for lane "${lane}" from ${source} hostname "${parsed.hostname}" does not end with ".run.app". Verify this is the correct Cloud Run URL.`);
  }

  return trimmed;
};

const buildBulkWorkerUri = (lane, notificationSettings = {}) => {
  const envVarName = `BULK_WORKER_URI_${String(lane).toUpperCase()}`;
  const envOverride = process.env[envVarName];
  if (envOverride) {
    return validateBulkWorkerUri(envOverride, lane, `${envVarName} env var`);
  }

  const settingsKey = BULK_WORKER_URI_SETTINGS_KEYS[lane];
  if (!settingsKey) {
    throw new Error(
      `Unknown bulk lane: "${lane}". Expected one of: ${Object.keys(BULK_WORKER_URI_SETTINGS_KEYS).join(", ")}.`,
    );
  }
  const uri = notificationSettings[settingsKey];
  if (!uri) {
    const project = process.env.GCLOUD_PROJECT || "(GCLOUD_PROJECT unset)";
    const serviceName = `processnotificationbatchbulk${lane}`;
    throw new Error(
      `Bulk worker URI not configured for lane "${lane}". ` +
      `Expected appSettings.notifications.${settingsKey} (on the Firestore doc where appName == "connectFaas") ` +
      `to contain the Cloud Run URL for service "${serviceName}" in project "${project}". ` +
      `Look it up with: gcloud run services describe ${serviceName} --region=us-central1 --project=${project} --format='value(status.url)' ` +
      `and write the result into Firestore. See the "Pre-deploy: bulk worker URI configuration" section of the rollout doc.`,
    );
  }
  return validateBulkWorkerUri(uri, lane, `appSettings.notifications.${settingsKey}`);
};

// Firestore-visible attempt lock duration. Kept much shorter than the Cloud Tasks dispatch deadline so a crashed attempt's lock expires before
// the queue's max-attempts (5) × min-backoff (60s) retry budget is exhausted.
// A successful send completes in seconds. This value only needs to cover slow batches.
// Higher-retry-count attempts can also take over a still-locked batch via parseBulkTaskAttemptRetryCount.
const BULK_TASK_ATTEMPT_LOCK_MS = 10 * 60 * 1000;
const DEFAULT_NOTIFICATION_SETTINGS = Object.freeze({
  useCloudTasksBulk: false,
  sendgridDeliveryModeOverride: null,
  bulkMailCategories: Object.freeze(["newsletter", "eNewsletter", "anniversaryNewsletter"]),
  notificationBatchLimit: 1000,             // SendGrid caps a single batch at 1000 recipients, so configured values are clamped to that ceiling.
  notificationReservationMs: 30 * 60 * 1000,
  bulkTaskMaxAttempts: 5,                   // Keep this aligned with the Cloud Tasks queue retry policy for planned bulk handlers.
  bulkThreshold: 5000,                      // Threshold for upgrading to bulk mail stream. Different sending rules apply.
  bulkDefaultBatchSize: 100,                // Planned non-Microsoft bulk recipient count per Cloud Task.
  bulkMicrosoftBatchSize: 50,               // Planned Microsoft-family bulk recipient count per Cloud Task.
  bulkRunStaleAfterDays: 7,                 // Days after which an in-flight bulk run's `queuedBulkRunDateKey` is treated as abandoned, so the spec can fire again. Safety net for stuck runs; large multi-day mailings (200K+ on the Microsoft lane) can take 3-4 days legitimately, so the default leaves room.
  sendgridBulkAsmGroupId: null,             // Optional SendGrid ASM unsubscribe group id for bulk mail.
  sendgridBulkAsmGroupsToDisplay: [],       // Optional SendGrid ASM preference-page groups for bulk mail.
  targetRecipientsPerHour: 5000,            // Target recipients per hour for non-Microsoft email domains.
  targetRecipientsPerHourMicrosoft: 1500,   // Target recipients per hour for Microsoft email domains.
  microsoftBulkDomains: Object.freeze(["outlook.com", "hotmail.com", "live.com", "msn.com"]),
  sendRetryMax: 5,                          // Maximum number of retries for send operations.
  emailBatchDelayMs: 1000,                  // Delay between email batches to avoid overwhelming the API. Used for transactional mail stream.
  // Bulk Cloud Tasks worker dispatch URIs.
  // Gotcha warning: The Firebase Admin SDK auto-derives a URL
  // assuming the firebase-deploy pattern, but our workers are deployed via
  // `gcloud run deploy --function=` and live at Cloud Run URLs that include a
  // per-project hash. Populate these per environment in the Firestore appSettings
  // doc (appName == "connectFaas") with the output of:
  //   gcloud run services describe processnotificationbatchbulkdefault \
  //     --region=us-central1 --project=<PROJECT> --format='value(status.url)'
  // (and the equivalent for processnotificationbatchbulkmicrosoft).
  bulkWorkerUriDefault: null,
  bulkWorkerUriMicrosoft: null,
});
// HMAC signature for unsubscribe URLs prevents unauthorized suppression of arbitrary emails.
// Secret is resolved from Secret Manager via resolveUnsubscribeSecret() before use.
let unsubscribeSecretKey = null;

const resolveUnsubscribeSecret = async () => {
  if (unsubscribeSecretKey) return unsubscribeSecretKey;
  const secretName = process.env.GCLOUD_UNSUBSCRIBE_SECRET;
  if (!secretName) throw new Error("GCLOUD_UNSUBSCRIBE_SECRET env var is not set");
  unsubscribeSecretKey = await getSecret(secretName);
  return unsubscribeSecretKey;
};

const generateUnsubscribeSignature = (email, token, secret) => {
  if (!secret) throw new Error("Unsubscribe secret not resolved; call resolveUnsubscribeSecret() first");
  return crypto.createHmac("sha256", secret).update(`${email}:${token}`).digest("hex").slice(0, 16);
};

const parseBooleanSetting = (value, fallback = false) => {
  if (typeof value === "boolean") return value;
  if (typeof value === "string") {
    if (value.toLowerCase() === "true") return true;
    if (value.toLowerCase() === "false") return false;
  }
  return fallback;
};

const parsePositiveIntSetting = (value, fallback) => {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
};

const parseOptionalPositiveIntSetting = (value, fallback = null) => {
  if (value === null || value === undefined || value === "") return fallback;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
};

const parseDeliveryModeSetting = (value, fallback = null) => {
  if (typeof value !== "string") return fallback;
  const normalizedValue = value.toLowerCase();
  return ["noop", "sandbox", "live"].includes(normalizedValue) ? normalizedValue : fallback;
};

const normalizeStringArraySetting = (value, fallback = []) => {
  if (Array.isArray(value)) {
    return [...new Set(value.map((entry) => typeof entry === "string" ? entry.trim() : "").filter(Boolean))];
  }

  if (typeof value === "string") {
    return [...new Set(value.split(",").map((entry) => entry.trim()).filter(Boolean))];
  }

  return [...fallback];
};

const normalizeDomainArraySetting = (value, fallback = []) =>
  [...new Set(normalizeStringArraySetting(value, fallback).map((domain) => domain.toLowerCase()))];

const parseBoundedIntSetting = (value, fallback, max) => {
  const parsed = parsePositiveIntSetting(value, fallback);
  return Number.isFinite(max) && max > 0 ? Math.min(parsed, max) : parsed;
};

const normalizePositiveIntArraySetting = (value) => {
  const rawValues = Array.isArray(value)
    ? value
    : typeof value === "string"
      ? value.split(",")
      : [];
  return [...new Set(rawValues
    .map((entry) => Number.parseInt(entry, 10))
    .filter((entry) => Number.isFinite(entry) && entry > 0))];
};

const isBulkMailCategory = (category, notificationSettings = DEFAULT_NOTIFICATION_SETTINGS) =>
  (notificationSettings.bulkMailCategories || DEFAULT_NOTIFICATION_SETTINGS.bulkMailCategories).includes(category);

/**
 * Load notification delivery tuning from Firestore app settings and merge with
 * hardcoded defaults.
 * @returns {Promise<object>} Normalized notification delivery settings.
 */
const getNotificationDeliverySettings = async () => {
  let notifications = {};

  try {
    const appSettings = await getAppSettings("connectFaas", ["notifications"]);
    notifications = appSettings?.notifications || {};
  } catch (error) {
    console.error("Error loading connectFaas notification settings. Falling back to defaults.", error);
  }

  return {
    useCloudTasksBulk: parseBooleanSetting(
      notifications.useCloudTasksBulk,
      DEFAULT_NOTIFICATION_SETTINGS.useCloudTasksBulk,
    ),
    sendgridDeliveryModeOverride: parseDeliveryModeSetting(
      notifications.sendgridDeliveryModeOverride,
      DEFAULT_NOTIFICATION_SETTINGS.sendgridDeliveryModeOverride,
    ),
    bulkMailCategories: normalizeStringArraySetting(
      notifications.bulkMailCategories,
      DEFAULT_NOTIFICATION_SETTINGS.bulkMailCategories,
    ),
    notificationBatchLimit: parseBoundedIntSetting(
      notifications.notificationBatchLimit,
      DEFAULT_NOTIFICATION_SETTINGS.notificationBatchLimit,
      1000,
    ),
    notificationReservationMs: parsePositiveIntSetting(
      notifications.notificationReservationMs,
      DEFAULT_NOTIFICATION_SETTINGS.notificationReservationMs,
    ),
    bulkRunStaleAfterDays: parsePositiveIntSetting(
      notifications.bulkRunStaleAfterDays,
      DEFAULT_NOTIFICATION_SETTINGS.bulkRunStaleAfterDays,
    ),
    bulkTaskMaxAttempts: parsePositiveIntSetting(
      notifications.bulkTaskMaxAttempts,
      DEFAULT_NOTIFICATION_SETTINGS.bulkTaskMaxAttempts,
    ),
    bulkThreshold: parsePositiveIntSetting(
      notifications.bulkThreshold,
      DEFAULT_NOTIFICATION_SETTINGS.bulkThreshold,
    ),
    bulkDefaultBatchSize: parseBoundedIntSetting(
      notifications.bulkDefaultBatchSize,
      DEFAULT_NOTIFICATION_SETTINGS.bulkDefaultBatchSize,
      1000,
    ),
    bulkMicrosoftBatchSize: parseBoundedIntSetting(
      notifications.bulkMicrosoftBatchSize,
      DEFAULT_NOTIFICATION_SETTINGS.bulkMicrosoftBatchSize,
      1000,
    ),
    sendgridBulkAsmGroupId: parseOptionalPositiveIntSetting(
      notifications.sendgridBulkAsmGroupId,
      DEFAULT_NOTIFICATION_SETTINGS.sendgridBulkAsmGroupId,
    ),
    sendgridBulkAsmGroupsToDisplay: normalizePositiveIntArraySetting(
      notifications.sendgridBulkAsmGroupsToDisplay,
    ),
    targetRecipientsPerHour: parsePositiveIntSetting(
      notifications.targetRecipientsPerHour,
      DEFAULT_NOTIFICATION_SETTINGS.targetRecipientsPerHour,
    ),
    targetRecipientsPerHourMicrosoft: parsePositiveIntSetting(
      notifications.targetRecipientsPerHourMicrosoft,
      DEFAULT_NOTIFICATION_SETTINGS.targetRecipientsPerHourMicrosoft,
    ),
    microsoftBulkDomains: normalizeDomainArraySetting(
      notifications.microsoftBulkDomains,
      DEFAULT_NOTIFICATION_SETTINGS.microsoftBulkDomains,
    ),
    sendRetryMax: parsePositiveIntSetting(
      notifications.sendRetryMax,
      DEFAULT_NOTIFICATION_SETTINGS.sendRetryMax,
    ),
    emailBatchDelayMs: parsePositiveIntSetting(
      notifications.emailBatchDelayMs,
      DEFAULT_NOTIFICATION_SETTINGS.emailBatchDelayMs,
    ),
    bulkWorkerUriDefault:
      typeof notifications.bulkWorkerUriDefault === "string"
        ? notifications.bulkWorkerUriDefault
        : DEFAULT_NOTIFICATION_SETTINGS.bulkWorkerUriDefault,
    bulkWorkerUriMicrosoft:
      typeof notifications.bulkWorkerUriMicrosoft === "string"
        ? notifications.bulkWorkerUriMicrosoft
        : DEFAULT_NOTIFICATION_SETTINGS.bulkWorkerUriMicrosoft,
  };
};

const getBulkUnsubscribeBaseUrl = () => {
  if (process.env.SG_UNSUBSCRIBE_URL) return process.env.SG_UNSUBSCRIBE_URL;

  const region = process.env.WEBHOOK_REGION || process.env.FUNCTION_REGION || "us-central1";
  if (!process.env.GCLOUD_PROJECT) {
    throw new Error("GCLOUD_PROJECT env var is required to derive the default unsubscribe URL");
  }

  // Keep the fallback pointed at the deployed webhook handler.
  // The webhook is the code path that actually records the suppression.
  return `https://${region}-${process.env.GCLOUD_PROJECT}.cloudfunctions.net/webhook?api=email-unsubscribe`;
};

const buildBulkUnsubscribeUrl = ({ normalizedEmail, token, signature }) => {
  const url = new URL(getBulkUnsubscribeBaseUrl());
  url.searchParams.set("email", normalizedEmail);
  url.searchParams.set("token", token);
  url.searchParams.set("sig", signature);
  return url.toString();
};

const ASM_GROUP_UNSUBSCRIBE_RAW_URL_TAG = "<%asm_group_unsubscribe_raw_url%>";

const buildBulkAsmConfig = (notificationSettings = DEFAULT_NOTIFICATION_SETTINGS) => {
  const groupId = Number(notificationSettings.sendgridBulkAsmGroupId);
  if (!Number.isFinite(groupId) || groupId <= 0) return null;

  const groupsToDisplay = normalizePositiveIntArraySetting(notificationSettings.sendgridBulkAsmGroupsToDisplay);
  const normalizedGroupsToDisplay = groupsToDisplay.length > 0
    ? [...new Set([groupId, ...groupsToDisplay])]
    : [groupId];

  return {
    group_id: groupId,
    groups_to_display: normalizedGroupsToDisplay,
  };
};

const buildBulkAsmUnsubscribeFooterHtml = (lang = "english") => {
  const footerTemplate = unsubscribeTextObj[lang] || unsubscribeTextObj.english || "";
  if (!footerTemplate) {
    return `<p><i><a href="${ASM_GROUP_UNSUBSCRIBE_RAW_URL_TAG}">Unsubscribe from this list</a>.</i></p>`;
  }

  return footerTemplate.replace(/<%\s*([^%]+?)\s*%>/g, (_match, linkText) =>
    `<a href="${ASM_GROUP_UNSUBSCRIBE_RAW_URL_TAG}">${linkText.trim()}</a>`);
};

// Returns a new string. Callers in the pagination loop assign the result to a local variable, NOT back to emailInSpec[lang].body,
// so the footer is not accumulated across pagination iterations.
const appendBulkAsmUnsubscribeFooter = (html = "", lang = "english") =>
  `${html || ""}\n${buildBulkAsmUnsubscribeFooterHtml(lang)}`;

const setupSendGrid = async () => {
  if (isSendGridSetup) return;
  try {
    const apiKey = await getSecret(process.env.GCLOUD_SENDGRID_SECRET);
    sgMail.setApiKey(apiKey);
    isSendGridSetup = true;
    // Resolve unsubscribe secret alongside SendGrid setup
    if (process.env.GCLOUD_UNSUBSCRIBE_SECRET) {
      await resolveUnsubscribeSecret();
    }
  } catch (error) {
    isSendGridSetup = false;
    throw error;
  }
};

const setupTwilio = async () => {
  if (isTwilioSetup) return;
  const secretsToFetch = {
    accountSid: process.env.TWILIO_ACCOUNT_SID,
    authToken: process.env.TWILIO_AUTH_TOKEN,
    messagingServiceSid: process.env.TWILIO_MESSAGING_SERVICE_SID
  };
  const client = new SecretManagerServiceClient();
  let fetchedSecrets = {};
  for (const [key, value] of Object.entries(secretsToFetch)) {
    const [version] = await client.accessSecretVersion({ name: value });
    fetchedSecrets[key] = version.payload.data.toString();
  }

  twilioClient = twilio(fetchedSecrets.accountSid, fetchedSecrets.authToken);
  messagingServiceSid = fetchedSecrets.messagingServiceSid;
  isTwilioSetup = true;
  twilioAuthToken = fetchedSecrets.authToken;
};

const normalizeRecipientEmail = (recipient) => {
  if (!recipient) return "";
  if (typeof recipient === "string") return normalizeEmailAddress(recipient);
  if (typeof recipient === "object") {
    return normalizeEmailAddress(
      recipient.email ||
      recipient.address ||
      recipient.emailAddress?.address ||
      "",
    );
  }
  return "";
};

const createBulkLaneCountMap = () => ({
  [BULK_LANE_DEFAULT]: 0,
  [BULK_LANE_MICROSOFT]: 0,
});

const createBulkLaneArrayMap = () => ({
  [BULK_LANE_DEFAULT]: [],
  [BULK_LANE_MICROSOFT]: [],
});

const getBulkLaneBatchSize = (notificationSettings = DEFAULT_NOTIFICATION_SETTINGS, lane = BULK_LANE_DEFAULT) => {
  const settingName = BULK_LANE_CONFIG[lane]?.batchSizeSetting || BULK_LANE_CONFIG[BULK_LANE_DEFAULT].batchSizeSetting;
  const batchSize = notificationSettings[settingName];
  return Number.isFinite(batchSize) && batchSize > 0
    ? Math.min(batchSize, 1000)
    : DEFAULT_NOTIFICATION_SETTINGS[settingName];
};

const getRecipientEmailDomain = (email) => {
  const normalized = normalizeRecipientEmail(email);
  if (!normalized.includes("@")) return "";
  return normalized.split("@").pop() || "";
};

const getBulkRecipientLane = (email, notificationSettings = DEFAULT_NOTIFICATION_SETTINGS) =>
  (notificationSettings.microsoftBulkDomains || DEFAULT_NOTIFICATION_SETTINGS.microsoftBulkDomains)
    .includes(getRecipientEmailDomain(email))
    ? BULK_LANE_MICROSOFT
    : BULK_LANE_DEFAULT;

const getBulkTargetRecipientsPerHour = (notificationSettings = DEFAULT_NOTIFICATION_SETTINGS, lane = BULK_LANE_DEFAULT) => {
  if (lane === BULK_LANE_MICROSOFT) {
    const microsoftTarget = notificationSettings.targetRecipientsPerHourMicrosoft;
    if (Number.isFinite(microsoftTarget) && microsoftTarget > 0) {
      // Keep Microsoft-family domains on their own throttle due to observed deliverability issues.
      // Start conservatively (1500/hour) and tune up or down from observed Outlook deferrals/blocks.
      // See Firestore -> appSettings -> connectFaas -> notifications -> targetRecipientsPerHourMicrosoft.
      return microsoftTarget;
    }
  }

  const defaultTarget = notificationSettings.targetRecipientsPerHour;
  if (!Number.isFinite(defaultTarget) || defaultTarget <= 0) {
    return 0;
  }
  return defaultTarget;
};

/**
 * Resolve the effective SendGrid delivery mode for the current environment.
 * Non-prod tiers can be overridden through app settings; prod remains fixed.
 * @param {object} notificationSettings Normalized notification delivery settings.
 * @returns {"noop"|"sandbox"|"live"} Resolved delivery mode.
 */
const getSendGridDeliveryMode = (notificationSettings = DEFAULT_NOTIFICATION_SETTINGS) => {
  const derivedMode = (() => {
    switch (sharedUtils.developmentTier) {
      case "PROD":
        return "live";
      case "STAGE":
        return "sandbox";
      default:
        return "noop";
    }
  })();

  if (sharedUtils.developmentTier !== "PROD" && notificationSettings.sendgridDeliveryModeOverride) {
    return notificationSettings.sendgridDeliveryModeOverride;
  }

  return derivedMode;
};

// Keep the delivery-mode split. It prevents non-prod experiments from training
// mailbox providers on accidental bulk traffic to real participants.
const sendViaSendGrid = async (message, { logLabel = "email", notificationSettings = null } = {}) => {
  const resolvedSettings = notificationSettings || await getNotificationDeliverySettings();
  const deliveryMode = getSendGridDeliveryMode(resolvedSettings);
  if (deliveryMode === "noop") {
    console.log(`SendGrid noop mode: skipped provider send for ${logLabel}.`);
    return { deliveryMode, response: null };
  }

  try {
    await setupSendGrid();
  } catch (error) {
    error.providerAttempted = false;
    throw error;
  }

  const outboundMessage = deliveryMode === "sandbox"
    ? {
      ...message,
      mail_settings: {
        ...(message.mail_settings || {}),
        sandbox_mode: { enable: true },
      },
    }
    : message;

  const response = await sgMail.send(outboundMessage);
  console.log(`SendGrid ${deliveryMode} mode: accepted ${logLabel}.`);
  return { deliveryMode, response };
};

const getProviderErrorStatusCode = (error = {}) => {
  const statusCode = error.statusCode ?? error.status ?? error.code ??
    error.cause?.statusCode ?? error.cause?.status ?? error.cause?.code;
  const parsed = Number(statusCode);
  return Number.isFinite(parsed) ? parsed : null;
};

const isProviderRejectedWithoutAcceptance = (error = {}) => {
  if (error.providerAttempted === false || error.cause?.providerAttempted === false) return true;
  const statusCode = getProviderErrorStatusCode(error);
  return statusCode != null && statusCode >= 400 && statusCode < 500;
};

const buildOutcomeRecipient = (record = {}, reason = "") => ({
  token: record.token || "",
  notificationId: record.id || "",
  reason,
});

const createBulkOutcomeCollector = (existing = {}) => ({
  filtered: [...(existing.filtered || [])],
  suppressed: [...(existing.suppressed || [])],
  providerFailed: [...(existing.providerFailed || [])],
  providerUnknown: [...(existing.providerUnknown || [])],
  sent: Number(existing.sent) || 0,
});

const addBlockedEmailRecordsToOutcomeCollector = (collector, blockedRecords = []) => {
  if (!collector || !Array.isArray(blockedRecords) || blockedRecords.length === 0) return;

  for (const record of blockedRecords) {
    const existingState = record._existingProcessingState || "";
    const existingIsSent = record._existingIsSent;
    if (existingIsSent !== false && existingState !== "provider_acceptance_unknown") {
      collector.sent++;
    } else if (isProviderSendStartedState(existingState)) {
      collector.providerUnknown.push(buildOutcomeRecipient(record, existingState));
    }
  }
};

const summarizeBulkOutcome = ({
  planned = 0,
  sent = 0,
  collector = createBulkOutcomeCollector(),
} = {}) => {
  const sentCount = Number.isFinite(sent) ? sent : Number(collector.sent) || 0;
  return {
    counts: {
      planned,
      sent: sentCount,
      filtered: collector.filtered.length,
      suppressed: collector.suppressed.length,
      providerFailed: collector.providerFailed.length,
      providerUnknown: collector.providerUnknown.length,
    },
    unsuccessful: {
      filtered: collector.filtered,
      suppressed: collector.suppressed,
      providerFailed: collector.providerFailed,
      providerUnknown: collector.providerUnknown,
    },
  };
};

/**
 * Reserve email records for sending on deterministic notifications docs, then mark provider acceptance or failure on the same records.
 * @param {object} options Delivery options.
 * @returns {Promise<{recordsToSend: object[]}>}
 */
const deliverReservedEmailRecords = async ({
  emailRecords,
  providerAttemptOwner,
  sendReservedRecords,
  notificationSettings = DEFAULT_NOTIFICATION_SETTINGS,
}) => {
  const { recordsToSend, blockedRecords = [] } = await reserveNotificationBatch(
    emailRecords,
    providerAttemptOwner,
    notificationSettings.notificationReservationMs,
  );

  if (recordsToSend.length > 0) {
    const {
      recordsToSend: providerRecordsToSend,
    } = await markNotificationBatchProviderSendStarted(recordsToSend, providerAttemptOwner);
    if (providerRecordsToSend.length === 0) {
      return { recordsToSend: [], blockedRecords };
    }

    let providerAccepted = false;
    try {
      await sendReservedRecords(providerRecordsToSend);
      providerAccepted = true;
      const acceptedCount = await markNotificationBatchAccepted(providerRecordsToSend, providerAttemptOwner);
      if (acceptedCount !== providerRecordsToSend.length) {
        throw new Error(
          `Provider accepted ${providerRecordsToSend.length} notification(s), but only ${acceptedCount} acceptance state write(s) succeeded.`,
        );
      }
    } catch (error) {
      if (providerAccepted || !isProviderRejectedWithoutAcceptance(error)) {
        await markNotificationBatchProviderAcceptanceUnknown(providerRecordsToSend, providerAttemptOwner, error);
      } else {
        await markNotificationBatchFailed(providerRecordsToSend, providerAttemptOwner, error);
      }
      if (providerAccepted || !isProviderRejectedWithoutAcceptance(error)) {
        error.providerUnknownRecords = providerRecordsToSend;
      } else {
        error.providerFailedRecords = providerRecordsToSend;
      }
      error.blockedRecords = blockedRecords;
      throw error;
    }

    return { recordsToSend: providerRecordsToSend, blockedRecords };
  }

  return { recordsToSend: [], blockedRecords };
};

/**
 * Send a reserved email batch with retry behavior for transient SendGrid
 * failures.
 * @param {object} options Send options.
 * @returns {Promise<void>}
 */
const sendReservedNotificationEmailBatch = async ({
  emailBatch,
  logLabel,
  failureLabel,
  notificationSettings = DEFAULT_NOTIFICATION_SETTINGS,
}) => {
  const maxRetries = notificationSettings.sendRetryMax;
  let sendSuccess = false;

  // Only 429 is retried in-process: the rate-limit response proves SendGrid
  // did NOT accept the message, so re-sending is safe.
  // Transient 5xx is treated as ambiguous-acceptance and moves to provider_acceptance_unknown.
  // Cloud Tasks handles broader retry for bulk; instant transactional sends do not retry to avoid duplicate-send risk.
  // Non-429 4xx is permanent rejection.
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      await sendViaSendGrid(emailBatch, { logLabel, notificationSettings });
      sendSuccess = true;
      break;
    } catch (error) {
      const statusCode = getProviderErrorStatusCode(error);
      const isRetryable = statusCode === 429;
      if (!isRetryable || attempt === maxRetries) {
        console.error(`Email send failed for ${failureLabel}. Attempt ${attempt + 1}/${maxRetries + 1}.`, error);
        const finalError = new Error(`Failed sending emails for ${failureLabel}.`, { cause: error });
        finalError.code = error.code;
        finalError.status = error.status;
        finalError.statusCode = error.statusCode;
        finalError.providerAttempted = error.providerAttempted !== false;
        throw finalError;
      }

      const delayMs = backoffMs(attempt) + Math.floor(Math.random() * 200);
      console.log(`Retrying ${failureLabel} in ${delayMs}ms (attempt ${attempt + 1})`);
      await delay(delayMs);
    }
  }

  if (!sendSuccess) throw new Error(`Failed sending emails for ${failureLabel}.`);
};

const stripProviderAttemptOwner = ({ _providerAttemptOwner, ...record }) => record;

const groupRecordsByProviderAttemptOwner = (records = []) => {
  const groupedRecords = new Map();
  for (const record of records) {
    const providerAttemptOwner = record._providerAttemptOwner || "";
    if (!groupedRecords.has(providerAttemptOwner)) groupedRecords.set(providerAttemptOwner, []);
    groupedRecords.get(providerAttemptOwner).push(record);
  }
  return groupedRecords;
};

const getProviderErrorFromRecord = (record = {}) => ({
  code: record.errorCode,
  statusCode: record.statusCode,
  message: record.errorMessage || "Provider send failed.",
});

const saveSmsSuccessRecords = async (records = []) => {
  for (const [providerAttemptOwner, groupedRecords] of groupRecordsByProviderAttemptOwner(records)) {
    const cleanRecords = groupedRecords.map(stripProviderAttemptOwner);
    if (providerAttemptOwner) {
      await markNotificationBatchAccepted(cleanRecords, providerAttemptOwner);
    } else {
      await saveNotificationBatch(cleanRecords);
    }
  }
};

const saveSmsFailureRecords = async (records = []) => {
  for (const [providerAttemptOwner, groupedRecords] of groupRecordsByProviderAttemptOwner(records)) {
    const cleanRecords = groupedRecords.map(stripProviderAttemptOwner);
    if (!providerAttemptOwner) {
      await saveNotificationBatch(cleanRecords);
      continue;
    }

    const rejectedRecords = [];
    const ambiguousRecords = [];
    for (const record of groupedRecords) {
      const providerError = getProviderErrorFromRecord(record);
      if (isProviderRejectedWithoutAcceptance(providerError)) {
        rejectedRecords.push(stripProviderAttemptOwner(record));
      } else {
        ambiguousRecords.push(stripProviderAttemptOwner(record));
      }
    }

    if (rejectedRecords.length > 0) {
      await markNotificationBatchFailed(rejectedRecords, providerAttemptOwner, getProviderErrorFromRecord(rejectedRecords[0]));
    }
    if (ambiguousRecords.length > 0) {
      await markNotificationBatchProviderAcceptanceUnknown(
        ambiguousRecords,
        providerAttemptOwner,
        getProviderErrorFromRecord(ambiguousRecords[0]),
      );
    }
  }
};

/**
 * Send Twilio SMS message using API.
 * Set up Twilio client and messaging service SID before calling this function.
 * @param {Object} smsRecord SMS record object to be saved to Firestore
 * @returns {Promise<Object>}
 */
const sendTwilioMessage = async (smsRecord) => {
  try {
    const result = await twilioClient.messages.create({
      body: smsRecord.notification.body,
      to: smsRecord.phone,
      messagingServiceSid,
    });
    const updatedSmsRecord = { ...smsRecord, messageSid: result.sid || "" };
    return { smsRecord: updatedSmsRecord, isSuccess: true, isRateLimit: false };
  } catch (error) {
    const errorCode = error.code?.toString() ?? "500";
    const statusCode = (error.status ?? error.statusCode)?.toString() ?? "500";
    if (errorCode === "20429" || statusCode === "429") {
      return { smsRecord, isSuccess: false, isRateLimit: true };
    }

    console.error(
      `Error sending SMS (participant token: ${smsRecord.token}; spec ID: ${smsRecord.notificationSpecificationsID}).`,
      error,
    );
    const errorMessage = error.message || 'Error occurred calling Twilio API.';
    const updatedSmsRecord = { ...smsRecord, errorCode, errorMessage, statusCode };
    return { smsRecord: updatedSmsRecord, isSuccess: false, isRateLimit: false };
  }
};

/**
 * Handles rate-limited batch sending of Twilio SMS messages with retry logic.
 * 
 * Note: Currently there's one phone number in sender pool. It can handle 160K messages in one hour. For higher throughput in prod, add one or more phone numbers to sender pool.
 */
class SmsBatchSender {
  #queue = [];
  #isProcessing = false;
  #sentCounts = {}; // { [specId]: { english: number, spanish: number } }
  #failedCounts = {}; // { [specId]: { english: number, spanish: number } }
  #retryCounts = {}; // { [specId]: { [recordId]: number } }
  #finishedSpecSet = new Set();
  #batchSize;
  #maxRetries;
  #prevBatchFinishTime = 0;
  #prevProgressLogTime = Date.now();
  #sendFn;
  #saveSuccessFn;
  #saveFailureFn;
  #delayFn;

  constructor({
    batchSize = 150,
    maxRetries = 5,
    sendFn = sendTwilioMessage,
    saveFn = null,
    saveSuccessFn = saveFn || saveSmsSuccessRecords,
    saveFailureFn = saveFn || saveSmsFailureRecords,
    delayFn = delay,
  } = {}) {
    this.#batchSize = batchSize;
    this.#maxRetries = maxRetries;
    this.#sendFn = sendFn;
    this.#saveSuccessFn = saveSuccessFn;
    this.#saveFailureFn = saveFailureFn;
    this.#delayFn = delayFn;
  }

  /**
   * Add multiple SMS records to the queue and trigger processing.
   * Each record must have notificationSpecificationsID and language properties.
   * @param {Object[]} smsRecords - Array of SMS record objects
   */
  addToQueue(smsRecords) {
    this.#queue.push(...smsRecords);
    this.#processQueue();
  }

  /**
   * Get counts of successfully sent SMS messages for a specific spec ID.
   * @param {string} specId - Notification specification ID
   * @returns {Object} Counts object: { english: number, spanish: number }
   */
  getSentCounts(specId) {
    return this.#sentCounts[specId] ? { ...this.#sentCounts[specId] } : { english: 0, spanish: 0 };
  }

  /**
   * Get counts of failed SMS messages for a specific spec ID.
   * @param {string} specId - Notification specification ID
   * @returns {Object} Counts object: { english: number, spanish: number }
   */
  getFailedCounts(specId) {
    return this.#failedCounts[specId] ? { ...this.#failedCounts[specId] } : { english: 0, spanish: 0 };
  }

  /**
   * Mark that all SMS messages for a spec have been added to the queue.
   * Call this after adding all messages for a spec to signal completion.
   * @param {string} specId - Notification specification ID
   */
  markSpecEnd(specId) {
    this.#queue.push({ specId, isEndMarker: true });
    this.#processQueue();
  }

  /**
   * Check if all SMS messages for a spec have been processed (sent or failed).
   * @param {string} specId - Notification specification ID
   * @returns {boolean} True if the spec's end marker has been processed
   */
  isSpecFinished(specId) {
    return this.#finishedSpecSet.has(specId);
  }

  /**
   * Wait for all SMS messages for a spec to be processed.
   * @param {string} specId - Notification specification ID
   * @param {number} [checkIntervalMs=1000] - Interval between checks in milliseconds
   * @returns {Promise<{sentCounts: {english: number, spanish: number}, failedCounts: {english: number, spanish: number}}>}
   */
  async waitForSpec(specId, checkIntervalMs = 1000) {
    while (!this.isSpecFinished(specId)) {
      await this.#delayFn(checkIntervalMs);
    }
    return {
      sentCounts: this.getSentCounts(specId),
      failedCounts: this.getFailedCounts(specId),
    };
  }

  /**
   * Increment the sent or failed count for a given spec ID and language.
   * @param {Object} countsObj - The counts object to update (this.#sentCounts or this.#failedCounts)
   * @param {string} specId - Notification specification ID
   * @param {string} language - Language key ("english" or "spanish")
   */
  #incrementCount(countsObj, specId, language) {
    if (!countsObj[specId]) {
      countsObj[specId] = { english: 0, spanish: 0 };
    }
    countsObj[specId][language]++;
  }

  /**
   * Log sent and failed counts for all in-progress specs. Throttled to at most once every 30 seconds.
   */
  #logProgress() {
    const now = Date.now();
    if (now - this.#prevProgressLogTime < 30_000) return;
    this.#prevProgressLogTime = now;

    const specIds = new Set([...Object.keys(this.#sentCounts), ...Object.keys(this.#failedCounts)]);
    for (const specId of specIds) {
      if (this.#finishedSpecSet.has(specId)) continue;
      const sent = this.#sentCounts[specId] ?? { english: 0, spanish: 0 };
      const failed = this.#failedCounts[specId] ?? { english: 0, spanish: 0 };
      console.log(
        `SMS in progress (spec ID ${specId}): sent ${sent.english + sent.spanish} (en: ${sent.english}, es: ${sent.spanish}), ` +
          `failed ${failed.english + failed.spanish} (en: ${failed.english}, es: ${failed.spanish}).`,
      );
    }
  }

  /**
   * Process the SMS queue in batches with rate-limit handling.
   * Sends batches of SMS messages, retries rate-limited messages (up to maxRetries),
   * saves successful records to Firestore, and marks specs as finished when their end markers are reached.
   * Only one instance runs at a time; subsequent calls are no-ops while processing is active.
   * @returns {Promise<void>}
   */
  async #processQueue() {
    if (this.#isProcessing) return;
    this.#isProcessing = true;
    const delayTimeMs = 1000;

    while (this.#queue.length > 0) {
      const elapsedTime = Date.now() - this.#prevBatchFinishTime;
      if (elapsedTime < delayTimeMs) {
        await this.#delayFn(delayTimeMs - elapsedTime);
      }

      const batchItems = this.#queue.splice(0, this.#batchSize);
      const endMarkerSpecIdSet = new Set(batchItems.filter((item) => item.isEndMarker).map((item) => item.specId));
      const batchSmsRecords = batchItems.filter((item) => !item.isEndMarker);

      if (batchSmsRecords.length > 0) {
        const batchSendResults = await Promise.all(batchSmsRecords.map((r) => this.#sendFn(r)));
        this.#prevBatchFinishTime = Date.now();

        const successRecords = [];
        const rateLimitRecords = [];
        const failedRecords = [];

        for (const result of batchSendResults) {
          if (result.isSuccess) {
            successRecords.push(result.smsRecord);
            continue;
          }

          if (result.isRateLimit) {
            rateLimitRecords.push(result.smsRecord);
            continue;
          }

          failedRecords.push(result.smsRecord);
        }

        if (successRecords.length > 0) {
          try {
            await this.#saveSuccessFn(successRecords);
            for (const record of successRecords) {
              this.#incrementCount(
                this.#sentCounts,
                record.notificationSpecificationsID,
                record.language,
              );
            }
          } catch (error) {
            console.error("Error running saveNotificationBatch.", error);
          }
        }

        for (const record of rateLimitRecords.reverse()) {
          const specId = record.notificationSpecificationsID;
          if (!this.#retryCounts[specId]) this.#retryCounts[specId] = {};
          this.#retryCounts[specId][record.id] = (this.#retryCounts[specId][record.id] || 0) + 1;

          if (this.#retryCounts[specId][record.id] > this.#maxRetries) {
            failedRecords.push({
              ...record,
              errorCode: "RATE_LIMIT_RETRIES_EXHAUSTED",
              errorMessage: `Message cannot be sent after ${this.#maxRetries} retries`,
              statusCode: "429",
            });
            console.error(
              `Message cannot be sent after ${this.#maxRetries} retries (id: ${record.id}; spec ID: ${record.notificationSpecificationsID}).`,
            );
            continue;
          }

          const currItems = [record];
          if (endMarkerSpecIdSet.has(specId)) {
            endMarkerSpecIdSet.delete(specId);
            currItems.push({ specId, isEndMarker: true });
          }
          this.#queue.unshift(...currItems);
        }

        if (failedRecords.length > 0) {
          try {
            await this.#saveFailureFn(failedRecords);
            for (const record of failedRecords) {
              this.#incrementCount(
                this.#failedCounts,
                record.notificationSpecificationsID,
                record.language,
              );
            }
          } catch (error) {
            console.error('Error running saveNotificationBatch for failed records.', error);
          }
        }

      }

      for (const specId of endMarkerSpecIdSet) {
        this.#finishedSpecSet.add(specId);
        delete this.#retryCounts[specId];
      }

      this.#logProgress();
    }
    this.#isProcessing = false;
  }
}

const smsBatchSender = new SmsBatchSender({});

const subscribeToNotification = async (req, res) => {
    setHeadersDomainRestricted(req, res);

    if(req.method === 'OPTIONS') return res.status(200).json({code: 200});

    if(req.method !== 'POST') {
        return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
    }

    if(!req.headers.authorization || req.headers.authorization.trim() === ""){
        return res.status(401).json(getResponseJSON('Authorization failed!', 401));
    }

    const idToken = req.headers.authorization.replace('Bearer','').trim();
    const { validateIDToken } = require('./firestore');
    const decodedToken = await validateIDToken(idToken);

    if(decodedToken instanceof Error){
        return res.status(401).json(getResponseJSON(decodedToken.message, 401));
    }

    if(!decodedToken){
        return res.status(401).json(getResponseJSON('Authorization failed!', 401));
    }
    const data = req.body;
    console.log(decodedToken.uid , JSON.stringify(data));
    if(Object.keys(data).length <= 0 && data.token === undefined){
        return res.status(400).json(getResponseJSON('Bad request!', 400));
    }
    const notificationToken = data.token;

    const { notificationTokenExists } = require('./firestore');
    const { storeNotificationTokens } = require('./firestore');
    const uid = await notificationTokenExists(notificationToken);
    if(uid && uid !== decodedToken.uid) return res.status(403).json(getResponseJSON('Token is already associated with another user', 403))
    if(uid) return res.status(400).json(getResponseJSON('Token already exists', 400));
    storeNotificationTokens({notificationToken, uid: decodedToken.uid})
    res.status(200).json({message: 'Success!', code:200})
}

const markAllNotificationsAsAlreadyRead = async (ids, collection) => {
  const { markNotificationAsRead } = require('./firestore');

  const promises = ids.filter(id => id).map((id) => markNotificationAsRead(id, collection));
  const results = await Promise.allSettled(promises);
  for (const result of results) {
    if (result.status === 'rejected') {
      console.error(`Error marking notification as read in ${collection} collection:`, result.reason);
    }
  }
};

const retrieveNotifications = async (req, res, uid) => {
  if (req.method !== "GET") {
    return res.status(405).json(getResponseJSON("Only GET requests are accepted!", 405));
  }

  const { retrieveUserNotifications } = require("./firestore");
  try {
    const notificationArray = await retrieveUserNotifications(uid);
    if (notificationArray.length > 0 && req.query.markasread === 'true') {
      await markAllNotificationsAsAlreadyRead(
        notificationArray.map((notification) => notification.id),
        "notifications"
      );
    }
    return res.status(200).json({ data: notificationArray, message: "Success", code: 200 });
  } catch (error) {
    console.error("Error when retrieving notifications.", error);
    return res.status(500).json({ data: [], message: "Internal Server Error", code: 500 });
  }
};

/**
 * Normalize an email address for duplicate comparison.
 */
const normalizeEmailForCompare = (email) => {
    if (typeof email !== "string") return null;
    const trimmed = email.trim();
    return trimmed ? trimmed.toLowerCase() : null;
};

/**
 * Remove any CC entry that duplicates the primary `to` recipient. SendGrid
 * rejects a personalization where the same address appears in both `to` and
 * `cc`/`bcc` ("Each email address in the personalization block should be
 * unique").
 */
const dedupeCcAgainstTo = (cc, toEmail) => {
    if (!cc) return undefined;
    const toKey = normalizeEmailForCompare(toEmail);

    if (Array.isArray(cc)) {
        const filtered = cc.filter((entry) => {
            const key = normalizeEmailForCompare(entry);
            return key && key !== toKey;
        });
        return filtered.length > 0 ? filtered : undefined;
    }

    const ccKey = normalizeEmailForCompare(cc);
    if (!ccKey || ccKey === toKey) return undefined;
    return cc;
};

const sendEmail = async (emailTo, messageSubject, html, cc) => {
    const notificationSettings = await getNotificationDeliverySettings();
    const msg = {
        to: emailTo,
        from: {
            name: process.env.SG_FROM_NAME || 'Connect for Cancer Prevention Study',
            email: process.env.SG_FROM_EMAIL || 'no-reply-myconnect@mail.nih.gov'
        },
        subject: messageSubject,
        html: html,
    };
    const dedupedCc = dedupeCcAgainstTo(cc, emailTo);
    if (dedupedCc) msg.cc = dedupedCc;
    msg.text = htmlToPlaintext(html);
    try {
        await sendViaSendGrid(msg, { logLabel: "sendEmail", notificationSettings });
    } catch (error) {
        let bodyDetail;
        try {
            bodyDetail = error?.response?.body
                ? JSON.stringify(error.response.body)
                : undefined;
        } catch {
            bodyDetail = "<unserializable response body>";
        }
        console.error("Email send failed:", {
            message: error?.message,
            code: error?.code,
            to: emailTo,
            cc: cc || undefined,
            from: msg.from?.email,
            subject: messageSubject,
            body: bodyDetail,
        });
        throw error;
    }
}

// Keep bounded concurrency for lightweight planning/classification work.
// The actual notification delivery paths are intentionally serialized below.
const runWithConcurrency = async (items, maxConcurrent, fn) => {
  let active = 0;
  let index = 0;
  const results = [];

  return new Promise((resolve) => {
    if (items.length === 0) return resolve([]);

    const runNext = () => {
      while (active < maxConcurrent && index < items.length) {
        const i = index++;
        active++;
        fn(items[i])
          .then((result) => { results[i] = result; })
          .catch((err) => { results[i] = err; })
          .finally(() => {
            active--;
            if (index >= items.length && active === 0) {
              resolve(results);
            } else {
              runNext();
            }
          });
      }
    };
    runNext();
  });
};

const runSequentiallyAndCollect = async (items, fn) => {
  const results = [];
  for (const item of items) {
    try {
      results.push(await fn(item));
    } catch (error) {
      results.push(error);
    }
  }
  return results;
};

const sanitizeTaskIdSegment = (value) => String(value || "")
  .toLowerCase()
  .replace(/[^a-z0-9-]+/g, "-")
  .replace(/^-+|-+$/g, "")
  .slice(0, 120) || "notification";

const buildBulkNotificationRunId = (specId, runDateKey, runSequence = 1) =>
  `${sanitizeTaskIdSegment(specId)}-${sanitizeTaskIdSegment(runDateKey)}-run-${runSequence}`;

const buildBulkNotificationBatchId = (lane, batchNumber) =>
  `${sanitizeTaskIdSegment(lane)}-batch-${batchNumber}`;

const buildPlannedBulkNotificationTaskId = (runId, lane, batchNumber) =>
  `${sanitizeTaskIdSegment(runId)}-${sanitizeTaskIdSegment(lane)}-batch-${batchNumber}`;

const isAlreadyExistsError = (error) => {
  const statusCode = error?.code || error?.status || error?.statusCode;
  return statusCode === 6 || statusCode === 409 || /already exists/i.test(error?.message || "");
};

const specHasEmailChannel = (notificationSpec) =>
  Array.isArray(notificationSpec?.notificationType) && notificationSpec.notificationType.includes("email");

const specIsEmailOnly = (notificationSpec) =>
  Array.isArray(notificationSpec?.notificationType) &&
  notificationSpec.notificationType.length > 0 &&
  notificationSpec.notificationType.every((notificationType) => notificationType === "email");

const parseNotificationConditions = (notificationSpec) => {
  if (!notificationSpec.conditions) return [];
  return JSON.parse(notificationSpec.conditions);
};

const serializePlannedBulkConditions = (conditions = []) =>
  (Array.isArray(conditions) ? conditions : []).map((condition) => {
    if (Array.isArray(condition) && condition.length === 3) {
      const [field, operator, value] = condition;
      return { field, operator, value };
    }
    return condition;
  });

const deserializePlannedBulkConditions = (conditions = []) =>
  (Array.isArray(conditions) ? conditions : []).map((condition) => {
    if (
      condition &&
      typeof condition === "object" &&
      !Array.isArray(condition) &&
      Object.prototype.hasOwnProperty.call(condition, "field") &&
      Object.prototype.hasOwnProperty.call(condition, "operator")
    ) {
      return [condition.field, condition.operator, condition.value];
    }
    return condition;
  });

const notificationEmailUsesLoginDetails = (notificationSpec = {}) =>
  langArray.some((lang) => (notificationSpec.email || {})[lang]?.body?.includes("<loginDetails>"));

const buildRedactedLoginDetails = (fetchedData = {}) => {
  if (typeof fetchedData.loginDetails === "string" && fetchedData.loginDetails) {
    return fetchedData.loginDetails;
  }

  if (fetchedData[conceptIds.signInMechanism] === "phone" && fetchedData[conceptIds.authenticationPhone]) {
    return redactPhoneLoginInfo(fetchedData[conceptIds.authenticationPhone]);
  }
  if (
    fetchedData[conceptIds.signInMechanism] === "password" &&
    fetchedData[conceptIds.authenticationEmail]
  ) {
    return redactEmailLoginInfo(fetchedData[conceptIds.authenticationEmail]);
  }
  if (
    fetchedData[conceptIds.signInMechanism] === "passwordAndPhone" &&
    fetchedData[conceptIds.authenticationEmail] &&
    fetchedData[conceptIds.authenticationPhone]
  ) {
    return `${redactPhoneLoginInfo(fetchedData[conceptIds.authenticationPhone])}, ${redactEmailLoginInfo(fetchedData[conceptIds.authenticationEmail])}`;
  }

  return "";
};

const buildNotificationEmailFieldsToFetch = (notificationSpec = {}) => {
  const fieldsToFetch = ["Connect_ID", "token", "state.uid", conceptIds.preferredLanguage.toString()];
  if (notificationSpec.firstNameField) fieldsToFetch.push(notificationSpec.firstNameField);
  if (notificationSpec.preferredNameField) fieldsToFetch.push(notificationSpec.preferredNameField);
  if (notificationSpec.emailField) fieldsToFetch.push(notificationSpec.emailField);

  if (notificationEmailUsesLoginDetails(notificationSpec)) {
    fieldsToFetch.push(
      `${conceptIds.signInMechanism}`,
      `${conceptIds.authenticationPhone}`,
      `${conceptIds.authenticationEmail}`,
    );
  }

  return [...new Set(fieldsToFetch.filter(Boolean))];
};

/**
 * Compute the per-batch delay needed to stay within the target hourly send
 * rate for the given lane.
 * @param {number} totalRecipientCount Number of recipients in the batch.
 * @param {object} notificationSettings Normalized notification delivery settings.
 * @param {string} lane Bulk lane name.
 * @returns {number} Delay in milliseconds.
 */
const computeBulkInterBatchDelayMs = (
  totalRecipientCount,
  notificationSettings = DEFAULT_NOTIFICATION_SETTINGS,
  lane = BULK_LANE_DEFAULT,
) => {
  const targetRecipientsPerHour = getBulkTargetRecipientsPerHour(notificationSettings, lane);
  if (!Number.isFinite(targetRecipientsPerHour) || targetRecipientsPerHour <= 0) {
    return 0;
  }
  if (!Number.isFinite(totalRecipientCount) || totalRecipientCount <= 0) {
    return 0;
  }

  const batchSize = Math.min(
    totalRecipientCount,
    notificationSettings.notificationBatchLimit,
  );
  return Math.ceil((batchSize * 3600000) / targetRecipientsPerHour);
};

/**
 * Compute the next-batch schedule delay for a bulk task chain based on the
 * slowest lane in the current batch and the number of active bulk specs.
 * @param {object} laneCounts Counts sent by lane for the current batch.
 * @param {number} activeBulkSpecCount Number of concurrently active bulk specs.
 * @param {object} notificationSettings Normalized notification delivery settings.
 * @returns {number} Delay in milliseconds.
 */
const computeBulkLaneScheduleDelayMs = (
  laneCounts = {},
  activeBulkSpecCount = 1,
  notificationSettings = DEFAULT_NOTIFICATION_SETTINGS,
) => {
  const specCount = Math.max(1, activeBulkSpecCount || 1);
  const laneDelayMs = Math.max(
    computeBulkInterBatchDelayMs(laneCounts[BULK_LANE_DEFAULT] || 0, notificationSettings, BULK_LANE_DEFAULT),
    computeBulkInterBatchDelayMs(laneCounts[BULK_LANE_MICROSOFT] || 0, notificationSettings, BULK_LANE_MICROSOFT),
  );
  return laneDelayMs * specCount;
};

const computePlannedBulkBatchScheduleDelaySeconds = (
  batchNumber = 1,
  lane = BULK_LANE_DEFAULT,
  notificationSettings = DEFAULT_NOTIFICATION_SETTINGS,
) => {
  const targetRecipientsPerHour = getBulkTargetRecipientsPerHour(notificationSettings, lane);
  if (!Number.isFinite(targetRecipientsPerHour) || targetRecipientsPerHour <= 0) return 0;

  const batchSize = getBulkLaneBatchSize(notificationSettings, lane);
  const delayMs = Math.max(0, (Math.max(1, batchNumber) - 1) * batchSize * 3600000 / targetRecipientsPerHour);
  return Math.ceil(delayMs / 1000);
};

const classifyNotificationSpec = async (
  notificationSpec,
  notificationSettings = DEFAULT_NOTIFICATION_SETTINGS,
) => {
  const timeParams = getTimeParams(notificationSpec);
  if (!timeParams) {
    return {
      notificationSpec,
      timeParams: null,
      conditions: [],
      mailStream: isBulkMailCategory(notificationSpec.category, notificationSettings) ? "bulk" : "transactional",
      totalRecipientCount: 0,
      skip: true,
    };
  }

  const conditions = parseNotificationConditions(notificationSpec);
  const categoryIsBulk = isBulkMailCategory(notificationSpec.category, notificationSettings);
  let mailStream = categoryIsBulk ? "bulk" : "transactional";
  let totalRecipientCount = 0;

  if (specHasEmailChannel(notificationSpec)) {
    totalRecipientCount = await countParticipantsForNotificationsBQ({
      notificationSpecId: notificationSpec.id,
      startTimeStr: timeParams.startTimeStr,
      stopTimeStr: timeParams.stopTimeStr,
      timeField: timeParams.timeField,
      conditions,
    });

    const bulkThreshold = notificationSettings.bulkThreshold;
    if (totalRecipientCount >= bulkThreshold) {
      mailStream = "bulk";
    }
  }

  return {
    notificationSpec,
    timeParams,
    conditions,
    mailStream,
    totalRecipientCount,
    skip: false,
  };
};

const getPlannedBulkTaskQueueName = (lane = BULK_LANE_DEFAULT) =>
  BULK_LANE_CONFIG[lane]?.queueName || BULK_LANE_CONFIG[BULK_LANE_DEFAULT].queueName;

const getPlannedBulkScheduledFor = (scheduleDelaySeconds = 0, now = new Date()) =>
  new Date(now.getTime() + (Math.max(0, scheduleDelaySeconds) * 1000)).toISOString();

const compactBulkRecipient = (recipient = {}, notificationSpec = {}) => {
  const compact = {};
  [
    "Connect_ID",
    "token",
    conceptIds.preferredLanguage,
    notificationSpec.firstNameField,
    notificationSpec.preferredNameField,
    notificationSpec.emailField,
  ].filter(Boolean).forEach((fieldName) => {
    if (recipient[fieldName] !== undefined && recipient[fieldName] !== null) {
      compact[fieldName] = recipient[fieldName];
    }
  });

  const uid = recipient.state?.uid;
  if (uid !== undefined && uid !== null) {
    compact.state = { uid };
  }

  if (notificationEmailUsesLoginDetails(notificationSpec)) {
    const loginDetails = buildRedactedLoginDetails(recipient);
    if (loginDetails) compact.loginDetails = loginDetails;
  }

  return compact;
};

const buildBulkRunSettingsSnapshot = (notificationSettings = DEFAULT_NOTIFICATION_SETTINGS) => ({
  bulkDefaultBatchSize: notificationSettings.bulkDefaultBatchSize,
  bulkMicrosoftBatchSize: notificationSettings.bulkMicrosoftBatchSize,
  sendgridBulkAsmGroupId: notificationSettings.sendgridBulkAsmGroupId,
  sendgridBulkAsmGroupsToDisplay: notificationSettings.sendgridBulkAsmGroupsToDisplay,
  targetRecipientsPerHour: notificationSettings.targetRecipientsPerHour,
  targetRecipientsPerHourMicrosoft: notificationSettings.targetRecipientsPerHourMicrosoft,
  microsoftBulkDomains: notificationSettings.microsoftBulkDomains || DEFAULT_NOTIFICATION_SETTINGS.microsoftBulkDomains,
});

// Memory bound: this planner accumulates the full lane-grouped recipient list in memory before slicing into batch docs.
// Each recipient is the compact payload (~6 small fields), so 200k recipients ~= 60MB, well within the 1024MB Cloud Functions Gen2 memory budget.
const buildBulkRunPlan = async ({
  notificationSpec,
  totalRecipientCount = 0,
  timeParams,
  conditions,
  runDateKey = getEasternDateKey(),
  runSequence = 1,
  notificationSettings = DEFAULT_NOTIFICATION_SETTINGS,
}) => {
  const runId = buildBulkNotificationRunId(notificationSpec.id, runDateKey, runSequence);
  const existingRun = await getBulkNotificationRun(runId);
  if (existingRun?.batchIds?.length > 0 && ["planned", "enqueue_failed", "queued", "running"].includes(existingRun.status)) {
    return { run: existingRun, batchDocs: null, reused: true };
  }

  const emailField = notificationSpec.emailField || "";
  const fieldsToFetch = buildNotificationEmailFieldsToFetch(notificationSpec);
  const recipientsByLane = createBulkLaneArrayMap();
  let previousToken = "";
  let plannedRecipientCount = 0;

  while (true) {
    const fetchedDataArray = await getParticipantsForNotificationsBQ({
      notificationSpecId: notificationSpec.id,
      startTimeStr: timeParams.startTimeStr,
      stopTimeStr: timeParams.stopTimeStr,
      timeField: timeParams.timeField,
      conditions,
      fieldsToFetch,
      limit: notificationSettings.notificationBatchLimit,
      previousToken,
    });

    if (fetchedDataArray.length === 0) break;

    for (const recipient of fetchedDataArray) {
      const email = normalizeEmailAddress(recipient[emailField]);
      if (!email) continue;
      const lane = getBulkRecipientLane(email, notificationSettings);
      recipientsByLane[lane].push(compactBulkRecipient(recipient, notificationSpec));
      plannedRecipientCount++;
    }

    if (fetchedDataArray.length < notificationSettings.notificationBatchLimit) break;
    previousToken = fetchedDataArray[fetchedDataArray.length - 1].token;
    if (!previousToken) break;
  }

  const createdAt = new Date().toISOString();
  const batchDocs = [];
  const laneRecipientCounts = createBulkLaneCountMap();
  const laneBatchCounts = createBulkLaneCountMap();

  for (const lane of [BULK_LANE_DEFAULT, BULK_LANE_MICROSOFT]) {
    const batchSize = getBulkLaneBatchSize(notificationSettings, lane);
    const recipients = recipientsByLane[lane];
    laneRecipientCounts[lane] = recipients.length;
    for (let i = 0; i < recipients.length; i += batchSize) {
      const batchNumber = Math.floor(i / batchSize) + 1;
      const batchRecipients = recipients.slice(i, i + batchSize);
      const scheduleDelaySeconds = computePlannedBulkBatchScheduleDelaySeconds(batchNumber, lane, notificationSettings);
      laneBatchCounts[lane]++;
      batchDocs.push({
        id: buildBulkNotificationBatchId(lane, batchNumber),
        runId,
        specId: notificationSpec.id,
        runDateKey,
        runSequence,
        lane,
        batchNumber,
        batchSize,
        recipientCount: batchRecipients.length,
        recipients: batchRecipients,
        scheduleDelaySeconds,
        status: "planned",
        counts: {
          planned: batchRecipients.length,
          sent: 0,
          filtered: 0,
          suppressed: 0,
          providerFailed: 0,
          providerUnknown: 0,
        },
        unsuccessful: {
          filtered: [],
          suppressed: [],
          providerFailed: [],
          providerUnknown: [],
        },
        createdAt,
      });
    }
  }

  const runDoc = {
    id: runId,
    specId: notificationSpec.id,
    category: notificationSpec.category,
    attempt: notificationSpec.attempt,
    runDateKey,
    runSequence,
    status: "planned",
    notificationSpec,
    totalRecipientCount,
    plannedRecipientCount,
    laneRecipientCounts,
    laneBatchCounts,
    settings: buildBulkRunSettingsSnapshot(notificationSettings),
    timeParams,
    conditions: serializePlannedBulkConditions(conditions),
    createdAt,
  };

  await saveBulkNotificationRunPlan({ runDoc, batchDocs });
  return { run: { ...runDoc, batchIds: batchDocs.map((batchDoc) => batchDoc.id) }, batchDocs, reused: false };
};

const enqueuePlannedBulkBatchTask = async ({
  queue,
  run,
  batchDoc,
  notificationSettings,
}) => {
  const taskId = buildPlannedBulkNotificationTaskId(run.id, batchDoc.lane, batchDoc.batchNumber);
  const payload = {
    runId: run.id,
    batchId: batchDoc.id,
    lane: batchDoc.lane,
    specId: run.specId,
    runDateKey: run.runDateKey,
    runSequence: run.runSequence,
  };

  // Resolve the dispatch URI before calling queue.enqueue so a misconfigured appSettings field surfaces in our logs, rather
  // than later as a silent 404 inside Cloud Tasks. Any throw here is fatal for this task.
  let uri;
  try {
    uri = buildBulkWorkerUri(batchDoc.lane, notificationSettings);
  } catch (error) {
    console.error(`Cannot enqueue planned bulk task ${taskId} (runId=${run.id}, batchId=${batchDoc.id}, lane=${batchDoc.lane}): ${error.message}`);
    throw error;
  }

  try {
    await queue.enqueue(payload, {
      scheduleDelaySeconds: batchDoc.scheduleDelaySeconds || 0,
      dispatchDeadlineSeconds: BULK_TASK_DISPATCH_DEADLINE_SECONDS,
      id: taskId,
      uri,
    });
    return { taskId, alreadyExisted: false };
  } catch (error) {
    if (isAlreadyExistsError(error)) {
      console.log(`Planned bulk notification task already exists: ${taskId}`);
      return { taskId, alreadyExisted: true };
    }
    // If the dispatch fails due to a wrong URL, this is the breadcrumb that ties the task identity to the URL used (Cloud Tasks does not surface).
    console.error(`Failed to enqueue planned bulk task ${taskId} (runId=${run.id}, batchId=${batchDoc.id}, lane=${batchDoc.lane}, uri=${uri}): ${error.message || error.code || "(no message)"}`);
    throw error;
  }
};

const enqueueBulkRunPlanTasks = async ({
  run,
  batchDocs,
  notificationSettings,
}) => {
  const { getFunctions } = require("firebase-admin/functions");
  const functions = getFunctions();
  const enqueued = [];
  const batchesToEnqueue = batchDocs || await getBulkNotificationRunBatches(run.id);

  // Bounded concurrency for batch enqueue. Each call is a small Cloud Tasks API + a single Firestore transaction.
  // Running ~5 in flight at a time shaves seconds off the planning phase for a 50-batch run without overwhelming either backend.
  const ENQUEUE_CONCURRENCY = 5;
  const candidates = batchesToEnqueue.filter(
    (batchDoc) => !["enqueued", "running", "complete", "failed"].includes(batchDoc.status),
  );
  const results = await runWithConcurrency(candidates, ENQUEUE_CONCURRENCY, async (batchDoc) => {
    const queueName = getPlannedBulkTaskQueueName(batchDoc.lane);
    const queue = functions.taskQueue(queueName);
    const { taskId, alreadyExisted } = await enqueuePlannedBulkBatchTask({ queue, run, batchDoc, notificationSettings });
    const scheduleDelaySeconds = batchDoc.scheduleDelaySeconds || 0;
    // On a fresh enqueue we record the calculated scheduledFor.
    // On resume (Cloud Tasks already has the task from a prior partial run),
    // the real scheduled-for time is whatever the prior enqueue set.
    // Leave the batch doc's existing scheduledFor untouched rather than overwriting it.
    if (alreadyExisted) {
      await markBulkNotificationBatchEnqueued({
        runId: run.id,
        batchId: batchDoc.id,
        taskId,
        queueName,
        scheduleDelaySeconds,
      });
    } else {
      await markBulkNotificationBatchEnqueued({
        runId: run.id,
        batchId: batchDoc.id,
        taskId,
        queueName,
        scheduleDelaySeconds,
        scheduledFor: getPlannedBulkScheduledFor(scheduleDelaySeconds),
      });
    }
    return { batchId: batchDoc.id, taskId, queueName, alreadyExisted };
  });

  let newlyEnqueued = 0;
  for (const result of results) {
    if (result instanceof Error) throw result;
    enqueued.push({ batchId: result.batchId, taskId: result.taskId, queueName: result.queueName });
    if (!result.alreadyExisted) newlyEnqueued++;
  }

  // Only flip the run to "queued" when this invocation actually enqueued at least one new task.
  // A pure-resume call where every task was already in Cloud Tasks does not need the additional run-level write.
  if (newlyEnqueued > 0) {
    await markBulkNotificationRunQueued(run.id);
  }
  return enqueued;
};

/**
 * Notifications handler triggered by an HTTP request from cloud scheduler.
 * @param {Request} req HTTP request
 * @param {Response} res HTTP response
 */
async function sendScheduledNotifications(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json(getResponseJSON("Only POST requests are accepted!", 405));
  }

  if (isSendingNotifications) {
    console.log("Function sendScheduledNotifications() is already running. Exiting...");
    return res.status(208).json(getResponseJSON("Function is already running.", 208));
  }

  if (!req.body || !req.body.scheduleAt) {
    return res.status(400).json(getResponseJSON("Field scheduleAt is missing in request body.", 400));
  }

  isSendingNotifications = true;
  try {
    const notificationSettings = await getNotificationDeliverySettings();
    const notificationSpecArray = await getNotificationSpecsByScheduleOncePerDay(
      req.body.scheduleAt,
      notificationSettings.bulkRunStaleAfterDays,
    );
    if (notificationSpecArray.length === 0) {
      console.log("Function sendScheduledNotifications() has run earlier today. Exiting...");
      return res.status(208).json(getResponseJSON("Function has run earlier today.", 208));
    }

    const useCloudTasks = notificationSettings.useCloudTasksBulk;
    const runDateKey = getEasternDateKey();
    const planResults = await runWithConcurrency(
      notificationSpecArray,
      4,
      (notificationSpec) => classifyNotificationSpec(notificationSpec, notificationSettings),
    );

    const failures = [];
    const successfulSpecIds = [];
    const inlineBulkPlans = [];
    const inlineTransactionalPlans = [];
    const queuedBulkPlans = [];

    for (let i = 0; i < planResults.length; i++) {
      const result = planResults[i];
      const notificationSpec = notificationSpecArray[i];

      if (result instanceof Error) {
        failures.push(new Error(`Failed planning notification spec ${notificationSpec?.id || "unknown"}`, { cause: result }));
        continue;
      }

      const plan = result;
      if (plan.skip) {
        successfulSpecIds.push(plan.notificationSpec.id);
        continue;
      }

      if (useCloudTasks && plan.mailStream === "bulk" && specHasEmailChannel(plan.notificationSpec) && specIsEmailOnly(plan.notificationSpec)) {
        queuedBulkPlans.push(plan);
        continue;
      }

      if (plan.mailStream === "bulk") {
        inlineBulkPlans.push(plan);
      } else {
        inlineTransactionalPlans.push(plan);
      }
    }

    let enqueuedBulkTaskCount = 0;
    if (queuedBulkPlans.length > 0) {
      for (const plan of queuedBulkPlans) {
        const runSequence = (Number(plan.notificationSpec.bulkRunSequence) || 0) + 1;
        let queuedMarkerWritten = false;
        try {
          const { run, batchDocs, reused } = await buildBulkRunPlan({
            notificationSpec: plan.notificationSpec,
            totalRecipientCount: plan.totalRecipientCount,
            timeParams: plan.timeParams,
            conditions: plan.conditions,
            runDateKey,
            runSequence,
            notificationSettings,
          });

          const plannedBatches = batchDocs || await getBulkNotificationRunBatches(run.id);
          if (plannedBatches.length === 0) {
            await finalizeBulkNotificationRunIfTerminal(run.id);
            console.log(`No planned bulk email batches for spec ${plan.notificationSpec.id}.`);
            continue;
          }

          // Mark the spec queued after the plan exists but before zero-delay tasks can run.
          // Do not commit bulkRunSequence yet:
          // If enqueueing partially fails, clearing this transient marker lets the scheduler
          // reuse the same runSequence and resume the same planned run.
          await markNotificationSpecsQueuedForRun(
            [plan.notificationSpec.id],
            runDateKey,
            undefined,
            { [plan.notificationSpec.id]: runSequence },
            { commitRunSequence: false },
          );
          queuedMarkerWritten = true;

          const enqueued = await enqueueBulkRunPlanTasks({
            run,
            batchDocs: plannedBatches,
            notificationSettings,
          });
          enqueuedBulkTaskCount += enqueued.length;
          console.log(
            `${reused ? "Resumed" : "Planned"} bulk run ${run.id} for spec ${plan.notificationSpec.id}; ${enqueued.length} task(s) enqueued.`,
          );
        } catch (error) {
          const runId = buildBulkNotificationRunId(plan.notificationSpec.id, runDateKey, runSequence);
          if (queuedMarkerWritten) {
            await clearNotificationSpecsQueuedRun(
              [plan.notificationSpec.id],
              runDateKey,
              { [plan.notificationSpec.id]: runSequence },
            );
          }
          await markBulkNotificationRunEnqueueFailed(runId, error);
          failures.push(new Error(`Failed enqueuing notification spec ${plan.notificationSpec.id}`, { cause: error }));
        }
      }
    }

    const runInlinePlan = async (plan) => handleNotificationSpec(plan.notificationSpec, {
      mailStream: plan.mailStream,
      totalRecipientCount: plan.totalRecipientCount,
      timeParams: plan.timeParams,
      conditions: plan.conditions,
      notificationSettings,
    });

    if (inlineBulkPlans.length > 0 || inlineTransactionalPlans.length > 0) {
      const inlinePlans = [...inlineBulkPlans, ...inlineTransactionalPlans];
      if (
        getSendGridDeliveryMode(notificationSettings) !== "noop" &&
        inlinePlans.some((plan) => specHasEmailChannel(plan.notificationSpec))
      ) {
        await setupSendGrid();
      }
      if (inlinePlans.some((plan) =>
        Array.isArray(plan.notificationSpec.notificationType) &&
          plan.notificationSpec.notificationType.includes("sms")
      )) {
        await setupTwilio();
      }
    }

    // Keep delivery serialized for now. Pacing is the primary throttle, and a
    // single in-flight spec at a time is easier to reason about operationally.
    const bulkResults = await runSequentiallyAndCollect(inlineBulkPlans, runInlinePlan);
    const opResults = await runSequentiallyAndCollect(inlineTransactionalPlans, runInlinePlan);

    for (let i = 0; i < bulkResults.length; i++) {
      const result = bulkResults[i];
      if (result instanceof Error) {
        failures.push(new Error(`Failed sending bulk notification spec ${inlineBulkPlans[i].notificationSpec.id}`, { cause: result }));
      } else {
        successfulSpecIds.push(inlineBulkPlans[i].notificationSpec.id);
      }
    }

    for (let i = 0; i < opResults.length; i++) {
      const result = opResults[i];
      if (result instanceof Error) {
        failures.push(new Error(`Failed sending transactional notification spec ${inlineTransactionalPlans[i].notificationSpec.id}`, { cause: result }));
      } else {
        successfulSpecIds.push(inlineTransactionalPlans[i].notificationSpec.id);
      }
    }

    if (successfulSpecIds.length > 0) {
      await markNotificationSpecsLastRun(successfulSpecIds);
    }

    if (failures.length > 0) {
      for (const err of failures) {
        console.error("Notification spec failed:", err);
      }
      console.error(`Finished with ${failures.length} notification spec failure(s).`);
      return res.status(500).json(getResponseJSON(`Finished with ${failures.length} spec failure(s).`, 500));
    }

    const bulkMsg = enqueuedBulkTaskCount > 0 ? ` ${enqueuedBulkTaskCount} bulk batch task(s) enqueued as Cloud Tasks.` : "";
    console.log(`Finished sending out notifications.${bulkMsg}`);
    return res.status(200).json(getResponseJSON(`Finished sending out notifications.${bulkMsg}`, 200));
  } catch (error) {
    console.error("Error occurred running function sendScheduledNotifications.", error);
    return res.status(500).json(getResponseJSON("Internal Server Error!", 500));
  } finally {
    isSendingNotifications = false;
  }
}

function getTimeParams(notificationSpec) {
  const { primaryField, time } = notificationSpec;

  if (/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z/.test(primaryField)) {
    const startTime = getAdjustedTime(primaryField, time.start.day, time.start.hour, time.start.minute);
    const stopTime = getAdjustedTime(primaryField, time.stop.day, time.stop.hour, time.stop.minute);
    const currentTime = new Date();
    if (startTime > currentTime || currentTime > stopTime) return null;
    return { startTimeStr: "", stopTimeStr: "", timeField: "" };
  }

  const startTime = getAdjustedTime(new Date(), -time.start.day, -time.start.hour, -time.start.minute);
  const stopTime = getAdjustedTime(new Date(), -time.stop.day, -time.stop.hour, -time.stop.minute);
  return {
    startTimeStr: startTime.toISOString(),
    stopTimeStr: stopTime.toISOString(),
    timeField: primaryField,
  };
}

async function handleNotificationSpec(notificationSpec, options = {}) {
  const notificationSettings = options.notificationSettings || await getNotificationDeliverySettings();
  const timeParams = options.timeParams ?? getTimeParams(notificationSpec);
  if (!timeParams) {
    return {
      specId: notificationSpec.id,
      category: notificationSpec.category,
      attempt: notificationSpec.attempt,
      skipped: true,
      skipReason: "outside_time_window",
      completedAt: new Date().toISOString(),
    };
  }

  const readableSpecString = notificationSpec.category + ", " + notificationSpec.attempt;
  const emailField = notificationSpec.emailField ?? "";
  const phoneField = notificationSpec.phoneField ?? "";
  const firstNameField = notificationSpec.firstNameField ?? "";
  const preferredNameField = notificationSpec.preferredNameField ?? "";
  const bulkThreshold = notificationSettings.bulkThreshold;
  const limit = options.limit || notificationSettings.notificationBatchLimit;
  const sender = options.smsBatchSender || (options.singleBatch ? new SmsBatchSender({}) : smsBatchSender);
  const conditions = options.conditions ?? parseNotificationConditions(notificationSpec);
  const plannedRecipients = Array.isArray(options.plannedRecipients) ? options.plannedRecipients : null;
  const outcomeCollector = options.outcomeCollector || null;
  const emailOnly = options.emailOnly === true;
  const providedCount = Number.isFinite(options.totalRecipientCount) && options.totalRecipientCount >= 0
    ? options.totalRecipientCount
    : null;

  let mailStream = options.mailStream || (isBulkMailCategory(notificationSpec.category, notificationSettings) ? "bulk" : "transactional");
  let estimatedTotalRecipientCount = providedCount;
  let hasAuthoritativeRecipientCount = providedCount != null;
  let observedRecipientCount = 0;
  let previousToken = options.previousToken || "";
  let hasNext = true;
  let nextPreviousToken = "";
  let skippedBySuppression = 0;
  let currentBatchNumber = options.batchNumber || 1;
  const providerAttemptOwner = options.providerAttemptOwner || `${notificationSpec.id}-${currentBatchNumber}-${uuid()}`;
  const totalBulkLaneSentCounts = createBulkLaneCountMap();
  let recommendedNextBulkDelayMs = 0;

  let fieldsToFetch = buildNotificationEmailFieldsToFetch(notificationSpec);
  !emailOnly && phoneField && fieldsToFetch.push(phoneField);
  Array.isArray(notificationSpec.notificationType) &&
    !emailOnly &&
    notificationSpec.notificationType.includes("sms") &&
    fieldsToFetch.push(conceptIds.canWeText.toString());
  fieldsToFetch = [...new Set(fieldsToFetch.filter(Boolean))];

  let emailInSpec = notificationSpec.email || {};
  let smsInSpec = notificationSpec.sms || {};
  let emailHasToken = false;
  let emailHasLoginDetails = false;
  let emailCount = { total: 0 };
  let smsCount = { total: 0 };

  for (const lang of langArray) {
    if (emailInSpec[lang]?.body) {
      let emailBody = emailInSpec[lang].body;
      if (!isBulkMailCategory(notificationSpec.category, notificationSettings)) {
        emailBody = converter.makeHtml(emailBody);
      }

      emailBody = emailBody.replace(/<firstName>/g, "{{firstName}}");
      if (emailBody.includes("${token}")) {
        emailHasToken = true;
        emailBody = emailBody.replace(/\${token}/g, "{{token}}");
      }

      if (emailBody.includes("<loginDetails>")) {
        emailHasLoginDetails = true;
        emailBody = emailBody.replace(/<loginDetails>/g, "{{loginDetails}}");
      }

      emailInSpec[lang].body = emailBody;
    }

    emailCount[lang] = 0;
    smsCount[lang] = 0;
  }

  if (!plannedRecipients && estimatedTotalRecipientCount == null && mailStream === "transactional" && specHasEmailChannel(notificationSpec)) {
    const preCount = await countParticipantsForNotificationsBQ({
      notificationSpecId: notificationSpec.id,
      startTimeStr: timeParams.startTimeStr,
      stopTimeStr: timeParams.stopTimeStr,
      timeField: timeParams.timeField,
      conditions,
    });
    if (preCount >= 0) {
      estimatedTotalRecipientCount = preCount;
      hasAuthoritativeRecipientCount = true;
      if (preCount >= bulkThreshold) {
        mailStream = "bulk";
        console.log(
          `Spec ${notificationSpec.id} pre-classified as bulk (estimated recipients: ${preCount}, threshold: ${bulkThreshold})`,
        );
      }
    }
  }

  while (hasNext) {
    let fetchedDataArray = [];
    if (plannedRecipients) {
      fetchedDataArray = currentBatchNumber === (options.batchNumber || 1) ? plannedRecipients : [];
    } else {
      try {
        fetchedDataArray = await getParticipantsForNotificationsBQ({
          notificationSpecId: notificationSpec.id,
          startTimeStr: timeParams.startTimeStr,
          stopTimeStr: timeParams.stopTimeStr,
          timeField: timeParams.timeField,
          conditions,
          fieldsToFetch,
          limit,
          previousToken,
        });
      } catch (error) {
        console.error(`getParticipantsForNotificationsBQ() error running spec ID ${notificationSpec.id}.`, error);
        throw error;
      }
    }

    if (fetchedDataArray.length === 0) {
      hasNext = false;
      break;
    }

    observedRecipientCount += fetchedDataArray.length;
    if (estimatedTotalRecipientCount == null || observedRecipientCount > estimatedTotalRecipientCount) {
      estimatedTotalRecipientCount = observedRecipientCount;
      hasAuthoritativeRecipientCount = false;
    }

    if (mailStream === "transactional" && observedRecipientCount >= bulkThreshold) {
      mailStream = "bulk";
      console.log(
        `Spec ${notificationSpec.id} upgraded to bulk after observing ${observedRecipientCount} recipient(s) (threshold: ${bulkThreshold})`,
      );
    }

    const processedBeforeBatch = options.singleBatch
      ? ((currentBatchNumber - 1) * limit)
      : (observedRecipientCount - fetchedDataArray.length);
    const processedAfterBatch = processedBeforeBatch + fetchedDataArray.length;
    hasNext = plannedRecipients ? false : hasAuthoritativeRecipientCount &&
      Number.isFinite(estimatedTotalRecipientCount) && estimatedTotalRecipientCount >= 0
      ? processedAfterBatch < estimatedTotalRecipientCount
      : fetchedDataArray.length === limit;
    nextPreviousToken = hasNext ? fetchedDataArray[fetchedDataArray.length - 1].token : "";

    const emailsInBatch = fetchedDataArray
      .map((d) => d[emailField])
      .filter(Boolean)
      .map((email) => normalizeEmailAddress(email))
      .filter(Boolean);
    const filteredEmails = new Set(emailsInBatch.filter((email) => shouldFilterEmailAddress(email)));
    const suppressedEmails = await getEmailSuppressions(
      emailsInBatch.filter((email) => !filteredEmails.has(email)),
      mailStream,
    );

    let notificationData = {};
    for (const lang of langArray) {
      notificationData[lang] = {
        emailRecordArray: [],
        emailPersonalizationArray: [],
        smsRecordArray: [],
        bulkLaneByRecordId: new Map(),
      };
    }
    const pageBulkLaneSentCounts = createBulkLaneCountMap();

    for (const fetchedData of fetchedDataArray) {
      if (!fetchedData[emailField] && !fetchedData[phoneField]) continue;

      const currDateTime = new Date().toISOString();
      const firstName = fetchedData[preferredNameField] || fetchedData[firstNameField];
      const prefLang = cidToLangMapper[fetchedData[conceptIds.preferredLanguage]] || "english";
      const recordCommonData = {
        notificationSpecificationsID: notificationSpec.id,
        attempt: notificationSpec.attempt,
        category: notificationSpec.category,
        Connect_ID: fetchedData.Connect_ID,
        token: fetchedData.token,
        uid: fetchedData.state?.uid,
        read: false,
      };
      const emailId = getNotificationRecordId({
        notificationSpecificationsID: notificationSpec.id,
        notificationType: "email",
        token: fetchedData.token,
      });
      const smsId = getNotificationRecordId({
        notificationSpecificationsID: notificationSpec.id,
        notificationType: "sms",
        token: fetchedData.token,
      });

      const normalizedEmail = normalizeEmailAddress(fetchedData[emailField]);
      if (normalizedEmail && filteredEmails.has(normalizedEmail)) {
        skippedBySuppression++;
        if (outcomeCollector) {
          outcomeCollector.filtered.push(buildOutcomeRecipient({
            ...recordCommonData,
            id: emailId,
            email: fetchedData[emailField],
          }, "filtered_address"));
        }
      } else if (emailInSpec[prefLang]?.body &&
        validEmailFormat.test(fetchedData[emailField]) &&
        !suppressedEmails.has(normalizedEmail)) {
        let substitutions = { firstName };
        let currEmailBody = emailInSpec[prefLang].body.replace(/{{firstName}}/g, firstName);

        if (emailHasLoginDetails) {
          const loginDetails = buildRedactedLoginDetails(fetchedData);
          if (!loginDetails) {
            console.log("No login details found for participant with token:", fetchedData.token);
            if (outcomeCollector) {
              outcomeCollector.filtered.push(buildOutcomeRecipient({
                ...recordCommonData,
                id: emailId,
                email: fetchedData[emailField],
              }, "missing_login_details"));
            }
            continue;
          }

          substitutions.loginDetails = loginDetails;
          currEmailBody = currEmailBody.replace(/{{loginDetails}}/g, loginDetails);
        }

        if (emailHasToken) {
          substitutions.token = fetchedData.token;
          currEmailBody = currEmailBody.replace(/{{token}}/g, fetchedData.token);
        }
        const bulkLane = mailStream === "bulk"
          ? getBulkRecipientLane(normalizedEmail, notificationSettings)
          : BULK_LANE_DEFAULT;

        const personalization = {
          to: fetchedData[emailField],
          substitutions,
          custom_args: {
            connect_id: fetchedData.Connect_ID,
            token: fetchedData.token,
            notification_id: emailId,
            gcloud_project: process.env.GCLOUD_PROJECT,
            mail_stream: mailStream,
          },
        };

        if (mailStream === "bulk") {
          if (!unsubscribeSecretKey) {
            await resolveUnsubscribeSecret();
          }
          // One-click unsubscribe on bulk mail reduces spam complaints and is part of large-sender expectations at major providers.
          // Current release keeps the app-owned signed unsubscribe route.
          // Next unsubscribe-process release should add SendGrid
          // `asm.group_id = 22391` ("Newsletter Communications") on bulk sends,
          // then re-evaluate whether this manual route is still needed.
          const sig = generateUnsubscribeSignature(normalizedEmail, fetchedData.token, unsubscribeSecretKey);
          const unsubUrl = buildBulkUnsubscribeUrl({
            normalizedEmail,
            token: fetchedData.token,
            signature: sig,
          });
          personalization.headers = {
            "List-Unsubscribe": `<${unsubUrl}>`,
            "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
          };
        }

        notificationData[prefLang].emailPersonalizationArray.push(personalization);

        notificationData[prefLang].emailRecordArray.push({
          ...recordCommonData,
          id: emailId,
          notificationType: "email",
          language: prefLang,
          email: fetchedData[emailField],
          notification: {
            title: emailInSpec[prefLang].subject,
            body: currEmailBody,
            time: currDateTime,
          },
        });
        notificationData[prefLang].bulkLaneByRecordId.set(emailId, bulkLane);
      } else if (suppressedEmails.has(normalizedEmail)) {
        skippedBySuppression++;
        if (outcomeCollector) {
          outcomeCollector.suppressed.push(buildOutcomeRecipient({
            ...recordCommonData,
            id: emailId,
            email: fetchedData[emailField],
          }, "suppressed"));
        }
      } else if (emailField && fetchedData[emailField]) {
        if (outcomeCollector) {
          outcomeCollector.filtered.push(buildOutcomeRecipient({
            ...recordCommonData,
            id: emailId,
            email: fetchedData[emailField],
          }, validEmailFormat.test(fetchedData[emailField]) ? "missing_email_template" : "invalid_email_format"));
        }
      }

      let canWeText = fetchedData[conceptIds.canWeText];
      if (typeof canWeText === "object" && canWeText.integer) {
        canWeText = canWeText.integer;
      }

      if (!emailOnly && smsInSpec[prefLang]?.body && fetchedData[phoneField]?.length >= 10 && canWeText === conceptIds.yes) {
        const phoneNumber = fetchedData[phoneField].replace(/\D/g, "");
        if (phoneNumber.length >= 10) {
          const smsTo = `+1${phoneNumber.slice(-10)}`;
          notificationData[prefLang].smsRecordArray.push({
            ...recordCommonData,
            id: smsId,
            notificationType: "sms",
            language: prefLang,
            phone: smsTo,
            notification: {
              body: smsInSpec[prefLang].body,
              time: currDateTime,
            },
          });
        }
      }
    }

    for (const lang of langArray) {
      let { emailRecordArray, emailPersonalizationArray, smsRecordArray, bulkLaneByRecordId } = notificationData[lang];
      if (emailPersonalizationArray.length > 0) {
        const personalizationByRecordId = new Map(
          emailRecordArray.map((record, idx) => [record.id, emailPersonalizationArray[idx]]),
        );
        let recordsToSend = [];
        let blockedRecords = [];
        try {
          ({ recordsToSend, blockedRecords = [] } = await deliverReservedEmailRecords({
            emailRecords: emailRecordArray,
            providerAttemptOwner,
            notificationSettings,
            sendReservedRecords: async (recordsToSend) => {
              const personalizationsToSend = recordsToSend
                .map((record) => personalizationByRecordId.get(record.id))
                .filter(Boolean);
              if (personalizationsToSend.length !== recordsToSend.length) {
                const error = new Error(`Missing personalizations for reserved records in ${notificationSpec.id}(${readableSpecString}).`);
                error.providerAttempted = false;
                throw error;
              }

              const bulkAsmConfig = mailStream === "bulk" ? buildBulkAsmConfig(notificationSettings) : null;
              const emailHtml = bulkAsmConfig
                ? appendBulkAsmUnsubscribeFooter(emailInSpec[lang].body, lang)
                : emailInSpec[lang].body;
              const emailBatch = {
                from: {
                  name: process.env.SG_FROM_NAME || "Connect for Cancer Prevention Study",
                  email: process.env.SG_FROM_EMAIL || "no-reply-myconnect@mail.nih.gov",
                },
                subject: emailInSpec[lang].subject,
                html: emailHtml,
                text: htmlToPlaintext(emailHtml),
                personalizations: personalizationsToSend,
              };

              if (mailStream === "bulk") {
                if (bulkAsmConfig) {
                  emailBatch.asm = bulkAsmConfig;
                  emailBatch.tracking_settings = {
                    subscription_tracking: {
                      enable: false,
                    },
                  };
                } else {
                  emailBatch.tracking_settings = {
                    // Fallback until SendGrid ASM groups are configured for bulk mail.
                    // ASM group-specific unsubscribe handling is the long-term model.
                    subscription_tracking: {
                      enable: true,
                      html: unsubscribeTextObj[lang] || unsubscribeTextObj.english,
                    },
                  };
                }
              }

              await sendReservedNotificationEmailBatch({
                emailBatch,
                logLabel: `notification spec ${notificationSpec.id} batch ${currentBatchNumber} (${lang})`,
                failureLabel: `${notificationSpec.id}(${readableSpecString})`,
                notificationSettings,
              });
            },
          }));
        } catch (error) {
          if (outcomeCollector) {
            for (const record of error.providerFailedRecords || []) {
              outcomeCollector.providerFailed.push(buildOutcomeRecipient(record, "provider_failed"));
            }
            for (const record of error.providerUnknownRecords || []) {
              outcomeCollector.providerUnknown.push(buildOutcomeRecipient(record, "provider_acceptance_unknown"));
            }
            addBlockedEmailRecordsToOutcomeCollector(outcomeCollector, error.blockedRecords);
          }
          throw error;
        }

        addBlockedEmailRecordsToOutcomeCollector(outcomeCollector, blockedRecords);
        emailCount[lang] += recordsToSend.length;
        if (outcomeCollector) outcomeCollector.sent += recordsToSend.length;
        for (const record of recordsToSend) {
          const bulkLane = bulkLaneByRecordId.get(record.id) || BULK_LANE_DEFAULT;
          pageBulkLaneSentCounts[bulkLane] += 1;
          totalBulkLaneSentCounts[bulkLane] += 1;
        }
      }

      if (smsRecordArray.length > 0) {
        const { recordsToSend: reservedSmsRecords } = await reserveNotificationBatch(
          smsRecordArray,
          providerAttemptOwner,
          notificationSettings.notificationReservationMs,
        );
        const {
          recordsToSend: smsRecordsToSend,
        } = await markNotificationBatchProviderSendStarted(reservedSmsRecords, providerAttemptOwner);
        if (smsRecordsToSend.length > 0) {
          sender.addToQueue(smsRecordsToSend.map((record) => ({
            ...record,
            _providerAttemptOwner: providerAttemptOwner,
          })));
        }
      }
    }

    recommendedNextBulkDelayMs = mailStream === "bulk"
      ? computeBulkLaneScheduleDelayMs(pageBulkLaneSentCounts, options.bulkSpecCount, notificationSettings)
      : 0;

    if (options.singleBatch) {
      break;
    }

    if (hasNext) {
      previousToken = nextPreviousToken;
      if (mailStream === "bulk") {
        await delay(recommendedNextBulkDelayMs);
      } else if (mailStream !== "bulk") {
        await delay(notificationSettings.emailBatchDelayMs);
      }
    }

    currentBatchNumber++;
  }

  sender.markSpecEnd(notificationSpec.id);
  const { sentCounts, failedCounts } = await sender.waitForSpec(notificationSpec.id);

  let totalFailed = 0;
  for (const lang of langArray) {
    smsCount[lang] = sentCounts[lang] || 0;
    emailCount.total += emailCount[lang];
    smsCount.total += smsCount[lang];
    totalFailed += failedCounts[lang] || 0;
  }

  if (skippedBySuppression > 0) {
    console.log(`Spec ${notificationSpec.id}: ${skippedBySuppression} emails suppressed`);
  }

  const summaryRecipientCount = estimatedTotalRecipientCount ?? observedRecipientCount;
  let messageArray = [
    `Finished notification spec: ${notificationSpec.id}(${readableSpecString}). mail_stream: ${mailStream}, recipients: ${summaryRecipientCount}`,
  ];
  if (emailCount.total === 0) {
    messageArray.push("No emails sent");
  } else {
    for (const lang of langArray) {
      messageArray.push(`Email (${lang}) sent: ${emailCount[lang]}`);
    }
  }

  if (smsCount.total === 0) {
    messageArray.push("No SMS sent");
  } else {
    for (const lang of langArray) {
      messageArray.push(`SMS (${lang}) sent: ${smsCount[lang]}`);
    }
    if (totalFailed > 0) {
      messageArray.push(`SMS failed: ${totalFailed}`);
    }
  }

  console.log(messageArray.join(". ") + ".");

  return {
    specId: notificationSpec.id,
    category: notificationSpec.category,
    attempt: notificationSpec.attempt,
    mailStream,
    totalRecipients: summaryRecipientCount,
    emailsSent: emailCount.total,
    bulkLaneSentCounts: totalBulkLaneSentCounts,
    recommendedNextBulkDelayMs,
    smsSent: smsCount.total,
    smsFailed: totalFailed,
    suppressed: skippedBySuppression,
    hasNext,
    nextPreviousToken,
    batchNumber: options.batchNumber || 1,
    completedAt: new Date().toISOString(),
  };
}

const storeNotificationSchema = async (req, res, authObj) => {
  logIPAddress(req);
  setHeaders(res);

  if (req.method === "OPTIONS") return res.status(200).json({ code: 200 });

  if (req.method !== "POST") return res.status(405).json(getResponseJSON("Only POST requests are accepted!", 405));

  if (!authObj) return res.status(401).json(getResponseJSON("Authorization failed!", 401));

  if (req.body.data === undefined || Object.keys(req.body.data).length < 1)
    return res.status(400).json(getResponseJSON("Bad request.", 400));

  try {
    const schema = req.body.data;
    if (schema.id) {
      const { retrieveNotificationSchemaByID } = require("./firestore");
      const docID = await retrieveNotificationSchemaByID(schema.id);
      if (docID === "") return res.status(404).json(getResponseJSON("Invalid notification Id.", 404));

      const { updateNotificationSchema } = require("./firestore");
      schema["modifiedAt"] = new Date().toISOString();
      if (authObj.userEmail) schema["modifiedBy"] = authObj.userEmail;
      await updateNotificationSchema(docID, schema);
    } else {
      schema["id"] = uuid();
      const { storeNewNotificationSchema } = require("./firestore");
      schema["createdAt"] = new Date().toISOString();
      if (authObj.userEmail) schema["createdBy"] = authObj.userEmail;
      await storeNewNotificationSchema(schema);
    }

    return res.status(200).json({ message: "Success!", code: 200, data: [{ schemaId: schema.id }] });
  } catch (error) {
    console.error("Error occurred storing notification schema.", error);
    return res.status(500).json({ message: error.message, code: 500, data: [] });
  }

};

const retrieveNotificationSchema = async (req, res, authObj) => {
  logIPAddress(req);
  setHeaders(res);

  if (req.method === "OPTIONS") return res.status(200).json({ code: 200 });

  if (req.method !== "GET") return res.status(405).json(getResponseJSON("Only GET requests are accepted!", 405));

  if (!authObj) return res.status(401).json(getResponseJSON("Authorization failed!", 401));

  if (!req.query.category)
    return res.status(400).json(getResponseJSON("category is missing in request parameter!", 400));

  const category = req.query.category;
  const getDrafts = req.query.drafts === "true";
  const { retrieveNotificationSchemaByCategory } = require("./firestore");

  try {
    const schemaArray = await retrieveNotificationSchemaByCategory(category, getDrafts);
    if (schemaArray.length === 0)
      return res.status(404).json({ data: [], message: `Notification schema not found for given category - ${category}`, code: 404 });

    return res.status(200).json({ data: schemaArray, code: 200 });
  } catch (error) {
    console.error("Error retrieving notification schemas.", error);
    return res.status(500).json({ data: [], message: error.message, code: 500 });
  }
};

const getParticipantNotification = async (req, res, authObj) => {
    logIPAddress(req);
    setHeaders(res);

    if (req.method === 'OPTIONS') return res.status(200).json({code: 200});
        
    if (req.method !== 'GET') return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
    
    let obj = {};
    if (authObj) obj = authObj;
    else {
        const { APIAuthorization } = require('./shared');
        const authorized = await APIAuthorization(req);
        if(authorized instanceof Error){
            return res.status(500).json(getResponseJSON(authorized.message, 500));
        }
    
        if(!authorized){
            return res.status(401).json(getResponseJSON('Authorization failed!', 401));
        }
    
        const { isParentEntity } = require('./shared');
        obj = await isParentEntity(authorized);
    }

    if(!req.query.token) return res.status(400).json(getResponseJSON('token is missing in request parameter!', 400));
    const token = req.query.token;
    const isParent = obj.isParent;
    const siteCodes = obj.siteCodes;
    const { getNotificationHistoryByParticipant } = require('./firestore');
    const data = await getNotificationHistoryByParticipant(token, siteCodes, isParent);
    if(!data) return res.status(400).json(getResponseJSON('Invalid token or you are not authorized to access data for given token', 200));

    return res.status(200).json({data, code: 200})
}

const getSiteNotification = async (req, res, authObj) => {
    logIPAddress(req);
    setHeaders(res);

    if (req.method === 'OPTIONS') return res.status(200).json({code: 200});
        
    if (req.method !== 'GET') return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
    
    let obj = {};
    if (authObj) obj = authObj;
    else {
        const { APIAuthorization } = require('./shared');
        const authorized = await APIAuthorization(req);
        if(authorized instanceof Error){
            return res.status(500).json(getResponseJSON(authorized.message, 500));
        }
    
        if(!authorized){
            return res.status(401).json(getResponseJSON('Authorization failed!', 401));
        }
    
        const { isParentEntity } = require('./shared');
        obj = await isParentEntity(authorized);
    }

    const { retrieveSiteNotifications } = require('./firestore');
    const siteNotifications = await retrieveSiteNotifications(obj.siteCode, obj.isParent);
    if (siteNotifications.length > 0 ) {
        await markAllNotificationsAsAlreadyRead(siteNotifications.map(dt => dt.id), 'siteNotifications');
    }
    
    return res.status(200).json({data: siteNotifications, code: 200})
}

const resolveAuthErrorDetails = ({ status, errorCode, providerErrorCode = "" } = {}) => {
    const normalizedProviderErrorCode = String(providerErrorCode || "").toLowerCase();
    const hasNumericStatus = Number.isFinite(status);
    const graphMappedErrorCode = (() => {
        if (!hasNumericStatus) return null;
        if (status === 429) return "auth/too-many-requests";
        if (status === 400 && normalizedProviderErrorCode.includes("invalid")) return "auth/invalid-email";
        if (status === 401 || status === 403) return "auth/operation-not-allowed";
        if (status >= 500) return "auth/network-request-failed";
        return "auth/operation-not-allowed";
    })();

    const resolvedErrorCode = errorCode || graphMappedErrorCode || "auth/network-request-failed";
    const resolvedStatus = hasNumericStatus ? status : (() => {
        if (resolvedErrorCode === "auth/too-many-requests") return 429;
        if (
            [
                "auth/invalid-email",
                "auth/missing-email",
                "auth/missing-continue-uri",
                "auth/unauthorized-continue-uri",
                "auth/invalid-continue-uri",
            ].includes(resolvedErrorCode)
        ) {
            return 400;
        }
        return 500;
    })();

    return {
        status: resolvedStatus,
        errorCode: resolvedErrorCode,
    };
};

const getGraphAccessToken = async () => {
    const [clientId, clientSecret, tenantId] = await Promise.all(
        [
            getSecret(process.env.APP_REGISTRATION_CLIENT_ID),
            getSecret(process.env.APP_REGISTRATION_CLIENT_SECRET),
            getSecret(process.env.APP_REGISTRATION_TENANT_ID),
        ]
    );

    const params = new URLSearchParams();
    params.append("grant_type", "client_credentials");
    params.append("scope", "https://graph.microsoft.com/.default");
    params.append("client_id", clientId);
    params.append("client_secret", clientSecret);

    const resAuthorize = await fetch(
        `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`,
        {
            method: "POST",
            headers: { "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8" },
            body: params,
        }
    );

    const authJson = await parseResponseJson(resAuthorize);
    const accessToken = authJson?.access_token;

    if (!resAuthorize.ok || !accessToken) {
        const error = new Error("Failed to obtain Microsoft Graph access token.");
        error.status = 502;
        error.errorCode = "auth/operation-not-allowed";
        error.upstreamStatus = resAuthorize.status;
        error.upstream = authJson;
        throw error;
    }

    return accessToken;
};

/**
 * Escapes a value for safe insertion into OData single-quoted string literals.
 * OData uses single quotes to delimit string values; embedded single quotes must be doubled.
 * Example: O'Connor -> O''Connor.
 *
 * @param {string} [value=""] - Raw string value to include in an OData filter.
 * @returns {string} Value escaped per OData string-literal rules (`'` -> `''`).
 */
const escapeODataString = (value = "") => String(value).replace(/'/g, "''");

/**
 * Sends Firebase magic-link authentication email through Microsoft Graph.
 * Keep this Outlook/M365-aligned path separate from bulk mail so auth email
 * reputation is not coupled to SendGrid campaign reputation.
 *
 * @param {Object} req - HTTP request body.
 * @param {Object} res - HTTP response body.
 * @returns {Promise<void>} Resolves after response is sent.
 */
const sendEmailLink = async (req, res) => {
    if (req.method !== "POST") {
        return res.status(405).json(getResponseJSON("Only POST requests are accepted!", 405));
    }

    const requestBody = parseRequestBody(req.body);
    const {
        email,
        continueUrl,
        preferredLanguage,
        authFlowId,
        authAttemptId,
        clientSendTs,
    } = requestBody;
    const resolvedAuthFlowId = authFlowId || `auth_flow_${uuid()}`;
    const resolvedAuthAttemptId = authAttemptId || `auth_attempt_${uuid()}`;

    try {
        if (!email) {
            return res.status(400).json({
                data: [],
                message: "Missing required field: email.",
                status: 400,
                code: 400,
                errorCode: "auth/missing-email",
                authFlowId: resolvedAuthFlowId,
                authAttemptId: resolvedAuthAttemptId,
            });
        }

        if (!continueUrl) {
            return res.status(400).json({
                data: [],
                message: "Missing required field: continueUrl.",
                status: 400,
                code: 400,
                errorCode: "auth/missing-continue-uri",
                authFlowId: resolvedAuthFlowId,
                authAttemptId: resolvedAuthAttemptId,
            });
        }

        if (!validEmailFormat.test(email)) {
            return res.status(400).json({
                data: [],
                message: "Invalid email format.",
                status: 400,
                code: 400,
                errorCode: "auth/invalid-email",
                authFlowId: resolvedAuthFlowId,
                authAttemptId: resolvedAuthAttemptId,
            });
        }

        const magicLink = await generateSignInWithEmailLink(email, continueUrl);

        const cleanMagicLink = cleanContinueUrl(magicLink);
        const accessToken = await getGraphAccessToken();

        const body = {
            message: {
                subject:
                    preferredLanguage === conceptIds.spanish
                        ? "Inicie sesión para Estudio Connect para la Prevención del Cáncer"
                        : "Sign in to Connect for Cancer Prevention Study",
                body: {
                    contentType: "html",
                    content: getTemplateForEmailLink(
                        email,
                        cleanMagicLink,
                        preferredLanguage
                    ),
                },
                toRecipients: [
                    {
                        emailAddress: {
                            address: email,
                        },
                    },
                ],
            },
        };
        const graphClientRequestId = uuid();

        const graphResponse = await fetch(
            `https://graph.microsoft.com/v1.0/users/${nihMailbox}/sendMail`,
            {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    Authorization: `Bearer ${accessToken}`,
                    "client-request-id": graphClientRequestId,
                    "return-client-request-id": "true",
                },
                body: JSON.stringify(body),
            }
        );

        const graphJson = await parseResponseJson(graphResponse);
        const graphRequestId = graphResponse.headers.get("request-id") || null;
        const graphClientRequestIdEcho = graphResponse.headers.get("client-request-id") || graphClientRequestId;

        if (!graphResponse.ok) {
            const providerErrorCode = graphJson?.error?.code || "";
            const resolvedError = resolveAuthErrorDetails({
                status: graphResponse.status,
                providerErrorCode,
            });
            return res.status(graphResponse.status).json({
                data: [],
                message: graphJson?.error?.message || "Failed to send email via Microsoft Graph.",
                status: graphResponse.status,
                code: graphResponse.status,
                errorCode: resolvedError.errorCode,
                provider: "microsoft_graph",
                providerStatus: "failed",
                providerErrorCode: providerErrorCode || null,
                graphRequestId,
                graphClientRequestId: graphClientRequestIdEcho,
                authFlowId: resolvedAuthFlowId,
                authAttemptId: resolvedAuthAttemptId,
                clientSendTs: clientSendTs || null,
                serverSendTs: new Date().toISOString(),
            });
        }

        return res.status(202).json({
            status: 202,
            code: 202,
            errorCode: null,
            message: "Email accepted for delivery.",
            provider: "microsoft_graph",
            providerStatus: "accepted",
            messageId: graphJson?.id || null,
            graphRequestId,
            graphClientRequestId: graphClientRequestIdEcho,
            authFlowId: resolvedAuthFlowId,
            authAttemptId: resolvedAuthAttemptId,
            clientSendTs: clientSendTs || null,
            serverSendTs: new Date().toISOString(),
        });
        
    } catch (err) {
        console.error(`Error in sendEmailLink(). ${err.message}`);
        const resolvedError = resolveAuthErrorDetails({
            status: err.status,
            errorCode: err.errorCode || err.code,
        });
        return res.status(resolvedError.status).json({
            data: [],
            message: `Error in sendEmailLink(). ${err.message}`,
            status: resolvedError.status,
            code: resolvedError.status,
            errorCode: resolvedError.errorCode,
            provider: "microsoft_graph",
            providerStatus: "failed",
            upstreamStatus: err.upstreamStatus || null,
            upstream: err.upstream || null,
            authFlowId: resolvedAuthFlowId,
            authAttemptId: resolvedAuthAttemptId,
            clientSendTs: clientSendTs || null,
            serverSendTs: new Date().toISOString(),
        });
    }
};

/**
 * Properly cleans the continueUrl parameter by finding where it should end
 * @param {string} url - The full authentication URL
 * @returns {string} - URL with cleaned continueUrl parameter
 */
const cleanContinueUrl = (url) => {
    const normalizedUrl = url.replace(/&amp;/g, '&');
    const continueUrlIndex = normalizedUrl.indexOf('continueUrl=');
    
    if (continueUrlIndex === -1) {
        return url;
    }

    const beforeContinueUrl = normalizedUrl.substring(0, continueUrlIndex + 'continueUrl='.length);
    const afterContinueUrlStart = normalizedUrl.substring(continueUrlIndex + 'continueUrl='.length);
    const baseUrlMatch = afterContinueUrlStart.match(/^(https:\/\/[^&#]+)/);
    
    if (!baseUrlMatch) {
        return url;
    }
    
    const baseUrl = baseUrlMatch[1];

    return beforeContinueUrl + baseUrl;
}

const dryRunNotificationSchema = async (req, res) => {
  if (req.method !== "GET") {
    return res.status(405).json({ data: [], message: "Only GET requests are accepted!", code: 405 });
  }

  if (!req.query.schemaId) {
    return res.status(400).json({ data: [], message: "schemaId is missing in request parameter!", code: 400 });
  }

  let spec = null;
  try {
    spec = await getNotificationSpecById(req.query.schemaId);
    if (!spec) {
      const message = `Notification spec ID ${req.query.schemaId} isn't found.`;
      return res.status(404).json({ data: [], message, code: 404 });
    }
    const { data, message, code } = await handleDryRun(spec);
    return res.status(code).json({ data, message, code });

  } catch (error) {
    return res.status(500).json({ data: [], message: JSON.stringify(error, null, 2), code: 500 });
  }

};

async function handleDryRun(spec) {
  const timeParams = getTimeParams(spec);
  if (!timeParams) return { data: [], message: "Ok", code: 200 };

  const emailInSpec = spec.email || {};
  const smsInSpec = spec.sms || {};
  const emailField = spec.emailField ?? "";
  const phoneField = spec.phoneField ?? "";
  let fieldsToFetch = ["token", "Connect_ID", conceptIds.preferredLanguage.toString()];
  emailField && fieldsToFetch.push(emailField);
  phoneField && fieldsToFetch.push(phoneField);
  spec.notificationType.includes("sms") && fieldsToFetch.push(conceptIds.canWeText.toString());

  const limit = 1000;
  let previousToken = "";
  let hasNext = true;
  let fetchedDataArray = [];
  let countObj = { email: {}, sms: {} };
  let conditions = [];
  if (spec.conditions) {
    conditions = JSON.parse(spec.conditions);
  }

  for (const lang of langArray) {
    countObj.email[lang] = 0;
    countObj.sms[lang] = 0;
  }

  while (hasNext) {
    try {
      fetchedDataArray = await getParticipantsForNotificationsBQ({
        notificationSpecId: spec.id,
        startTimeStr: timeParams.startTimeStr,
        stopTimeStr: timeParams.stopTimeStr,
        timeField: timeParams.timeField,
        conditions,
        fieldsToFetch,
        limit,
        previousToken,
      });
    } catch (error) {
      console.error(`Error dry running spec ID ${spec.id}.`, error);
      return { data: [countObj], message: JSON.stringify(error, null, 2), code: 500 };
    }

    if (fetchedDataArray.length === 0) break;
    hasNext = fetchedDataArray.length === limit;
    if (hasNext) {
      previousToken = fetchedDataArray[fetchedDataArray.length - 1].token;
    }

    for (const fetchedData of fetchedDataArray) {
      if (!fetchedData[emailField] && !fetchedData[phoneField]) continue;

      const prefLang = cidToLangMapper[fetchedData[conceptIds.preferredLanguage]] || "english";

      if (emailInSpec[prefLang] && validEmailFormat.test(fetchedData[emailField])) {
        countObj.email[prefLang]++;
      }

      if (
        smsInSpec[prefLang] &&
        fetchedData[phoneField]?.length >= 10 &&
        fetchedData[conceptIds.canWeText] === conceptIds.yes
      ) {
        countObj.sms[prefLang]++;
      }
    }
  }

  return { data: [countObj], message: "Ok", code: 200 };
}

const sendInstantNotification = async (requestData) => {
  const notificationSettings = await getNotificationDeliverySettings();
  const notificationSpec = await getNotificationSpecByCategoryAndAttempt(requestData.category, requestData.attempt);
  const errMsg = `Error sending instant notification (${requestData.category}, ${requestData.attempt}) to participant with ID ${requestData.connectId}`;

  if (!notificationSpec) {
    throw new Error(`${errMsg}. Notification spec not found.`);
  }

  const isNotificationSent = await checkIsNotificationSent(requestData.token, notificationSpec.id);
  if (isNotificationSent) {
    throw new Error(`${errMsg}. Notification already sent.`);
  }

  const normalizedEmail = normalizeEmailAddress(requestData.email);
  if (normalizedEmail && shouldFilterEmailAddress(normalizedEmail)) {
    console.log(`Instant notification filtered for ${normalizedEmail}`);
    return;
  }
  if (normalizedEmail && await isEmailSuppressed(normalizedEmail, "transactional")) {
    console.log(`Instant notification suppressed for ${normalizedEmail}`);
    return;
  }

  const emailOfPrefLang = notificationSpec.email[requestData.preferredLanguage] || notificationSpec.email.english;
  const currEmailBody = emailOfPrefLang.body
    .replace(/{{firstName}}/g, requestData.substitutions.firstName)
    .replace(/{{loginDetails}}/g, requestData.substitutions.loginDetails);
  const notificationId = getNotificationRecordId({
    notificationSpecificationsID: notificationSpec.id,
    notificationType: "email",
    token: requestData.token,
  });
  const providerAttemptOwner = `instant-${notificationId}`;

  const emailDataToSg = {
    from: {
      name: process.env.SG_FROM_NAME || "Connect for Cancer Prevention Study",
      email: process.env.SG_FROM_EMAIL || "no-reply-myconnect@mail.nih.gov",
    },
    subject: emailOfPrefLang.subject,
    html: emailOfPrefLang.body,
    text: htmlToPlaintext(emailOfPrefLang.body),
    personalizations: [
      {
        to: requestData.email,
        substitutions: requestData.substitutions,
        custom_args: {
          connect_id: requestData.connectId,
          token: requestData.token,
          notification_id: notificationId,
          gcloud_project: process.env.GCLOUD_PROJECT,
          mail_stream: "transactional",
        },
      },
    ],
  };

  const currEmailRecord = {
    id: notificationId,
    notificationType: "email",
    language: requestData.preferredLanguage,
    email: requestData.email,
    notification: {
      title: emailOfPrefLang.subject,
      body: currEmailBody,
      time: new Date().toISOString(),
    },
    notificationSpecificationsID: notificationSpec.id,
    attempt: requestData.attempt,
    category: requestData.category,
    Connect_ID: requestData.connectId,
    token: requestData.token,
    uid: requestData.uid,
    read: false,
  };

  try {
    await deliverReservedEmailRecords({
      emailRecords: [currEmailRecord],
      providerAttemptOwner,
      notificationSettings,
      sendReservedRecords: async () => {
        await sendViaSendGrid(emailDataToSg, {
          logLabel: `instant notification ${notificationSpec.id} for ${requestData.token}`,
          notificationSettings,
        });
      },
    });
  } catch (err) {
    throw new Error(errMsg, { cause: err });
  }
};

const validateTwilioRequest = async (req) => {
  if (!isTwilioSetup) {
    await setupTwilio();
  }
  const twilioSignature = req.headers["x-twilio-signature"];
  const requestUrl = `https://${req.get("host")}${req.originalUrl}`;
  if (!twilio.validateRequest(twilioAuthToken, twilioSignature, requestUrl, req.body)) {
    console.warn(`Twilio request validation failed. twilioSignature: ${twilioSignature}, requestUrl: ${requestUrl}`);
    return false;
  }

  return true;
};

const handleIncomingSms = async (req, res) => {
  const isRequestValid = await validateTwilioRequest(req);
  if (!isRequestValid) {
    return res.status(403).json(getResponseJSON("Invalid Twilio signature.", 403));
  }

  const smsData = req.body;
  try {
    await storeIncomingSmsData(smsData);
    const { tokens, preferredLanguage } = await getTokensAndPreferredLanguageByPhone(smsData.From);

    const optOutType = smsData.OptOutType || "";
    if (["START", "STOP"].includes(optOutType)) {
      const isSmsAllowed = optOutType === "START";
      await updateSmsPermission(tokens, isSmsAllowed);
    } else if (optOutType !== "HELP") {
      const isSpanishPreferred = preferredLanguage === conceptIds.spanish;
      const replyMessage = isSpanishPreferred
        ? "Para ayuda, visite MyConnect.cancer.gov/support. Responda PARAR para cancelar su suscripción."
        : "For help, visit MyConnect.cancer.gov/support. Reply STOP to unsubscribe.";
      const messagingResponse = new twilio.twiml.MessagingResponse();
      messagingResponse.message(replyMessage);

      return res.status(200).type("text/xml").send(messagingResponse.toString());
    }
  } catch (error) {
      console.error("Error handling incoming message.", error);
      return res.status(500).json(getResponseJSON(error.message || "Internal server error", 500));
  }

  return res.sendStatus(204);
};

const processPlannedBulkNotificationBatch = async (expectedLane, req) => {
  const notificationSettings = await getNotificationDeliverySettings();
  const {
    runId = "",
    batchId = "",
    lane = expectedLane,
    specId = "",
    runDateKey = getEasternDateKey(),
    runSequence = 1,
  } = req.data || {};

  if (!runId || !batchId) {
    throw new Error("processPlannedBulkNotificationBatch: missing runId or batchId");
  }
  if (lane !== expectedLane) {
    throw new Error(`processPlannedBulkNotificationBatch: expected lane ${expectedLane}, received ${lane}`);
  }

  const planned = await getBulkNotificationBatch(runId, batchId);
  if (!planned?.run || !planned?.batch) {
    throw new Error(`processPlannedBulkNotificationBatch: planned batch not found for ${runId}/${batchId}`);
  }

  const { run, batch } = planned;
  const notificationSpec = run.notificationSpec;
  if (!notificationSpec?.id) {
    throw new Error(`processPlannedBulkNotificationBatch: missing notificationSpec for run ${runId}`);
  }
  if (specId && specId !== notificationSpec.id) {
    throw new Error(`processPlannedBulkNotificationBatch: task spec ${specId} does not match run spec ${notificationSpec.id}`);
  }
  if (batch.status === "complete") {
    console.log(`[Cloud Task] Planned bulk ${lane} batch ${batchId} for run ${runId} is already complete.`);
    await finalizeBulkNotificationRunIfTerminal(runId);
    return { skipped: true, skipReason: "batch_already_complete", runId, batchId };
  }
  if (batch.status === "failed") {
    console.log(`[Cloud Task] Planned bulk ${lane} batch ${batchId} for run ${runId} is already failed.`);
    await finalizeBulkNotificationRunIfTerminal(runId);
    return { skipped: true, skipReason: "batch_already_failed", runId, batchId };
  }

  const retryCount = Number.isFinite(req.retryCount) ? req.retryCount : 0;
  const taskId = req.id || buildPlannedBulkNotificationTaskId(runId, lane, batch.batchNumber || 1);
  const taskAttemptOwner = `${taskId}-attempt-${retryCount}`;
  console.log(`[Cloud Task] Processing planned bulk ${lane} batch ${batchId} for run ${runId}`);
  const collector = createBulkOutcomeCollector(batch.unsuccessful);
  const didClaimBatch = await markBulkNotificationBatchRunning({
    runId,
    batchId,
    taskAttemptOwner,
    attemptDurationMs: BULK_TASK_ATTEMPT_LOCK_MS,
  });
  if (!didClaimBatch) {
    console.log(`[Cloud Task] Planned bulk ${lane} batch ${batchId} for run ${runId} was not claimed for running state.`);
    const latestPlanned = await getBulkNotificationBatch(runId, batchId);
    if (latestPlanned?.batch?.status === "running") {
      const activeAttemptError = new Error(`Planned bulk ${lane} batch ${batchId} for run ${runId} is already owned by an active task attempt.`);
      activeAttemptError.code = "ACTIVE_BULK_BATCH_ATTEMPT";
      throw activeAttemptError;
    }
    await finalizeBulkNotificationRunIfTerminal(runId);
    return { skipped: true, skipReason: "batch_claim_rejected", runId, batchId };
  }

  try {
    if (getSendGridDeliveryMode(notificationSettings) !== "noop") {
      await setupSendGrid();
    }

    const result = await handleNotificationSpec(notificationSpec, {
      singleBatch: true,
      plannedRecipients: batch.recipients || [],
      emailOnly: true,
      mailStream: "bulk",
      totalRecipientCount: batch.recipientCount || 0,
      timeParams: run.timeParams,
      conditions: deserializePlannedBulkConditions(run.conditions || []),
      batchNumber: batch.batchNumber || 1,
      providerAttemptOwner: `${runId}-${batchId}`,
      smsBatchSender: new SmsBatchSender({}),
      notificationSettings,
      outcomeCollector: collector,
    });

    await markBulkNotificationBatchComplete({
      runId,
      batchId,
      taskAttemptOwner,
      ...summarizeBulkOutcome({
        planned: batch.recipientCount || (batch.recipients || []).length,
        sent: result.emailsSent || collector.sent || 0,
        collector,
      }),
    });
    await finalizeBulkNotificationRunIfTerminal(runId);
    console.log(`[Cloud Task] Completed planned bulk ${lane} batch ${batchId} for run ${runId}.`);
    return result;
  } catch (error) {
    const isFinalAttempt = retryCount + 1 >= notificationSettings.bulkTaskMaxAttempts;
    if (isFinalAttempt) {
      await markBulkNotificationBatchFailed({
        runId,
        batchId,
        taskAttemptOwner,
        error,
        ...summarizeBulkOutcome({
          planned: batch.recipientCount || (batch.recipients || []).length,
          sent: collector.sent || 0,
          collector,
        }),
      });
      await finalizeBulkNotificationRunIfTerminal(runId);
    }
    throw error;
  }
};

const processNotificationBatchBulkDefault = (req) =>
  processPlannedBulkNotificationBatch(BULK_LANE_DEFAULT, req);

const processNotificationBatchBulkMicrosoft = (req) =>
  processPlannedBulkNotificationBatch(BULK_LANE_MICROSOFT, req);

module.exports = {
  subscribeToNotification,
  retrieveNotifications,
  sendScheduledNotifications,
  storeNotificationSchema,
  retrieveNotificationSchema,
  getParticipantNotification,
  sendEmail,
  dedupeCcAgainstTo,
  getSiteNotification,
  sendEmailLink,
  dryRunNotificationSchema,
  sendInstantNotification,
  handleDryRun,
  handleIncomingSms,
  SmsBatchSender,
  validateTwilioRequest,
  handleNotificationSpec,
  generateUnsubscribeSignature,
  resolveUnsubscribeSecret,
  processNotificationBatchBulkDefault,
  processNotificationBatchBulkMicrosoft,
  processPlannedBulkNotificationBatch,
  buildBulkWorkerUri,
  validateBulkWorkerUri,
};
