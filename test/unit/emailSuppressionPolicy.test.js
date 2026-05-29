const {
  normalizeEmailAddress,
  normalizeSuppressionKey,
  shouldFilterEmailAddress,
  getEmailSuppressionPolicyByReason,
  getEmailSuppressionPolicyForSendGridEvent,
  getEmailSuppressionPolicyForImportType,
  buildEmailSuppressionDoc,
} = require("../../utils/emailSuppressionPolicy");

describe("emailSuppressionPolicy", () => {
  describe("normalizeEmailAddress", () => {
    it("should trim and lowercase string email values", () => {
      expect(normalizeEmailAddress("  User@Example.COM  ")).toBe("user@example.com");
    });

    it("should return an empty string for non-string values", () => {
      expect(normalizeEmailAddress(null)).toBe("");
      expect(normalizeEmailAddress(undefined)).toBe("");
      expect(normalizeEmailAddress({ email: "user@example.com" })).toBe("");
    });
  });

  describe("normalizeSuppressionKey", () => {
    it("should trim, lowercase, and normalize separators in suppression labels", () => {
      expect(normalizeSuppressionKey(" Spam Reports ")).toBe("spam_reports");
      expect(normalizeSuppressionKey("group-unsubscribes")).toBe("group_unsubscribes");
    });
  });

  describe("shouldFilterEmailAddress", () => {
    it("should filter hard-hygiene no-reply and placeholder patterns", () => {
      expect(shouldFilterEmailAddress("noreply@nih.gov")).toBe(true);
      expect(shouldFilterEmailAddress("do-not-reply@nih.gov")).toBe(true);
      expect(shouldFilterEmailAddress("user@example.com")).toBe(true);
      expect(shouldFilterEmailAddress("user@sub.localhost")).toBe(true);
      expect(shouldFilterEmailAddress("user@episphere.github.io")).toBe(true);
      expect(shouldFilterEmailAddress("user@bad.com.com")).toBe(true);
    });

    it("should not filter normal recipient addresses", () => {
      expect(shouldFilterEmailAddress("user@example.gov")).toBe(false);
      expect(shouldFilterEmailAddress("participant@nih.gov")).toBe(false);
    });

    it("should filter empty or malformed strings (no `@`)", () => {
      // Prevents buildEmailSuppressionDoc from persisting invalid strings as Firestore doc ids in emailAddressStatus.
      expect(shouldFilterEmailAddress("")).toBe(true);
      expect(shouldFilterEmailAddress("   ")).toBe(true);
      expect(shouldFilterEmailAddress("not-an-email")).toBe(true);
      expect(shouldFilterEmailAddress("missing-at-sign.example.com")).toBe(true);
    });
  });

  describe("getEmailSuppressionPolicyByReason", () => {
    it("should return the expected stream flags for known reasons", () => {
      expect(getEmailSuppressionPolicyByReason("hard_bounce")).toEqual({
        reason: "hard_bounce",
        suppressBulk: true,
        suppressTransactional: true,
      });
      expect(getEmailSuppressionPolicyByReason("unsubscribed")).toEqual({
        reason: "unsubscribed",
        suppressBulk: true,
        suppressTransactional: false,
      });
      expect(getEmailSuppressionPolicyByReason("global_unsubscribe")).toEqual({
        reason: "global_unsubscribe",
        suppressBulk: true,
        suppressTransactional: true,
      });
      expect(getEmailSuppressionPolicyByReason("legacy_global_unsubscribe")).toEqual({
        reason: "legacy_global_unsubscribe",
        suppressBulk: true,
        suppressTransactional: true,
      });
    });

    it("should return null for unknown reasons", () => {
      expect(getEmailSuppressionPolicyByReason("unknown_reason")).toBeNull();
    });
  });

  describe("getEmailSuppressionPolicyForSendGridEvent", () => {
    it("should classify the supported SendGrid suppression events", () => {
      expect(getEmailSuppressionPolicyForSendGridEvent({
        event: "bounce",
        type: "bounce",
      })?.reason).toBe("hard_bounce");

      expect(getEmailSuppressionPolicyForSendGridEvent({
        event: "spamreport",
      })?.reason).toBe("spam_report");

      expect(getEmailSuppressionPolicyForSendGridEvent({
        event: "dropped",
        reason: "Invalid recipient address",
      })?.reason).toBe("invalid_email");

      expect(getEmailSuppressionPolicyForSendGridEvent({
        event: "dropped",
        reason: "Bounced Address",
      })?.reason).toBe("hard_bounce");

      expect(getEmailSuppressionPolicyForSendGridEvent({
        event: "dropped",
        reason: "Spam Reporting Address",
      })?.reason).toBe("spam_report");

      expect(getEmailSuppressionPolicyForSendGridEvent({
        event: "dropped",
        reason: "Unsubscribed Address",
      })?.reason).toBe("global_unsubscribe");

      expect(getEmailSuppressionPolicyForSendGridEvent({
        event: "unsubscribe",
      })?.reason).toBe("global_unsubscribe");

      expect(getEmailSuppressionPolicyForSendGridEvent({
        event: "group_unsubscribe",
      })?.reason).toBe("unsubscribed");

      expect(getEmailSuppressionPolicyForSendGridEvent({
        event: "bounce",
        type: "blocked",
      })?.reason).toBe("blocked");
      expect(getEmailSuppressionPolicyForSendGridEvent({
        event: "blocked",
      })?.reason).toBe("blocked");

      // Bounce events without a `type` field (or with an unknown type) must
      // default to hard_bounce so we never silently lose a real bounce.
      expect(getEmailSuppressionPolicyForSendGridEvent({
        event: "bounce",
      })?.reason).toBe("hard_bounce");
      expect(getEmailSuppressionPolicyForSendGridEvent({
        event: "bounce",
        type: "",
      })?.reason).toBe("hard_bounce");
      expect(getEmailSuppressionPolicyForSendGridEvent({
        event: "bounce",
        type: "future-unknown-type",
      })?.reason).toBe("hard_bounce");

      // Dropped-reason precedence: bounce wins over invalid when both
      // substrings are present (most-specific suppression first).
      expect(getEmailSuppressionPolicyForSendGridEvent({
        event: "dropped",
        reason: "Bounced address - invalid format",
      })?.reason).toBe("hard_bounce");
      expect(getEmailSuppressionPolicyForSendGridEvent({
        event: "dropped",
        reason: "Spam Reporting Address (also marked invalid)",
      })?.reason).toBe("spam_report");
    });

    it("should return null for non-suppressing SendGrid events", () => {
      expect(getEmailSuppressionPolicyForSendGridEvent({
        event: "delivered",
      })).toBeNull();
      expect(getEmailSuppressionPolicyForSendGridEvent({
        event: "dropped",
        reason: "Mailbox unavailable",
      })).toBeNull();
    });
  });

  describe("getEmailSuppressionPolicyForImportType", () => {
    it("should map backfill import types to the same suppression policy table", () => {
      expect(getEmailSuppressionPolicyForImportType("bounces")).toEqual({
        reason: "hard_bounce",
        suppressBulk: true,
        suppressTransactional: true,
      });
      expect(getEmailSuppressionPolicyForImportType("global_unsubscribes")).toEqual({
        reason: "global_unsubscribe",
        suppressBulk: true,
        suppressTransactional: true,
      });
      expect(getEmailSuppressionPolicyForImportType(" GLOBAL_UNSUBSCRIBES ")).toEqual({
        reason: "global_unsubscribe",
        suppressBulk: true,
        suppressTransactional: true,
      });
      expect(getEmailSuppressionPolicyForImportType("legacy_global_unsubscribes")).toEqual({
        reason: "legacy_global_unsubscribe",
        suppressBulk: true,
        suppressTransactional: true,
      });
      expect(getEmailSuppressionPolicyForImportType("spam_reports")).toEqual({
        reason: "spam_report",
        suppressBulk: true,
        suppressTransactional: true,
      });
      expect(getEmailSuppressionPolicyForImportType("invalid_emails")).toEqual({
        reason: "invalid_email",
        suppressBulk: true,
        suppressTransactional: true,
      });
      expect(getEmailSuppressionPolicyForImportType("group_unsubscribes")).toEqual({
        reason: "unsubscribed",
        suppressBulk: true,
        suppressTransactional: false,
      });
      expect(getEmailSuppressionPolicyForImportType("bounce")).toBeNull();
      expect(getEmailSuppressionPolicyForImportType("unsubscribe")).toBeNull();
      expect(getEmailSuppressionPolicyForImportType("invalid emails")).toBeNull();
      expect(getEmailSuppressionPolicyForImportType("blocks")).toBeNull();
    });

    it("should return null for unsupported backfill import types", () => {
      expect(getEmailSuppressionPolicyForImportType("mystery")).toBeNull();
    });
  });

  describe("buildEmailSuppressionDoc", () => {
    it("should build a monotonic suppression doc and omit false flags", () => {
      const doc = buildEmailSuppressionDoc({
        email: "  User@Example.GOV  ",
        reason: "unsubscribed",
        notificationId: "notif-123",
        token: "tok-123",
      });

      expect(doc).toEqual({
        normalizedEmail: "user@example.gov",
        status: "suppressed",
        reason: "unsubscribed",
        lastEventAt: expect.any(String),
        lastNotificationId: "notif-123",
        manualOverride: false,
        token: "tok-123",
        suppressBulk: true,
      });
      expect(doc).not.toHaveProperty("suppressTransactional");
    });

    it("should honor an explicit policy object and preserve caller overrides", () => {
      const doc = buildEmailSuppressionDoc({
        normalizedEmail: "user@example.gov",
        policy: {
          reason: "manual_review",
          suppressBulk: false,
          suppressTransactional: true,
        },
        status: "review",
        manualOverride: true,
        notificationId: null,
        lastEventAt: "2026-04-14T12:00:00.000Z",
      });

      expect(doc).toEqual({
        normalizedEmail: "user@example.gov",
        status: "review",
        reason: "manual_review",
        lastEventAt: "2026-04-14T12:00:00.000Z",
        lastNotificationId: null,
        manualOverride: true,
        suppressTransactional: true,
      });
    });

    it("should return null when there is no normalized email or no reason", () => {
      expect(buildEmailSuppressionDoc({ email: "", reason: "hard_bounce" })).toBeNull();
      expect(buildEmailSuppressionDoc({ email: "user@example.com" })).toBeNull();
    });

    it("should return null for filtered internal or malformed recipient addresses", () => {
      expect(buildEmailSuppressionDoc({ email: "noreply@nih.gov", reason: "hard_bounce" })).toBeNull();
      expect(buildEmailSuppressionDoc({ email: "bad@domain.com.com", reason: "hard_bounce" })).toBeNull();
    });
  });
});
