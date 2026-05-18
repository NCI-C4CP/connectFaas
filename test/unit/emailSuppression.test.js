/**
 * Email Suppression System Tests: emailAddressStatus Firestore collection and suppression logic.
 */

const FirestoreMocks = require("../mocks/core/firestoreMocks");
const MockHelpers = require("../mocks/helpers/mockHelpers");

const firestoreMocks = new FirestoreMocks();
const mockHelpers = new MockHelpers();

// Mock firebase-admin
const mockAdmin = {
  initializeApp: vi.fn(),
  firestore: vi.fn(() => firestoreMocks.mockFirestore),
  storage: vi.fn(() => ({
    bucket: vi.fn().mockReturnValue({
      file: vi.fn().mockReturnValue({
        save: vi.fn().mockResolvedValue(undefined),
        download: vi.fn().mockResolvedValue([Buffer.from("")]),
        exists: vi.fn().mockResolvedValue([false]),
      }),
    }),
  })),
  auth: vi.fn(() => ({
    verifyIdToken: vi.fn().mockResolvedValue({ uid: "test-uid" }),
  })),
};

const restoreModuleMocks = mockHelpers.setupModuleMocks(mockAdmin);
require("../../utils/shared");
require("../../utils/bigquery");

// Mock shared.js to only the functions firestore.js imports
const sharedPath = require.resolve("../../utils/shared");
const origSharedExports = require.cache[sharedPath].exports;
const sharedMock = {
  ...origSharedExports,
  delay: vi.fn().mockResolvedValue(undefined),
};
require.cache[sharedPath].exports = sharedMock;

// Mock bigquery.js to add phone-token lookup used by firestore.js
const bigqueryPath = require.resolve("../../utils/bigquery");
const origBigqueryExports = require.cache[bigqueryPath].exports;
const bigqueryMock = {
  ...origBigqueryExports,
  getParticipantTokensByPhoneNumber: vi.fn().mockResolvedValue([]),
};
require.cache[bigqueryPath].exports = bigqueryMock;

// Add getAll to the Firestore mock to support batch lookups
firestoreMocks.mockFirestore.getAll = vi.fn().mockResolvedValue([]);
const firestorePath = require.resolve("../../utils/firestore");
delete require.cache[firestorePath];
const firestoreModule = require("../../utils/firestore");

describe("Email Suppression System", () => {
  // Shared across queued-run, bulk-run, and state-machine scenarios.
  const STALE_RUN_DATE_KEY = "2026-04-01";
  const STALE_RUN_UPDATED_AT = "2026-04-01T15:00:00.000Z";

  beforeEach(() => {
    // Clear overrides.
    firestoreMocks._collectionOverrides.clear();
    vi.clearAllMocks();
    sharedMock.delay.mockResolvedValue(undefined);
    firestoreMocks.mockFirestore.getAll.mockReset().mockResolvedValue([]);
  });

  afterAll(() => {
    require.cache[sharedPath].exports = origSharedExports;
    require.cache[bigqueryPath].exports = origBigqueryExports;
    delete require.cache[firestorePath];
    restoreModuleMocks();
  });

  // addEmailSuppression
  describe("addEmailSuppression", () => {
    let docSetSpy;
    let collectionDocSpy;
    let existingDocData;

    const setupEmailCollection = () => {
      existingDocData = null;
      docSetSpy = vi.fn();
      collectionDocSpy = vi.fn().mockImplementation((docId) => ({
        id: docId,
        get: vi.fn().mockResolvedValue({
          exists: existingDocData !== null,
          data: () => existingDocData,
        }),
        set: vi.fn().mockResolvedValue(undefined),
        update: vi.fn().mockResolvedValue(undefined),
        delete: vi.fn().mockResolvedValue(undefined),
      }));

      firestoreMocks._collectionOverrides.set("emailAddressStatus", {
        doc: collectionDocSpy,
        where: vi.fn().mockReturnValue({
          get: vi.fn().mockResolvedValue({ empty: true, size: 0, docs: [] }),
        }),
      });

      firestoreMocks.mockFirestore.runTransaction.mockReset().mockImplementation(async (updateFunction) => {
        const transaction = {
          get: vi.fn().mockResolvedValue({
            exists: existingDocData !== null,
            data: () => existingDocData,
          }),
          getAll: vi.fn().mockResolvedValue([]),
          set: docSetSpy,
          update: vi.fn(),
          delete: vi.fn(),
        };
        return await updateFunction(transaction);
      });
    };

    beforeEach(() => {
      setupEmailCollection();
    });

    it("should write suppression doc keyed by normalized (lowercased, trimmed) email", async () => {
      await firestoreModule.addEmailSuppression(
        "  User@Example.GOV  ",
        "hard_bounce",
        "notif-123",
        true,
        true,
        { token: "tok-123" }
      );

      expect(collectionDocSpy).toHaveBeenCalledWith("user@example.gov");
      expect(docSetSpy).toHaveBeenCalledTimes(1);
    });

    it("should only write true suppression flags (monotonic escalation)", async () => {
      await firestoreModule.addEmailSuppression(
        "test@example.gov",
        "unsubscribed",
        "notif-456",
        true,
        false,
        { token: "tok-456" }
      );

      const data = docSetSpy.mock.calls[0][1];
      expect(data.suppressBulk).toBe(true);
      // suppressTransactional=false is omitted; merge:true preserves existing value.
      expect(data).not.toHaveProperty("suppressTransactional");
    });

    it("should include both flags when both are true", async () => {
      await firestoreModule.addEmailSuppression(
        "test@example.gov",
        "hard_bounce",
        "notif-789",
        true,
        true,
        { token: "tok-789" }
      );

      const data = docSetSpy.mock.calls[0][1];
      expect(data.suppressBulk).toBe(true);
      expect(data.suppressTransactional).toBe(true);
    });

    it("should omit both flags when both are false (blocked)", async () => {
      await firestoreModule.addEmailSuppression(
        "test@example.gov",
        "blocked",
        "notif-blk",
        false,
        false,
        { token: "tok-blk" }
      );

      const data = docSetSpy.mock.calls[0][1];
      expect(data).not.toHaveProperty("suppressBulk");
      expect(data).not.toHaveProperty("suppressTransactional");
      expect(data.status).toBe("suppressed");
      expect(data.reason).toBe("blocked");
    });

    it("should use merge:true to preserve existing fields", async () => {
      await firestoreModule.addEmailSuppression(
        "test@example.gov",
        "hard_bounce",
        "notif-789",
        true,
        true,
        { token: "tok-789" }
      );

      const setCall = docSetSpy.mock.calls[0];
      expect(setCall[2]).toEqual({ merge: true });
    });

    it("should store reason, lastEventAt, lastNotificationId on first write", async () => {
      await firestoreModule.addEmailSuppression(
        "test@example.gov",
        "spam_report",
        "notif-abc",
        true,
        true,
        { token: "tok-abc" }
      );

      const data = docSetSpy.mock.calls[0][1];
      expect(data.reason).toBe("spam_report");
      expect(data.lastNotificationId).toBe("notif-abc");
      // manualOverride:false is intentionally NOT written so that a prior
      // manualOverride:true is never reverted by webhook events.
      expect(data).not.toHaveProperty("manualOverride");
      expect(data.token).toBe("tok-abc");
      expect(data.lastEventAt).toBeDefined();
      expect(typeof data.lastEventAt).toBe("string");
    });

    it("should preserve a stronger existing reason and not regress to a weaker one", async () => {
      existingDocData = {
        reason: "hard_bounce",
        suppressBulk: true,
        suppressTransactional: true,
        lastEventAt: "2030-01-01T00:00:00.000Z",
        lastNotificationId: "old-notif",
      };

      await firestoreModule.addEmailSuppression(
        "test@example.gov",
        "unsubscribed",
        "weak-notif",
        true,
        false,
        { token: "tok" }
      );

      const data = docSetSpy.mock.calls[0][1];
      // suppressBulk continues to be true; reason should NOT regress.
      expect(data).not.toHaveProperty("reason");
      expect(data).not.toHaveProperty("lastNotificationId");
    });

    it("should not advance lastEventAt to an older event timestamp", async () => {
      existingDocData = {
        reason: "hard_bounce",
        lastEventAt: "2030-01-01T00:00:00.000Z",
      };

      await firestoreModule.addEmailSuppression(
        "test@example.gov",
        "hard_bounce",
        "notif",
        true,
        true,
        { token: "tok", eventTimestamp: 1000 } // 1970-01-01
      );

      const data = docSetSpy.mock.calls[0][1];
      expect(data).not.toHaveProperty("lastEventAt");
    });

    it("should never revert manualOverride:true via a webhook event", async () => {
      existingDocData = {
        reason: "hard_bounce",
        manualOverride: true,
      };

      await firestoreModule.addEmailSuppression(
        "test@example.gov",
        "unsubscribed",
        "notif",
        true,
        false,
        { token: "tok" }
      );

      const data = docSetSpy.mock.calls[0][1];
      expect(data).not.toHaveProperty("manualOverride");
    });

    it("should handle mixed-case email by normalizing to lowercase", async () => {
      await firestoreModule.addEmailSuppression(
        "John.Doe@GMAIL.COM",
        "hard_bounce",
        "notif-xyz",
        true,
        true,
        {}
      );

      expect(collectionDocSpy).toHaveBeenCalledWith("john.doe@gmail.com");
    });
  });

  // isEmailSuppressed

  describe("isEmailSuppressed", () => {
    it("should return false when no suppression doc exists", async () => {
      // Default mock returns non-existent doc
      const result = await firestoreModule.isEmailSuppressed("new@example.com", "bulk");
      expect(result).toBe(false);
    });

    it("should return true for bulk mail when suppressBulk is true", async () => {
      firestoreMocks.setupDocumentRetrieval("emailAddressStatus", "test@example.com", {
        suppressBulk: true,
        suppressTransactional: false,
      });

      const result = await firestoreModule.isEmailSuppressed("test@example.com", "bulk");
      expect(result).toBe(true);
    });

    it("should return false for bulk mail when suppressBulk is false", async () => {
      firestoreMocks.setupDocumentRetrieval("emailAddressStatus", "test@example.com", {
        suppressBulk: false,
        suppressTransactional: true,
      });

      const result = await firestoreModule.isEmailSuppressed("test@example.com", "bulk");
      expect(result).toBe(false);
    });

    it("should return true for transactional mail when suppressTransactional is true", async () => {
      firestoreMocks.setupDocumentRetrieval("emailAddressStatus", "test@example.com", {
        suppressBulk: false,
        suppressTransactional: true,
      });

      const result = await firestoreModule.isEmailSuppressed("test@example.com", "transactional");
      expect(result).toBe(true);
    });

    it("should return false for transactional mail when suppressTransactional is false", async () => {
      firestoreMocks.setupDocumentRetrieval("emailAddressStatus", "test@example.com", {
        suppressBulk: true,
        suppressTransactional: false,
      });

      const result = await firestoreModule.isEmailSuppressed("test@example.com", "transactional");
      expect(result).toBe(false);
    });

    it("should normalize email before lookup", async () => {
      firestoreMocks.setupDocumentRetrieval("emailAddressStatus", "user@test.com", {
        suppressBulk: true,
        suppressTransactional: true,
      });

      const result = await firestoreModule.isEmailSuppressed("  USER@TEST.COM  ", "bulk");
      expect(result).toBe(true);
    });
  });

  // getEmailSuppressions

  describe("getEmailSuppressions", () => {
    const setupGetAll = (docs) => {
      firestoreMocks.mockFirestore.getAll = vi.fn().mockResolvedValue(docs);
      // Set up emailAddressStatus collection for doc ref creation
      firestoreMocks._collectionOverrides.set("emailAddressStatus", {
        doc: vi.fn().mockImplementation((docId) => ({
          get: vi.fn().mockResolvedValue({ exists: false }),
          set: vi.fn().mockResolvedValue(undefined),
        })),
      });
    };

    it("should return empty Set for empty input array", async () => {
      const result = await firestoreModule.getEmailSuppressions([], "bulk");
      expect(result).toBeInstanceOf(Set);
      expect(result.size).toBe(0);
    });

    it("should return empty Set when no emails are suppressed", async () => {
      setupGetAll([
        { exists: false, id: "a@test.com" },
        { exists: false, id: "b@test.com" },
      ]);

      const result = await firestoreModule.getEmailSuppressions(
        ["a@test.com", "b@test.com"],
        "bulk"
      );
      expect(result).toBeInstanceOf(Set);
      expect(result.size).toBe(0);
    });

    it("should return Set of suppressed emails for given mailStream", async () => {
      setupGetAll([
        {
          exists: true,
          id: "bounced@test.com",
          data: () => ({ suppressBulk: true, suppressTransactional: true }),
        },
        {
          exists: true,
          id: "ok@test.com",
          data: () => ({ suppressBulk: false, suppressTransactional: false }),
        },
      ]);

      const result = await firestoreModule.getEmailSuppressions(
        ["bounced@test.com", "ok@test.com"],
        "bulk"
      );
      expect(result).toBeInstanceOf(Set);
      expect(result.has("bounced@test.com")).toBe(true);
      expect(result.has("ok@test.com")).toBe(false);
    });

    it("should handle batch lookups via db.getAll chunked to 100", async () => {
      const emails = Array.from({ length: 150 }, (_, i) => `user${i}@test.com`);
      setupGetAll(
        emails.slice(0, 100).map((e) => ({ exists: false, id: e }))
      );
      // Mock for second chunk
      firestoreMocks.mockFirestore.getAll
        .mockResolvedValueOnce(emails.slice(0, 100).map((e) => ({ exists: false, id: e })))
        .mockResolvedValueOnce(emails.slice(100).map((e) => ({ exists: false, id: e })));

      await firestoreModule.getEmailSuppressions(emails, "bulk");

      expect(firestoreMocks.mockFirestore.getAll).toHaveBeenCalledTimes(2);
    });

    it("should handle mix of suppressed and non-suppressed emails", async () => {
      setupGetAll([
        {
          exists: true,
          id: "spam@test.com",
          data: () => ({ suppressBulk: true, suppressTransactional: true }),
        },
        {
          exists: true,
          id: "unsub@test.com",
          data: () => ({ suppressBulk: true, suppressTransactional: false }),
        },
        { exists: false, id: "new@test.com" },
      ]);

      const result = await firestoreModule.getEmailSuppressions(
        ["spam@test.com", "unsub@test.com", "new@test.com"],
        "transactional"
      );
      expect(result.has("spam@test.com")).toBe(true);
      expect(result.has("unsub@test.com")).toBe(false);
      expect(result.has("new@test.com")).toBe(false);
    });
  });

  describe("queued bulk notification spec helpers", () => {
    const setupNotificationSpecsCollection = (docs = []) => {
      const toSnapshot = (matchingDocs) => ({
        empty: matchingDocs.length === 0,
        size: matchingDocs.length,
        docs: matchingDocs.map((doc) => ({
          id: doc.id,
          data: () => doc,
          ref: { id: doc.id },
        })),
        forEach: (callback) => matchingDocs.forEach((doc) => callback({
          id: doc.id,
          data: () => doc,
          ref: { id: doc.id },
        })),
      });

      const collectionMock = {
        where: vi.fn().mockImplementation((field, op, value) => {
          if (field === "id" && op === "in") {
            return {
              get: vi.fn().mockResolvedValue(toSnapshot(docs.filter((doc) => value.includes(doc.id)))),
            };
          }

          if (field === "scheduleAt" && op === "==") {
            return {
              where: vi.fn().mockImplementation((innerField, innerOp, innerValue) => ({
                get: vi.fn().mockResolvedValue(
                  toSnapshot(docs.filter((doc) => doc.scheduleAt === value && doc[innerField] === innerValue)),
                ),
              })),
            };
          }

          throw new Error(`Unexpected notificationSpecifications query: ${field} ${op}`);
        }),
      };

      firestoreMocks._collectionOverrides.set("notificationSpecifications", collectionMock);
      return collectionMock;
    };

    it("should mark queued specs with the runDateKey and queued timestamp", async () => {
      const mockBatch = firestoreMocks.createMockBatch();
      firestoreMocks.mockFirestore.batch.mockReturnValueOnce(mockBatch);
      setupNotificationSpecsCollection([
        { id: "spec-a" },
        { id: "spec-b" },
      ]);

      const count = await firestoreModule.markNotificationSpecsQueuedForRun(
        ["spec-a", "spec-a", "", "spec-b"],
        STALE_RUN_DATE_KEY,
        STALE_RUN_UPDATED_AT,
      );

      expect(count).toBe(2);
      expect(mockBatch.update).toHaveBeenCalledTimes(2);
      expect(mockBatch.update).toHaveBeenNthCalledWith(1, { id: "spec-a" }, {
        queuedBulkRunDateKey: STALE_RUN_DATE_KEY,
        queuedBulkRunUpdatedAt: STALE_RUN_UPDATED_AT,
      });
      expect(mockBatch.update).toHaveBeenNthCalledWith(2, { id: "spec-b" }, {
        queuedBulkRunDateKey: STALE_RUN_DATE_KEY,
        queuedBulkRunUpdatedAt: STALE_RUN_UPDATED_AT,
      });
      expect(mockBatch.commit).toHaveBeenCalledTimes(1);
    });

    it("should persist bulk run sequence metadata for queued specs when provided", async () => {
      const mockBatch = firestoreMocks.createMockBatch();
      firestoreMocks.mockFirestore.batch.mockReturnValueOnce(mockBatch);
      setupNotificationSpecsCollection([
        { id: "spec-a" },
        { id: "spec-b" },
      ]);

      const count = await firestoreModule.markNotificationSpecsQueuedForRun(
        ["spec-a", "spec-b"],
        STALE_RUN_DATE_KEY,
        STALE_RUN_UPDATED_AT,
        { "spec-a": 2, "spec-b": 7 },
      );

      expect(count).toBe(2);
      expect(mockBatch.update).toHaveBeenNthCalledWith(1, { id: "spec-a" }, {
        queuedBulkRunDateKey: STALE_RUN_DATE_KEY,
        queuedBulkRunUpdatedAt: STALE_RUN_UPDATED_AT,
        queuedBulkRunSequence: 2,
        bulkRunSequence: 2,
      });
      expect(mockBatch.update).toHaveBeenNthCalledWith(2, { id: "spec-b" }, {
        queuedBulkRunDateKey: STALE_RUN_DATE_KEY,
        queuedBulkRunUpdatedAt: STALE_RUN_UPDATED_AT,
        queuedBulkRunSequence: 7,
        bulkRunSequence: 7,
      });
    });

    it("should allow transient queued markers without committing bulk run sequence", async () => {
      const mockBatch = firestoreMocks.createMockBatch();
      firestoreMocks.mockFirestore.batch.mockReturnValueOnce(mockBatch);
      setupNotificationSpecsCollection([
        { id: "spec-a", bulkRunSequence: 1 },
      ]);

      const count = await firestoreModule.markNotificationSpecsQueuedForRun(
        ["spec-a"],
        STALE_RUN_DATE_KEY,
        STALE_RUN_UPDATED_AT,
        { "spec-a": 2 },
        { commitRunSequence: false },
      );

      expect(count).toBe(1);
      expect(mockBatch.update).toHaveBeenCalledWith({ id: "spec-a" }, {
        queuedBulkRunDateKey: STALE_RUN_DATE_KEY,
        queuedBulkRunUpdatedAt: STALE_RUN_UPDATED_AT,
        queuedBulkRunSequence: 2,
      });
    });

    it("should chunk queued spec updates to stay within the Firestore in-query limit", async () => {
      const firstBatch = firestoreMocks.createMockBatch();
      const secondBatch = firestoreMocks.createMockBatch();
      firestoreMocks.mockFirestore.batch
        .mockReturnValueOnce(firstBatch)
        .mockReturnValueOnce(secondBatch);

      const docs = Array.from({ length: 31 }, (_, idx) => ({ id: `spec-${idx + 1}` }));
      setupNotificationSpecsCollection(docs);

      const count = await firestoreModule.markNotificationSpecsQueuedForRun(
        docs.map((doc) => doc.id),
        STALE_RUN_DATE_KEY,
        STALE_RUN_UPDATED_AT,
      );

      expect(count).toBe(31);
      expect(firstBatch.update).toHaveBeenCalledTimes(30);
      expect(firstBatch.commit).toHaveBeenCalledTimes(1);
      expect(secondBatch.update).toHaveBeenCalledTimes(1);
      expect(secondBatch.commit).toHaveBeenCalledTimes(1);
    });

    it("should only clear queued markers whose runDateKey matches the failed task chain", async () => {
      const mockBatch = firestoreMocks.createMockBatch();
      firestoreMocks.mockFirestore.batch.mockReturnValueOnce(mockBatch);
      setupNotificationSpecsCollection([
        { id: "spec-a", queuedBulkRunDateKey: STALE_RUN_DATE_KEY },
        { id: "spec-b", queuedBulkRunDateKey: "2026-03-31" },
      ]);

      const count = await firestoreModule.clearNotificationSpecsQueuedRun(
        ["spec-a", "spec-b"],
        STALE_RUN_DATE_KEY,
      );

      expect(count).toBe(1);
      expect(mockBatch.update).toHaveBeenCalledTimes(1);
      expect(mockBatch.update).toHaveBeenCalledWith({ id: "spec-a" }, {
        queuedBulkRunDateKey: "delete",
        queuedBulkRunUpdatedAt: "delete",
        queuedBulkRunSequence: "delete",
      });
      expect(mockBatch.commit).toHaveBeenCalledTimes(1);
    });

    it("should not clear a newer same-day queued marker for an older run sequence", async () => {
      const mockBatch = firestoreMocks.createMockBatch();
      firestoreMocks.mockFirestore.batch.mockReturnValueOnce(mockBatch);
      setupNotificationSpecsCollection([
        {
          id: "spec-a",
          queuedBulkRunDateKey: STALE_RUN_DATE_KEY,
          queuedBulkRunSequence: 2,
          bulkRunSequence: 2,
        },
      ]);

      const count = await firestoreModule.clearNotificationSpecsQueuedRun(
        ["spec-a"],
        STALE_RUN_DATE_KEY,
        { "spec-a": 1 },
      );

      expect(count).toBe(0);
      expect(mockBatch.update).not.toHaveBeenCalled();
      expect(mockBatch.commit).not.toHaveBeenCalled();
    });

    it("should complete queued runs by marking lastRunTime and clearing the queued marker", async () => {
      const mockBatch = firestoreMocks.createMockBatch();
      firestoreMocks.mockFirestore.batch.mockReturnValueOnce(mockBatch);
      setupNotificationSpecsCollection([
        { id: "spec-a", queuedBulkRunDateKey: STALE_RUN_DATE_KEY },
        { id: "spec-b", queuedBulkRunDateKey: "2026-03-31" },
      ]);

      const count = await firestoreModule.completeNotificationSpecsQueuedRun(
        ["spec-a", "spec-b"],
        STALE_RUN_DATE_KEY,
        "2026-04-01T20:00:00.000Z",
      );

      expect(count).toBe(1);
      expect(mockBatch.update).toHaveBeenCalledTimes(1);
      expect(mockBatch.update).toHaveBeenCalledWith({ id: "spec-a" }, {
        lastRunTime: "2026-04-01T20:00:00.000Z",
        lastRunDateKey: STALE_RUN_DATE_KEY,
        queuedBulkRunDateKey: "delete",
        queuedBulkRunUpdatedAt: "delete",
        queuedBulkRunSequence: "delete",
      });
      expect(mockBatch.commit).toHaveBeenCalledTimes(1);
    });

    it("should not complete a newer same-day queued marker for an older run sequence", async () => {
      const mockBatch = firestoreMocks.createMockBatch();
      firestoreMocks.mockFirestore.batch.mockReturnValueOnce(mockBatch);
      setupNotificationSpecsCollection([
        {
          id: "spec-a",
          queuedBulkRunDateKey: STALE_RUN_DATE_KEY,
          queuedBulkRunSequence: 2,
          bulkRunSequence: 2,
        },
      ]);

      const count = await firestoreModule.completeNotificationSpecsQueuedRun(
        ["spec-a"],
        STALE_RUN_DATE_KEY,
        "2026-04-01T20:00:00.000Z",
        { "spec-a": 1 },
      );

      expect(count).toBe(0);
      expect(mockBatch.update).not.toHaveBeenCalled();
      expect(mockBatch.commit).not.toHaveBeenCalled();
    });

    it("should exclude already-run and already-queued specs from once-per-day selection", async () => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date("2026-04-01T12:00:00.000Z"));
      setupNotificationSpecsCollection([
        { id: "eligible-spec", scheduleAt: "15:00", isDraft: false, lastRunDateKey: "2026-03-31" },
        { id: "ran-today", scheduleAt: "15:00", isDraft: false, lastRunDateKey: STALE_RUN_DATE_KEY },
        { id: "queued-today", scheduleAt: "15:00", isDraft: false, queuedBulkRunDateKey: STALE_RUN_DATE_KEY },
        { id: "", scheduleAt: "15:00", isDraft: false, lastRunDateKey: "2026-03-31" },
      ]);

      const specs = await firestoreModule.getNotificationSpecsByScheduleOncePerDay("15:00");

      expect(specs.map((spec) => spec.id)).toEqual(["eligible-spec"]);
      vi.useRealTimers();
    });

    it("should fall back to legacy lastRunTime when lastRunDateKey is missing", async () => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date("2026-04-01T12:00:00.000Z"));
      setupNotificationSpecsCollection([
        { id: "legacy-eligible", scheduleAt: "15:00", isDraft: false, lastRunTime: "2026-03-31T15:00:00.000Z" },
        { id: "legacy-ran-today", scheduleAt: "15:00", isDraft: false, lastRunTime: STALE_RUN_UPDATED_AT },
      ]);

      const specs = await firestoreModule.getNotificationSpecsByScheduleOncePerDay("15:00");

      expect(specs.map((spec) => spec.id)).toEqual(["legacy-eligible"]);
      vi.useRealTimers();
    });
  });

  describe("bulk notification run helpers", () => {
    const setupBulkRunsCollection = ({ runs = {}, batchesByRun = {} } = {}) => {
      const runSetSpies = new Map();
      const batchSetSpies = new Map();

      const buildBatchRef = (runId, batchId) => {
        const key = `${runId}/${batchId}`;
        if (!batchSetSpies.has(key)) batchSetSpies.set(key, vi.fn().mockResolvedValue(undefined));
        return {
          id: batchId,
          path: `notificationBulkRuns/${runId}/batches/${batchId}`,
          get: vi.fn().mockResolvedValue(
            batchesByRun[runId]?.[batchId]
              ? { exists: true, id: batchId, data: () => batchesByRun[runId][batchId] }
              : { exists: false, id: batchId, data: () => null },
          ),
          set: batchSetSpies.get(key),
        };
      };

      const buildRunRef = (runId) => {
        if (!runSetSpies.has(runId)) runSetSpies.set(runId, vi.fn().mockResolvedValue(undefined));
        return {
          id: runId,
          path: `notificationBulkRuns/${runId}`,
          get: vi.fn().mockResolvedValue(
            runs[runId]
              ? { exists: true, id: runId, data: () => runs[runId] }
              : { exists: false, id: runId, data: () => null },
          ),
          set: runSetSpies.get(runId),
          collection: vi.fn().mockImplementation((collectionName) => {
            if (collectionName !== "batches") throw new Error(`Unexpected subcollection ${collectionName}`);
            return {
              doc: vi.fn((batchId) => buildBatchRef(runId, batchId)),
              get: vi.fn().mockResolvedValue({
                docs: Object.entries(batchesByRun[runId] || {}).map(([id, data]) => ({
                  id,
                  data: () => data,
                })),
              }),
            };
          }),
        };
      };

      const collectionMock = {
        doc: vi.fn((runId) => buildRunRef(runId)),
      };
      firestoreMocks._collectionOverrides.set("notificationBulkRuns", collectionMock);
      firestoreMocks.mockFirestore.runTransaction.mockImplementation(async (transactionFn) => {
        const transaction = {
          get: vi.fn((ref) => ref.get()),
          set: vi.fn((ref, data, options) => ref.set(data, options)),
        };
        return transactionFn(transaction);
      });
      return { collectionMock, runSetSpies, batchSetSpies };
    };

    it("should save run plans with batch ids, lane indexes, and chunked batch writes", async () => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date("2026-04-01T12:00:00.000Z"));
      const firstBatch = firestoreMocks.createMockBatch();
      const secondBatch = firestoreMocks.createMockBatch();
      firestoreMocks.mockFirestore.batch
        .mockReturnValueOnce(firstBatch)
        .mockReturnValueOnce(secondBatch);
      const { runSetSpies } = setupBulkRunsCollection();
      const batchDocs = Array.from({ length: 451 }, (_, idx) => ({
        id: `default-batch-${idx + 1}`,
        lane: "default",
        batchNumber: idx + 1,
        recipientCount: 1,
      }));

      const result = await firestoreModule.saveBulkNotificationRunPlan({
        runDoc: {
          id: "run-save-plan",
          specId: "spec-save-plan",
          settings: { bulkDefaultBatchSize: 100 },
        },
        batchDocs,
      });

      expect(result).toEqual({
        runId: "run-save-plan",
        batchCount: 451,
        batchIdsByLane: {
          default: batchDocs.map((batchDoc) => batchDoc.id),
        },
      });
      expect(firstBatch.set).toHaveBeenCalledTimes(450);
      expect(secondBatch.set).toHaveBeenCalledTimes(1);
      expect(firstBatch.commit).toHaveBeenCalledTimes(1);
      expect(secondBatch.commit).toHaveBeenCalledTimes(1);
      expect(runSetSpies.get("run-save-plan")).toHaveBeenCalledWith(expect.objectContaining({
        id: "run-save-plan",
        specId: "spec-save-plan",
        batchIds: batchDocs.map((batchDoc) => batchDoc.id),
        batchIdsByLane: { default: batchDocs.map((batchDoc) => batchDoc.id) },
        batchCount: 451,
        status: "planned",
        updatedAt: "2026-04-01T12:00:00.000Z",
      }), { merge: true });
      vi.useRealTimers();
    });

    it("should write enqueue, running, complete, and failed batch status metadata", async () => {
      const { runSetSpies, batchSetSpies } = setupBulkRunsCollection({
        runs: { "run-status": { id: "run-status", specId: "spec-status" } },
        batchesByRun: { "run-status": { "default-batch-1": { id: "default-batch-1" } } },
      });

      await firestoreModule.markBulkNotificationRunQueued(
        "run-status",
        "2026-04-01T11:59:00.000Z",
      );
      await firestoreModule.markBulkNotificationBatchEnqueued({
        runId: "run-status",
        batchId: "default-batch-1",
        taskId: "task-1",
        queueName: "processNotificationBatchBulkDefault",
        scheduleDelaySeconds: 12,
        scheduledFor: "2026-04-01T12:00:12.000Z",
        enqueuedAt: "2026-04-01T12:00:00.000Z",
      });
      await firestoreModule.markBulkNotificationBatchRunning({
        runId: "run-status",
        batchId: "default-batch-1",
        taskAttemptOwner: "task-1-attempt-0",
        startedAt: "2026-04-01T12:01:00.000Z",
      });
      await firestoreModule.markBulkNotificationBatchComplete({
        runId: "run-status",
        batchId: "default-batch-1",
        taskAttemptOwner: "task-1-attempt-0",
        counts: { planned: 1, sent: 1 },
        unsuccessful: { filtered: [] },
        completedAt: "2026-04-01T12:02:00.000Z",
      });
      await firestoreModule.markBulkNotificationBatchFailed({
        runId: "run-status",
        batchId: "default-batch-1",
        taskAttemptOwner: "task-1-attempt-0",
        counts: { planned: 1, sent: 0, providerFailed: 1 },
        unsuccessful: { providerFailed: [{ email: "failed@test.gov" }] },
        error: Object.assign(new Error("Provider rejected"), { statusCode: 400, code: "400" }),
        failedAt: "2026-04-01T12:03:00.000Z",
      });

      const batchSet = batchSetSpies.get("run-status/default-batch-1");
      expect(runSetSpies.get("run-status")).toHaveBeenCalledWith(expect.objectContaining({
        status: "queued",
        queuedAt: "2026-04-01T11:59:00.000Z",
      }), { merge: true });
      expect(batchSet).toHaveBeenNthCalledWith(1, expect.objectContaining({
        status: "enqueued",
        taskId: "task-1",
        queueName: "processNotificationBatchBulkDefault",
        scheduleDelaySeconds: 12,
        scheduledFor: "2026-04-01T12:00:12.000Z",
      }), { merge: true });
      expect(batchSet).toHaveBeenNthCalledWith(2, expect.objectContaining({
        status: "running",
        startedAt: "2026-04-01T12:01:00.000Z",
        taskAttemptOwner: "task-1-attempt-0",
        taskAttemptExpiresAt: "2026-04-01T12:31:00.000Z",
      }), { merge: true });
      expect(runSetSpies.get("run-status")).toHaveBeenCalledWith(expect.objectContaining({
        status: "running",
      }), { merge: true });
      expect(batchSet).toHaveBeenNthCalledWith(3, expect.objectContaining({
        status: "complete",
        counts: { planned: 1, sent: 1 },
        taskAttemptExpiresAt: "delete",
      }), { merge: true });
      expect(batchSet).toHaveBeenNthCalledWith(4, expect.objectContaining({
        status: "failed",
        lastErrorMessage: "Provider rejected",
        lastErrorCode: "400",
        lastErrorStatus: "400",
        taskAttemptExpiresAt: "delete",
      }), { merge: true });
    });

    it("should reject same-retry-count duplicate dispatches and stale terminal writes from foreign owners", async () => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date("2026-04-01T12:05:00.000Z"));
      const { batchSetSpies } = setupBulkRunsCollection({
        runs: { "run-active-attempt": { id: "run-active-attempt", specId: "spec-active" } },
        batchesByRun: {
          "run-active-attempt": {
            "default-batch-1": {
              id: "default-batch-1",
              status: "running",
              taskAttemptOwner: "task-attempt-0",
              taskAttemptExpiresAt: "2026-04-01T12:30:00.000Z",
            },
          },
        },
      });

      // Same retry count as the existing owner = duplicate dispatch, must be
      // rejected while the lock is still active.
      const duplicateClaim = await firestoreModule.markBulkNotificationBatchRunning({
        runId: "run-active-attempt",
        batchId: "default-batch-1",
        taskAttemptOwner: "task-attempt-0",
        startedAt: "2026-04-01T12:05:00.000Z",
      });
      // A foreign owner (different task entirely) cannot finalize a batch
      // owned by another attempt.
      const staleComplete = await firestoreModule.markBulkNotificationBatchComplete({
        runId: "run-active-attempt",
        batchId: "default-batch-1",
        taskAttemptOwner: "rogue-attempt-9",
        counts: { planned: 1, sent: 0, providerUnknown: 1 },
        completedAt: "2026-04-01T12:05:30.000Z",
      });
      const staleFailed = await firestoreModule.markBulkNotificationBatchFailed({
        runId: "run-active-attempt",
        batchId: "default-batch-1",
        taskAttemptOwner: "rogue-attempt-9",
        error: new Error("late duplicate"),
        failedAt: "2026-04-01T12:06:00.000Z",
      });

      expect(duplicateClaim).toBe(false);
      expect(staleComplete).toBe(false);
      expect(staleFailed).toBe(false);
      expect(batchSetSpies.get("run-active-attempt/default-batch-1")).not.toHaveBeenCalled();
      vi.useRealTimers();
    });

    it("should let a higher-retry-count attempt take over an active running batch lock (Cloud Tasks proves prior attempt is done)", async () => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date("2026-04-01T12:05:00.000Z"));
      const { batchSetSpies } = setupBulkRunsCollection({
        runs: { "run-takeover": { id: "run-takeover", specId: "spec-takeover" } },
        batchesByRun: {
          "run-takeover": {
            "default-batch-1": {
              id: "default-batch-1",
              status: "running",
              taskAttemptOwner: "task-attempt-0",
              taskAttemptExpiresAt: "2026-04-01T12:30:00.000Z",
            },
          },
        },
      });

      const claimed = await firestoreModule.markBulkNotificationBatchRunning({
        runId: "run-takeover",
        batchId: "default-batch-1",
        taskAttemptOwner: "task-attempt-1",
        startedAt: "2026-04-01T12:05:00.000Z",
      });

      expect(claimed).toBe(true);
      expect(batchSetSpies.get("run-takeover/default-batch-1")).toHaveBeenCalledWith(
        expect.objectContaining({
          status: "running",
          taskAttemptOwner: "task-attempt-1",
        }),
        { merge: true },
      );
      vi.useRealTimers();
    });

    it("should allow a new batch task attempt after the prior running attempt expires", async () => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date("2026-04-01T12:31:01.000Z"));
      const { batchSetSpies } = setupBulkRunsCollection({
        runs: { "run-expired-attempt": { id: "run-expired-attempt", specId: "spec-expired" } },
        batchesByRun: {
          "run-expired-attempt": {
            "default-batch-1": {
              id: "default-batch-1",
              status: "running",
              taskAttemptOwner: "task-attempt-0",
              taskAttemptExpiresAt: "2026-04-01T12:30:00.000Z",
            },
          },
        },
      });

      const claimed = await firestoreModule.markBulkNotificationBatchRunning({
        runId: "run-expired-attempt",
        batchId: "default-batch-1",
        taskAttemptOwner: "task-attempt-1",
        startedAt: "2026-04-01T12:31:01.000Z",
        attemptDurationMs: 60000,
      });

      expect(claimed).toBe(true);
      expect(batchSetSpies.get("run-expired-attempt/default-batch-1")).toHaveBeenCalledWith(expect.objectContaining({
        status: "running",
        taskAttemptOwner: "task-attempt-1",
        taskAttemptExpiresAt: "2026-04-01T12:32:01.000Z",
      }), { merge: true });
      vi.useRealTimers();
    });

    it("should fall back to startedAt when a running batch has no explicit attempt expiry", async () => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date("2026-04-01T12:05:00.000Z"));
      const { batchSetSpies } = setupBulkRunsCollection({
        runs: { "run-started-at-expiry": { id: "run-started-at-expiry", specId: "spec-expiry" } },
        batchesByRun: {
          "run-started-at-expiry": {
            "default-batch-1": {
              id: "default-batch-1",
              status: "running",
              taskAttemptOwner: "task-attempt-0",
              startedAt: "2026-04-01T11:40:00.000Z",
            },
          },
        },
      });

      // Same retry count = duplicate dispatch, blocked while the implicit
      // lock derived from startedAt is still active.
      const blockedClaim = await firestoreModule.markBulkNotificationBatchRunning({
        runId: "run-started-at-expiry",
        batchId: "default-batch-1",
        taskAttemptOwner: "task-attempt-0",
        startedAt: "2026-04-01T12:05:00.000Z",
      });

      vi.setSystemTime(new Date("2026-04-01T12:10:01.000Z"));
      const expiredClaim = await firestoreModule.markBulkNotificationBatchRunning({
        runId: "run-started-at-expiry",
        batchId: "default-batch-1",
        taskAttemptOwner: "task-attempt-2",
        startedAt: "2026-04-01T12:10:01.000Z",
      });

      expect(blockedClaim).toBe(false);
      expect(expiredClaim).toBe(true);
      expect(batchSetSpies.get("run-started-at-expiry/default-batch-1")).toHaveBeenCalledTimes(1);
      expect(batchSetSpies.get("run-started-at-expiry/default-batch-1")).toHaveBeenCalledWith(expect.objectContaining({
        status: "running",
        taskAttemptOwner: "task-attempt-2",
        taskAttemptExpiresAt: "2026-04-01T12:40:01.000Z",
      }), { merge: true });
      vi.useRealTimers();
    });

    it("should not regress terminal batch status metadata", async () => {
      const { runSetSpies, batchSetSpies } = setupBulkRunsCollection({
        runs: { "run-terminal": { id: "run-terminal", specId: "spec-terminal", status: "failed" } },
        batchesByRun: {
          "run-terminal": {
            "complete-batch": { id: "complete-batch", status: "complete" },
            "failed-batch": { id: "failed-batch", status: "failed" },
            "planned-batch": { id: "planned-batch", status: "planned" },
          },
        },
      });

      const runningResult = await firestoreModule.markBulkNotificationBatchRunning({
        runId: "run-terminal",
        batchId: "complete-batch",
        startedAt: "2026-04-01T12:01:00.000Z",
      });
      const enqueuedResult = await firestoreModule.markBulkNotificationBatchEnqueued({
        runId: "run-terminal",
        batchId: "complete-batch",
        taskId: "late-task",
        enqueuedAt: "2026-04-01T12:01:30.000Z",
      });
      const completeResult = await firestoreModule.markBulkNotificationBatchComplete({
        runId: "run-terminal",
        batchId: "failed-batch",
        counts: { planned: 1, sent: 1 },
        completedAt: "2026-04-01T12:02:00.000Z",
      });
      const failedResult = await firestoreModule.markBulkNotificationBatchFailed({
        runId: "run-terminal",
        batchId: "complete-batch",
        counts: { planned: 1, sent: 0 },
        error: new Error("late failure"),
        failedAt: "2026-04-01T12:03:00.000Z",
      });
      const completeOnTerminalRunResult = await firestoreModule.markBulkNotificationBatchComplete({
        runId: "run-terminal",
        batchId: "planned-batch",
        counts: { planned: 1, sent: 1 },
        completedAt: "2026-04-01T12:03:30.000Z",
      });
      const queuedResult = await firestoreModule.markBulkNotificationRunQueued(
        "run-terminal",
        "2026-04-01T12:04:00.000Z",
      );
      const enqueueFailedResult = await firestoreModule.markBulkNotificationRunEnqueueFailed(
        "run-terminal",
        new Error("late enqueue failure"),
        "2026-04-01T12:05:00.000Z",
      );

      expect(runningResult).toBe(false);
      expect(enqueuedResult).toBe(false);
      expect(completeResult).toBe(false);
      expect(failedResult).toBe(false);
      expect(completeOnTerminalRunResult).toBe(false);
      expect(queuedResult).toBe(false);
      expect(enqueueFailedResult).toBe(false);
      expect(batchSetSpies.get("run-terminal/complete-batch")).not.toHaveBeenCalled();
      expect(batchSetSpies.get("run-terminal/failed-batch")).not.toHaveBeenCalled();
      expect(batchSetSpies.get("run-terminal/planned-batch")).not.toHaveBeenCalled();
      expect(runSetSpies.get("run-terminal")).not.toHaveBeenCalled();
    });

    it("should finalize only when all expected batches are terminal", async () => {
      const { runSetSpies } = setupBulkRunsCollection({
        runs: {
          "run-not-terminal": {
            id: "run-not-terminal",
            specId: "spec-not-terminal",
            runDateKey: STALE_RUN_DATE_KEY,
            batchCount: 2,
          },
        },
        batchesByRun: {
          "run-not-terminal": {
            "default-batch-1": { status: "complete" },
            "default-batch-2": { status: "running" },
          },
        },
      });

      const result = await firestoreModule.finalizeBulkNotificationRunIfTerminal(
        "run-not-terminal",
        "2026-04-01T14:00:00.000Z",
      );

      expect(result).toEqual({ finalized: false });
      expect(runSetSpies.get("run-not-terminal")).not.toHaveBeenCalled();
    });

    it("should complete successful terminal runs and mark the spec run complete", async () => {
      const mockBatch = firestoreMocks.createMockBatch();
      firestoreMocks.mockFirestore.batch.mockReturnValueOnce(mockBatch);
      const { runSetSpies } = setupBulkRunsCollection({
        runs: {
          "run-complete": {
            id: "run-complete",
            specId: "spec-complete",
            runDateKey: STALE_RUN_DATE_KEY,
            batchCount: 2,
          },
        },
        batchesByRun: {
          "run-complete": {
            "default-batch-1": { status: "complete" },
            "microsoft-batch-1": { status: "complete" },
          },
        },
      });

      const collectionMock = {
        where: vi.fn().mockReturnValue({
          get: vi.fn().mockResolvedValue({
            docs: [{
              id: "spec-complete",
              data: () => ({ id: "spec-complete", queuedBulkRunDateKey: STALE_RUN_DATE_KEY }),
              ref: { id: "spec-complete" },
            }],
          }),
        }),
      };
      firestoreMocks._collectionOverrides.set("notificationSpecifications", collectionMock);

      const result = await firestoreModule.finalizeBulkNotificationRunIfTerminal(
        "run-complete",
        "2026-04-01T14:00:00.000Z",
      );

      expect(result).toEqual({ finalized: true, status: "complete" });
      expect(runSetSpies.get("run-complete")).toHaveBeenCalledWith({
        status: "complete",
        completedAt: "2026-04-01T14:00:00.000Z",
        updatedAt: "2026-04-01T14:00:00.000Z",
      }, { merge: true });
      expect(mockBatch.update).toHaveBeenCalledWith({ id: "spec-complete" }, expect.objectContaining({
        lastRunTime: "2026-04-01T14:00:00.000Z",
        lastRunDateKey: STALE_RUN_DATE_KEY,
        queuedBulkRunDateKey: "delete",
      }));
    });

    it("should fail terminal runs with failed batches and clear the queued marker", async () => {
      const mockBatch = firestoreMocks.createMockBatch();
      firestoreMocks.mockFirestore.batch.mockReturnValueOnce(mockBatch);
      const { runSetSpies } = setupBulkRunsCollection({
        runs: {
          "run-failed": {
            id: "run-failed",
            specId: "spec-failed",
            runDateKey: STALE_RUN_DATE_KEY,
            runSequence: 3,
            batchCount: 2,
          },
        },
        batchesByRun: {
          "run-failed": {
            "default-batch-1": { status: "complete" },
            "microsoft-batch-1": { status: "failed" },
          },
        },
      });

      firestoreMocks._collectionOverrides.set("notificationSpecifications", {
        where: vi.fn().mockReturnValue({
          get: vi.fn().mockResolvedValue({
            docs: [{
              id: "spec-failed",
              data: () => ({
                id: "spec-failed",
                queuedBulkRunDateKey: STALE_RUN_DATE_KEY,
                bulkRunSequence: 2,
              }),
              ref: { id: "spec-failed" },
            }],
          }),
        }),
      });

      const result = await firestoreModule.finalizeBulkNotificationRunIfTerminal(
        "run-failed",
        "2026-04-01T14:00:00.000Z",
      );

      expect(result).toEqual({ finalized: true, status: "failed" });
      expect(runSetSpies.get("run-failed")).toHaveBeenCalledWith({
        status: "failed",
        completedAt: "2026-04-01T14:00:00.000Z",
        updatedAt: "2026-04-01T14:00:00.000Z",
      }, { merge: true });
      expect(mockBatch.update).toHaveBeenCalledWith({ id: "spec-failed" }, expect.objectContaining({
        queuedBulkRunDateKey: "delete",
        queuedBulkRunUpdatedAt: "delete",
        queuedBulkRunSequence: "delete",
        bulkRunSequence: 3,
      }));
    });
  });

  describe("notification provider delivery state machine", () => {
    const notificationRecord = {
      id: "notif-1",
      notificationSpecificationsID: "spec-1",
      notificationType: "email",
      token: "tok-1",
    };

    const runTransactionUpdateWithSnapshots = async (snapshots, updateFn) => {
      const setSpy = vi.fn();
      firestoreMocks.mockFirestore.runTransaction.mockImplementationOnce(async (transactionFn) => {
        const transaction = {
          getAll: vi.fn().mockResolvedValue(snapshots),
          set: setSpy,
        };
        await transactionFn(transaction);
      });

      const result = await updateFn();
      return { result, setSpy };
    };

    it("should skip active foreign reservations and claim expired reservations", async () => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date("2026-04-01T12:00:00.000Z"));

      const active = await runTransactionUpdateWithSnapshots([
        {
          exists: true,
          data: () => ({
            processingState: "reserved",
            providerAttemptOwner: "owner-a",
            reservationExpiresAt: "2026-04-01T12:10:00.000Z",
          }),
        },
      ], () => firestoreModule.reserveNotificationBatch([notificationRecord], "owner-b", 30 * 60 * 1000));

      expect(active.result.recordsToSend).toEqual([]);
      expect(active.setSpy).not.toHaveBeenCalled();

      const expired = await runTransactionUpdateWithSnapshots([
        {
          exists: true,
          data: () => ({
            processingState: "reserved",
            providerAttemptOwner: "owner-a",
            reservationExpiresAt: "2026-04-01T11:59:59.000Z",
            sendAttemptCount: 1,
          }),
        },
      ], () => firestoreModule.reserveNotificationBatch([notificationRecord], "owner-b", 30 * 60 * 1000));

      expect(expired.result.recordsToSend).toEqual([notificationRecord]);
      expect(expired.setSpy).toHaveBeenCalledTimes(1);
      expect(expired.setSpy.mock.calls[0][1]).toEqual(expect.objectContaining({
        processingState: "reserved",
        providerAttemptOwner: "owner-b",
        reservationExpiresAt: "2026-04-01T12:30:00.000Z",
        sendAttemptCount: 2,
      }));

      vi.useRealTimers();
    });

    it("should not reserve records once a provider send may already be in flight", async () => {
      const oldReservation = "2026-04-01T11:00:00.000Z";
      const { result, setSpy } = await runTransactionUpdateWithSnapshots([
        {
          exists: true,
          data: () => ({
            processingState: "provider_send_in_flight",
            isSent: false,
            providerAttemptOwner: "owner-a",
            reservationExpiresAt: oldReservation,
          }),
        },
      ], () => firestoreModule.reserveNotificationBatch([notificationRecord], "owner-b", 1000));

      expect(result.recordsToSend).toEqual([]);
      expect(setSpy).not.toHaveBeenCalled();

      const unknown = await runTransactionUpdateWithSnapshots([
        {
          exists: true,
          data: () => ({
            processingState: "provider_acceptance_unknown",
            isSent: false,
            providerAcceptanceUnknownAt: "2026-04-01T12:00:00.000Z",
          }),
        },
      ], () => firestoreModule.reserveNotificationBatch([notificationRecord], "owner-b", 1000));

      expect(unknown.result.recordsToSend).toEqual([]);
      expect(unknown.setSpy).not.toHaveBeenCalled();
    });

    it("should move matching reserved records to provider_send_in_flight before external send", async () => {
      const { result, setSpy } = await runTransactionUpdateWithSnapshots([
        {
          exists: true,
          data: () => ({ processingState: "reserved", providerAttemptOwner: "owner-a" }),
        },
      ], () => firestoreModule.markNotificationBatchProviderSendStarted(
        [notificationRecord],
        "owner-a",
        STALE_RUN_UPDATED_AT,
      ));

      expect(result.recordsToSend).toEqual([notificationRecord]);
      expect(result.updatedCount).toBe(1);
      expect(setSpy).toHaveBeenCalledTimes(1);
      expect(setSpy.mock.calls[0][1]).toEqual(expect.objectContaining({
        processingState: "provider_send_in_flight",
        isSent: false,
        providerAttemptOwner: "owner-a",
        providerAttemptStartedAt: STALE_RUN_UPDATED_AT,
        reservationExpiresAt: "delete",
      }));
    });

    it("should not move reserved records to provider_send_in_flight for a different owner", async () => {
      const { result, setSpy } = await runTransactionUpdateWithSnapshots([
        {
          exists: true,
          data: () => ({ processingState: "reserved", providerAttemptOwner: "owner-b" }),
        },
      ], () => firestoreModule.markNotificationBatchProviderSendStarted(
        [notificationRecord],
        "owner-a",
        STALE_RUN_UPDATED_AT,
      ));

      expect(result.recordsToSend).toEqual([]);
      expect(result.updatedCount).toBe(0);
      expect(setSpy).not.toHaveBeenCalled();
    });

    it("should mark in-flight provider attempts as acceptance_unknown instead of failed", async () => {
      const { result, setSpy } = await runTransactionUpdateWithSnapshots([
        {
          exists: true,
          data: () => ({
            processingState: "provider_send_in_flight",
            providerAttemptOwner: "owner-a",
            reservationExpiresAt: "2026-04-01T16:30:00.000Z",
          }),
        },
      ], () => firestoreModule.markNotificationBatchProviderAcceptanceUnknown(
        [notificationRecord],
        "owner-a",
        Object.assign(new Error("network timeout"), { code: "ETIMEDOUT" }),
        "2026-04-01T16:00:00.000Z",
      ));

      expect(result).toBe(1);
      expect(setSpy).toHaveBeenCalledTimes(1);
      expect(setSpy.mock.calls[0][1]).toEqual(expect.objectContaining({
        processingState: "provider_acceptance_unknown",
        isSent: false,
        providerAcceptanceUnknownAt: "2026-04-01T16:00:00.000Z",
        providerAttemptOwner: "owner-a",
        lastProviderErrorCode: "ETIMEDOUT",
        reservationExpiresAt: "delete",
      }));
    });

    it("should clear provider attempt fields when marking records accepted", async () => {
      const { result, setSpy } = await runTransactionUpdateWithSnapshots([
        {
          exists: true,
          data: () => ({
            processingState: "provider_send_in_flight",
            providerAttemptOwner: "owner-a",
            reservationExpiresAt: "2026-04-01T16:30:00.000Z",
          }),
        },
      ], () => firestoreModule.markNotificationBatchAccepted(
        [notificationRecord],
        "owner-a",
        "2026-04-01T17:00:00.000Z",
      ));

      expect(result).toBe(1);
      expect(setSpy).toHaveBeenCalledTimes(1);
      expect(setSpy.mock.calls[0][1]).toEqual(expect.objectContaining({
        processingState: "provider_accepted",
        isSent: true,
        providerAcceptedAt: "2026-04-01T17:00:00.000Z",
        providerAttemptOwner: "delete",
        reservationExpiresAt: "delete",
        lastProviderErrorCode: "delete",
        lastProviderErrorMessage: "delete",
        lastProviderErrorStatus: "delete",
      }));
    });

    const runFailedUpdateWithSnapshots = async (snapshots) => {
      const setSpy = vi.fn();
      firestoreMocks.mockFirestore.runTransaction.mockImplementationOnce(async (updateFn) => {
        const transaction = {
          getAll: vi.fn().mockResolvedValue(snapshots),
          set: setSpy,
        };
        await updateFn(transaction);
      });

      const count = await firestoreModule.markNotificationBatchFailed(
        [{
          id: "notif-1",
          notificationSpecificationsID: "spec-1",
          notificationType: "email",
          token: "tok-1",
        }],
        "owner-a",
        new Error("provider failed"),
        "2026-04-01T16:00:00.000Z",
      );

      return { count, setSpy };
    };

    it("should mark failed records when the current provider attempt owner matches", async () => {
      const { count, setSpy } = await runFailedUpdateWithSnapshots([
        {
          exists: true,
          data: () => ({ processingState: "reserved", providerAttemptOwner: "owner-a" }),
        },
      ]);

      expect(count).toBe(1);
      expect(setSpy).toHaveBeenCalledTimes(1);
      expect(setSpy.mock.calls[0][1]).toEqual(expect.objectContaining({
        processingState: "send_failed",
        isSent: false,
        lastProviderErrorMessage: "provider failed",
        providerAttemptOwner: "delete",
        reservationExpiresAt: "delete",
      }));
    });

    it("should not overwrite accepted records or records reserved by another owner", async () => {
      const { count, setSpy } = await runFailedUpdateWithSnapshots([
        {
          exists: true,
          data: () => ({ processingState: "provider_accepted", isSent: true }),
        },
      ]);

      expect(count).toBe(0);
      expect(setSpy).not.toHaveBeenCalled();

      const mismatch = await runFailedUpdateWithSnapshots([
        {
          exists: true,
            data: () => ({ processingState: "reserved", providerAttemptOwner: "owner-b" }),
          },
        ]);

      expect(mismatch.count).toBe(0);
      expect(mismatch.setSpy).not.toHaveBeenCalled();

      const unknown = await runFailedUpdateWithSnapshots([
        {
          exists: true,
          data: () => ({ processingState: "provider_acceptance_unknown", providerAttemptOwner: "owner-a" }),
        },
      ]);

      expect(unknown.count).toBe(0);
      expect(unknown.setSpy).not.toHaveBeenCalled();
    });
  });

  // processSendGridEvent, suppression integration

  describe("processSendGridEvent, suppression integration", () => {
    const makeEvent = (overrides = {}) => ({
      notification_id: "notif-001",
      gcloud_project: process.env.GCLOUD_PROJECT || "test-project",
      event: "delivered",
      timestamp: 1700000000,
      ...overrides,
    });

    let emailDocSetSpy;
    let emailDocCalledWith;

    const setupNotificationDoc = (docData = {}, { directDocExists = true } = {}) => {
      const mockRef = { update: vi.fn().mockResolvedValue(undefined) };
      const mockDoc = {
        id: "notif-001",
        exists: true,
        data: () => ({ email: "user@example.gov", token: "tok-001", Connect_ID: "C-001", ...docData }),
        ref: mockRef,
      };
      const mockSnapshot = { size: 1, docs: [mockDoc] };
      const missingDoc = { exists: false, data: () => null };

      const notifCollection = {
        doc: vi.fn().mockReturnValue({
          get: vi.fn().mockResolvedValue(directDocExists ? mockDoc : missingDoc),
        }),
        where: vi.fn().mockReturnValue({
          get: vi.fn().mockResolvedValue(mockSnapshot),
        }),
      };
      firestoreMocks._collectionOverrides.set("notifications", notifCollection);

      // Set up trackable emailAddressStatus collection
      emailDocSetSpy = vi.fn().mockResolvedValue(undefined);
      emailDocCalledWith = [];
      firestoreMocks._collectionOverrides.set("emailAddressStatus", {
        doc: vi.fn().mockImplementation((docId) => {
          emailDocCalledWith.push(docId);
          return {
            get: vi.fn().mockResolvedValue({ exists: false, data: () => null }),
            set: emailDocSetSpy,
            update: vi.fn().mockResolvedValue(undefined),
          };
        }),
      });

      return { mockRef, mockDoc, notifCollection };
    };

    beforeEach(() => {
      process.env.GCLOUD_PROJECT = "test-project";
    });

    it("should resolve provider_acceptance_unknown when a correlated SendGrid event arrives", async () => {
      const { mockRef } = setupNotificationDoc({
        processingState: "provider_acceptance_unknown",
        isSent: false,
        providerAttemptOwner: "run-batch-owner",
      });

      await firestoreModule.processSendGridEvent(makeEvent({
        event: "delivered",
        timestamp: 1700000000,
      }));

      expect(mockRef.update).toHaveBeenCalledWith(expect.objectContaining({
        deliveredStatus: true,
        processingState: "provider_accepted",
        isSent: true,
        providerAcceptedAt: "2023-11-14T22:13:20.000Z",
        providerAttemptOwner: "delete",
      }));
    });

    it("should resolve provider_send_in_flight when any correlated SendGrid event arrives", async () => {
      const { mockRef } = setupNotificationDoc({
        processingState: "provider_send_in_flight",
        isSent: false,
        providerAttemptOwner: "run-batch-owner",
      });

      await firestoreModule.processSendGridEvent(makeEvent({
        event: "bounce",
        type: "blocked",
        reason: "Temporarily blocked",
        timestamp: 1700000000,
      }));

      expect(mockRef.update).toHaveBeenCalledWith(expect.objectContaining({
        bounceStatus: true,
        processingState: "provider_accepted",
        isSent: true,
        providerAcceptedAt: "2023-11-14T22:13:20.000Z",
        providerAttemptOwner: "delete",
      }));
    });

    it("should transition provider_send_in_flight to send_failed (not provider_accepted) on a dropped event", async () => {
      const { mockRef } = setupNotificationDoc({
        processingState: "provider_send_in_flight",
        isSent: false,
        providerAttemptOwner: "run-batch-owner",
      });

      await firestoreModule.processSendGridEvent(makeEvent({
        event: "dropped",
        reason: "Bounced Address",
        timestamp: 1700000000,
      }));

      expect(mockRef.update).toHaveBeenCalledWith(expect.objectContaining({
        droppedStatus: true,
        processingState: "send_failed",
        isSent: false,
        providerAttemptOwner: "delete",
      }));
    });

    it("should fall back to the legacy id query when deterministic notification doc lookup misses", async () => {
      const { mockRef, notifCollection } = setupNotificationDoc({}, { directDocExists: false });

      await firestoreModule.processSendGridEvent(makeEvent({
        event: "delivered",
        timestamp: 1700000000,
      }));

      expect(notifCollection.doc).toHaveBeenCalledWith("notif-001");
      expect(notifCollection.where).toHaveBeenCalledWith("id", "==", "notif-001");
      expect(mockRef.update).toHaveBeenCalledWith(expect.objectContaining({
        deliveredStatus: true,
      }));
    });

    // Hard bounce
    it("should suppress all mail (bulk + transactional) on hard bounce", async () => {
      const { mockRef } = setupNotificationDoc();
      const event = makeEvent({ event: "bounce", type: "bounce", reason: "550 User unknown" });

      await firestoreModule.processSendGridEvent(event);

      expect(mockRef.update).toHaveBeenCalled();
      expect(emailDocSetSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          token: "tok-001",
          suppressBulk: true,
          suppressTransactional: true,
          reason: "hard_bounce",
        }),
        { merge: true }
      );
    });

    // Spam report
    it("should suppress all mail on spam report", async () => {
      setupNotificationDoc();

      const event = makeEvent({ event: "spamreport" });
      await firestoreModule.processSendGridEvent(event);

      expect(emailDocSetSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          suppressBulk: true,
          suppressTransactional: true,
          reason: "spam_report",
        }),
        { merge: true }
      );
    });

    // Invalid email (dropped)
    it("should suppress all mail when dropped with 'invalid' reason", async () => {
      setupNotificationDoc();

      const event = makeEvent({ event: "dropped", reason: "Invalid email address" });
      await firestoreModule.processSendGridEvent(event);

      expect(emailDocSetSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          suppressBulk: true,
          suppressTransactional: true,
          reason: "invalid_email",
        }),
        { merge: true }
      );
    });

    it("should catch up app suppressions from SendGrid dropped suppression reasons", async () => {
      setupNotificationDoc();

      await firestoreModule.processSendGridEvent(makeEvent({ event: "dropped", reason: "Bounced Address" }));
      await firestoreModule.processSendGridEvent(makeEvent({ event: "dropped", reason: "Spam Reporting Address" }));
      await firestoreModule.processSendGridEvent(makeEvent({ event: "dropped", reason: "Unsubscribed Address" }));

      expect(emailDocSetSpy).toHaveBeenCalledTimes(3);
      expect(emailDocSetSpy.mock.calls[0][0]).toEqual(expect.objectContaining({
        suppressBulk: true,
        suppressTransactional: true,
        reason: "hard_bounce",
      }));
      expect(emailDocSetSpy.mock.calls[1][0]).toEqual(expect.objectContaining({
        suppressBulk: true,
        suppressTransactional: true,
        reason: "spam_report",
      }));
      expect(emailDocSetSpy.mock.calls[2][0]).toEqual(expect.objectContaining({
        suppressBulk: true,
        suppressTransactional: true,
        reason: "global_unsubscribe",
      }));
    });

    // Provider-global unsubscribe
    it("should suppress all mail on unsubscribe event", async () => {
      setupNotificationDoc();

      const event = makeEvent({ event: "unsubscribe" });
      await firestoreModule.processSendGridEvent(event);

      expect(emailDocSetSpy).toHaveBeenCalledTimes(1);
      const setCall = emailDocSetSpy.mock.calls[0];
      expect(setCall[0].suppressBulk).toBe(true);
      expect(setCall[0].suppressTransactional).toBe(true);
      expect(setCall[0].reason).toBe("global_unsubscribe");
      expect(setCall[1]).toEqual({ merge: true });
    });

    // Group unsubscribe
    it("should suppress bulk only on group_unsubscribe event (monotonic)", async () => {
      setupNotificationDoc();

      const event = makeEvent({ event: "group_unsubscribe" });
      await firestoreModule.processSendGridEvent(event);

      expect(emailDocSetSpy).toHaveBeenCalledTimes(1);
      const setCall = emailDocSetSpy.mock.calls[0];
      expect(setCall[0].suppressBulk).toBe(true);
      expect(setCall[0]).not.toHaveProperty("suppressTransactional");
      expect(setCall[0].reason).toBe("unsubscribed");
      expect(setCall[1]).toEqual({ merge: true });
    });

    // Block
    it("should record block but NOT auto-suppress (neither flag written)", async () => {
      setupNotificationDoc();

      const event = makeEvent({ event: "bounce", type: "blocked", reason: "Blocked by ISP" });
      await firestoreModule.processSendGridEvent(event);

      expect(emailDocSetSpy).toHaveBeenCalledTimes(1);
      const setCall = emailDocSetSpy.mock.calls[0];
      expect(setCall[0]).not.toHaveProperty("suppressBulk");
      expect(setCall[0]).not.toHaveProperty("suppressTransactional");
      expect(setCall[0].reason).toBe("blocked");
      expect(setCall[1]).toEqual({ merge: true });
    });

    // Defer
    it("should NOT suppress on defer event", async () => {
      setupNotificationDoc();

      const event = makeEvent({ event: "deferred" });
      await firestoreModule.processSendGridEvent(event);

      expect(emailDocSetSpy).not.toHaveBeenCalled();
    });

    // Delivered
    it("should NOT call addEmailSuppression on delivered event", async () => {
      setupNotificationDoc();

      const event = makeEvent({ event: "delivered" });
      await firestoreModule.processSendGridEvent(event);

      expect(emailDocSetSpy).not.toHaveBeenCalled();
    });

    // Event storage
    it("should store event response, statusCode, type, attempt, sg_event_id, sg_message_id", async () => {
      const { mockRef } = setupNotificationDoc();

      const event = makeEvent({
        event: "bounce",
        type: "bounce",
        reason: "550 Unknown",
        response: "550 5.1.1 The email account does not exist",
        status: "5.1.1",
        attempt: "3",
        sg_event_id: "evt-abc123",
        sg_message_id: "msg-xyz789",
      });

      await firestoreModule.processSendGridEvent(event);

      const updateData = mockRef.update.mock.calls[0][0];
      expect(updateData.bounceStatus).toBe(true);
      expect(updateData.bounceDate).toBeDefined();
      expect(updateData.bounceReason).toBe("550 Unknown");
      expect(updateData.bounceResponse).toBe("550 5.1.1 The email account does not exist");
      expect(updateData.bounceStatusCode).toBe("5.1.1");
      expect(updateData.bounceType).toBe("bounce");
      expect(updateData.bounceAttempt).toBe("3");
      expect(updateData.bounceSgEventId).toBe("evt-abc123");
      expect(updateData.bounceSgMessageId).toBe("msg-xyz789");
    });

    it("should skip duplicate event field rewrites while still applying suppression side effects", async () => {
      const { mockRef } = setupNotificationDoc({
        bounceSgEventId: "evt-duplicate",
      });

      await firestoreModule.processSendGridEvent(makeEvent({
        event: "bounce",
        type: "bounce",
        reason: "550 Unknown",
        sg_event_id: "evt-duplicate",
      }));

      expect(mockRef.update).not.toHaveBeenCalled();
      expect(emailDocSetSpy).toHaveBeenCalledWith(expect.objectContaining({
        reason: "hard_bounce",
        suppressBulk: true,
        suppressTransactional: true,
      }), { merge: true });
    });

    it("should store reason for all event types, not just bounce/dropped", async () => {
      const { mockRef } = setupNotificationDoc();

      const event = makeEvent({
        event: "deferred",
        reason: "Temporary failure",
        response: "421 Try again later",
      });

      await firestoreModule.processSendGridEvent(event);

      const updateData = mockRef.update.mock.calls[0][0];
      expect(updateData.deferredReason).toBe("Temporary failure");
      expect(updateData.deferredResponse).toBe("421 Try again later");
    });

    it("should fall back to SendGrid custom_args token linkage when the notification doc is missing token", async () => {
      setupNotificationDoc({ token: undefined });

      const event = makeEvent({
        event: "bounce",
        type: "bounce",
        token: "tok-from-event",
      });

      await firestoreModule.processSendGridEvent(event);

      expect(emailDocSetSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          token: "tok-from-event",
          reason: "hard_bounce",
        }),
        { merge: true }
      );
    });
  });
});
