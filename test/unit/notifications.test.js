require("../../utils/shared");
require("../../utils/firestore");
require("../../utils/bigquery");
require("@sendgrid/mail");
require("firebase-admin/functions");
require("@google-cloud/secret-manager");
require("twilio");

const sgMailMock = {
  setApiKey: vi.fn(),
  send: vi.fn().mockResolvedValue([{ statusCode: 202, body: {} }]),
};

const taskQueueMock = {
  enqueue: vi.fn().mockResolvedValue(undefined),
};

const taskQueueSelectorMock = vi.fn(() => taskQueueMock);

const functionsAdminMock = {
  getFunctions: vi.fn(() => ({
    taskQueue: taskQueueSelectorMock,
  })),
};

const bigqueryMock = {
  getParticipantsForNotificationsBQ: vi.fn().mockResolvedValue([]),
  countParticipantsForNotificationsBQ: vi.fn().mockResolvedValue(0),
};

const twilioClientMock = {
  messages: {
    create: vi.fn().mockResolvedValue({ sid: "SM_mock_sid" }),
  },
};

const twilioMock = vi.fn(() => twilioClientMock);

class SecretManagerServiceClientMock {
  async accessSecretVersion() {
    return [{ payload: { data: Buffer.from("mock-secret-value") } }];
  }
}

const secretManagerMock = {
  SecretManagerServiceClient: SecretManagerServiceClientMock,
};

const sharedMock = {
  getResponseJSON: vi.fn((message, code) => ({ message, code })),
  validEmailFormat: /^[^@\s]+@[^@\s]+\.[^@\s]+$/,
  getTemplateForEmailLink: vi.fn().mockReturnValue("<p>email body</p>"),
  getSecret: vi.fn(),
  parseResponseJson: vi.fn(),
  setHeadersDomainRestricted: vi.fn(),
  setHeaders: vi.fn(),
  logIPAddress: vi.fn(),
  redactEmailLoginInfo: vi.fn(),
  redactPhoneLoginInfo: vi.fn(),
  nihMailbox: "noreply@example.com",
  cidToLangMapper: {},
  unsubscribeTextObj: {},
  getAdjustedTime: vi.fn(),
  getEasternDateKey: vi.fn((date = new Date()) => {
    const formatter = new Intl.DateTimeFormat("en-CA", {
      timeZone: "America/New_York",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    });
    return formatter.format(date);
  }),
  parseRequestBody: (body) => {
    if (!body) return {};
    const parsed = typeof body === "string" ? JSON.parse(body) : body;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
    return parsed;
  },
  delay: vi.fn().mockResolvedValue(undefined),
  backoffMs: vi.fn((attempt) => 200 * Math.pow(2, attempt)),
  developmentTier: "DEV",
};

const firestoreMock = {
  generateSignInWithEmailLink: vi.fn(),
  getNotificationSpecById: vi.fn(),
  getNotificationSpecByCategoryAndAttempt: vi.fn(),
  getNotificationSpecsByScheduleOncePerDay: vi.fn(),
  markNotificationSpecsQueuedForRun: vi.fn().mockResolvedValue(0),
  clearNotificationSpecsQueuedRun: vi.fn().mockResolvedValue(0),
  markNotificationSpecsLastRun: vi.fn().mockResolvedValue(0),
  getBulkNotificationRun: vi.fn().mockResolvedValue(null),
  saveBulkNotificationRunPlan: vi.fn().mockResolvedValue({}),
  getBulkNotificationBatch: vi.fn(),
  getBulkNotificationRunBatches: vi.fn().mockResolvedValue([]),
  markBulkNotificationBatchEnqueued: vi.fn().mockResolvedValue(true),
  markBulkNotificationRunEnqueueFailed: vi.fn().mockResolvedValue(true),
  markBulkNotificationRunQueued: vi.fn().mockResolvedValue(true),
  markBulkNotificationBatchRunning: vi.fn().mockResolvedValue(true),
  markBulkNotificationBatchComplete: vi.fn().mockResolvedValue(true),
  markBulkNotificationBatchFailed: vi.fn().mockResolvedValue(true),
  finalizeBulkNotificationRunIfTerminal: vi.fn().mockResolvedValue({ finalized: false }),
  saveNotificationBatch: vi.fn().mockResolvedValue(undefined),
  getNotificationRecordId: vi.fn((record = {}) => [
    record.notificationSpecificationsID || "",
    record.notificationType || "",
    record.token || "",
  ].map((value) => encodeURIComponent(String(value))).join("__")),
  reserveNotificationBatch: vi.fn(async (records) => ({
    recordsToSend: records,
  })),
  markNotificationBatchProviderSendStarted: vi.fn(async (records) => ({
    recordsToSend: records,
    updatedCount: records.length,
  })),
  markNotificationBatchProviderAcceptanceUnknown: vi.fn().mockResolvedValue(0),
  markNotificationBatchAccepted: vi.fn(async (records = []) => records.length),
  markNotificationBatchFailed: vi.fn().mockResolvedValue(0),
  storeNotification: vi.fn().mockResolvedValue(undefined),
  checkIsNotificationSent: vi.fn(),
  updateSmsPermission: vi.fn(),
  getAppSettings: vi.fn().mockResolvedValue({}),
  // Suppression functions mocked here so notifications.js can import them
  addEmailSuppression: vi.fn().mockResolvedValue(undefined),
  isEmailSuppressed: vi.fn().mockResolvedValue(false),
  getEmailSuppressions: vi.fn().mockResolvedValue(new Set()),
};

const sharedPath = require.resolve("../../utils/shared");
const firestorePath = require.resolve("../../utils/firestore");
const bigqueryPath = require.resolve("../../utils/bigquery");
const sgMailPath = require.resolve("@sendgrid/mail");
const functionsAdminPath = require.resolve("firebase-admin/functions");
const secretManagerPath = require.resolve("@google-cloud/secret-manager");
const twilioPath = require.resolve("twilio");
const notificationsPath = require.resolve("../../utils/notifications");

const origSharedExports = require.cache[sharedPath].exports;
const origFirestoreExports = require.cache[firestorePath].exports;
const origBigqueryExports = require.cache[bigqueryPath].exports;
const origSgMailExports = require.cache[sgMailPath].exports;
const origFunctionsAdminExports = require.cache[functionsAdminPath].exports;
const origSecretManagerExports = require.cache[secretManagerPath].exports;
const origTwilioExports = require.cache[twilioPath].exports;

const { SmsBatchSender } = require("../../utils/notifications");

describe("Notifications Unit Tests", () => {
  let notificationsModule;
  let originalFetch;
  let fetchStub;

  const createResponseMock = () => ({
    status: vi.fn().mockReturnThis(),
    json: vi.fn().mockReturnThis(),
  });

  const createFetchResponse = ({ ok = true, status = 200, headers = {} } = {}) => ({
    ok,
    status,
    headers: {
      get: (name) => headers[name] || null,
    },
  });

  const mockGraphSecretLookups = () => {
    sharedMock.getSecret.mockImplementation((key) => {
      if (key === process.env.APP_REGISTRATION_CLIENT_ID) return Promise.resolve("client-id");
      if (key === process.env.APP_REGISTRATION_CLIENT_SECRET) return Promise.resolve("client-secret");
      if (key === process.env.APP_REGISTRATION_TENANT_ID) return Promise.resolve("tenant-id");
      return Promise.resolve(undefined);
    });
  };

  const setNotificationSettings = (overrides = {}) => {
    firestoreMock.getAppSettings.mockResolvedValue({
      notifications: { ...overrides },
    });
  };

  beforeEach(() => {
    originalFetch = global.fetch;

    // Reset all mocks
    sharedMock.getResponseJSON.mockReset().mockImplementation((message, code) => ({ message, code }));
    sharedMock.getTemplateForEmailLink.mockReset().mockReturnValue("<p>email body</p>");
    sharedMock.getSecret.mockReset().mockResolvedValue("fake-secret");
    sharedMock.parseResponseJson.mockReset();
    sharedMock.developmentTier = "DEV";

    firestoreMock.generateSignInWithEmailLink.mockReset();
    firestoreMock.getNotificationSpecByCategoryAndAttempt.mockReset();
    firestoreMock.getNotificationSpecsByScheduleOncePerDay.mockReset();
    firestoreMock.markNotificationSpecsQueuedForRun.mockReset().mockResolvedValue(0);
    firestoreMock.clearNotificationSpecsQueuedRun.mockReset().mockResolvedValue(0);
    firestoreMock.markNotificationSpecsLastRun.mockReset().mockResolvedValue(0);
    firestoreMock.getBulkNotificationRun.mockReset().mockResolvedValue(null);
    firestoreMock.saveBulkNotificationRunPlan.mockReset().mockResolvedValue({});
    firestoreMock.getBulkNotificationBatch.mockReset();
    firestoreMock.getBulkNotificationRunBatches.mockReset().mockResolvedValue([]);
    firestoreMock.markBulkNotificationBatchEnqueued.mockReset().mockResolvedValue(true);
    firestoreMock.markBulkNotificationRunEnqueueFailed.mockReset().mockResolvedValue(true);
    firestoreMock.markBulkNotificationRunQueued.mockReset().mockResolvedValue(true);
    firestoreMock.markBulkNotificationBatchRunning.mockReset().mockResolvedValue(true);
    firestoreMock.markBulkNotificationBatchComplete.mockReset().mockResolvedValue(true);
    firestoreMock.markBulkNotificationBatchFailed.mockReset().mockResolvedValue(true);
    firestoreMock.finalizeBulkNotificationRunIfTerminal.mockReset().mockResolvedValue({ finalized: false });
    firestoreMock.getNotificationRecordId.mockReset().mockImplementation((record = {}) => [
      record.notificationSpecificationsID || "",
      record.notificationType || "",
      record.token || "",
    ].map((value) => encodeURIComponent(String(value))).join("__"));
    firestoreMock.checkIsNotificationSent.mockReset();
    firestoreMock.storeNotification.mockReset().mockResolvedValue(undefined);
    firestoreMock.getAppSettings.mockReset().mockResolvedValue({});
    firestoreMock.saveNotificationBatch.mockReset().mockResolvedValue(undefined);
    firestoreMock.reserveNotificationBatch.mockReset().mockImplementation(async (records) => ({
      recordsToSend: records,
    }));
    firestoreMock.markNotificationBatchProviderSendStarted.mockReset().mockImplementation(async (records) => ({
      recordsToSend: records,
      updatedCount: records.length,
    }));
    firestoreMock.markNotificationBatchProviderAcceptanceUnknown.mockReset().mockResolvedValue(0);
    firestoreMock.markNotificationBatchAccepted.mockReset().mockImplementation(async (records = []) => records.length);
    firestoreMock.markNotificationBatchFailed.mockReset().mockResolvedValue(0);
    firestoreMock.isEmailSuppressed.mockReset().mockResolvedValue(false);
    firestoreMock.getEmailSuppressions.mockReset().mockResolvedValue(new Set());

    // Reset new mocks
    sgMailMock.setApiKey.mockReset();
    sgMailMock.send.mockReset().mockResolvedValue([{ statusCode: 202, body: {} }]);
    bigqueryMock.getParticipantsForNotificationsBQ.mockReset().mockResolvedValue([]);
    bigqueryMock.countParticipantsForNotificationsBQ.mockReset().mockResolvedValue(0);
    taskQueueMock.enqueue.mockReset().mockResolvedValue(undefined);
    taskQueueSelectorMock.mockReset().mockImplementation(() => taskQueueMock);
    functionsAdminMock.getFunctions.mockReset().mockImplementation(() => ({
      taskQueue: taskQueueSelectorMock,
    }));
    twilioMock.mockClear();
    twilioClientMock.messages.create.mockReset().mockResolvedValue({ sid: "SM_mock_sid" });

    // Install mocks into require.cache
    require.cache[sharedPath].exports = sharedMock;
    require.cache[firestorePath].exports = firestoreMock;
    require.cache[bigqueryPath].exports = bigqueryMock;
    require.cache[sgMailPath].exports = sgMailMock;
    require.cache[functionsAdminPath].exports = functionsAdminMock;
    require.cache[secretManagerPath].exports = secretManagerMock;
    require.cache[twilioPath].exports = twilioMock;

    // Clear module under test
    delete require.cache[notificationsPath];

    // Setup required secret/env inputs for the module under test
    process.env.APP_REGISTRATION_CLIENT_ID = "secret/client-id";
    process.env.APP_REGISTRATION_CLIENT_SECRET = "secret/client-secret";
    process.env.APP_REGISTRATION_TENANT_ID = "secret/tenant-id";
    process.env.GCLOUD_SENDGRID_SECRET = "secret/sendgrid-key";
    process.env.TWILIO_ACCOUNT_SID = "secret/twilio-account";
    process.env.TWILIO_AUTH_TOKEN = "secret/twilio-auth";
    process.env.TWILIO_MESSAGING_SERVICE_SID = "secret/twilio-service";

    // Mock global fetch
    fetchStub = vi.fn();
    global.fetch = fetchStub;

    // Now require the module under test
    notificationsModule = require("../../utils/notifications");
  });

  afterEach(() => {
    vi.restoreAllMocks();
    if (originalFetch === undefined) {
      delete global.fetch;
    } else {
      global.fetch = originalFetch;
    }
    delete require.cache[notificationsPath];
    delete process.env.APP_REGISTRATION_CLIENT_ID;
    delete process.env.APP_REGISTRATION_CLIENT_SECRET;
    delete process.env.APP_REGISTRATION_TENANT_ID;
    delete process.env.GCLOUD_PROJECT;
    delete process.env.GCLOUD_SENDGRID_SECRET;
    delete process.env.GCLOUD_UNSUBSCRIBE_SECRET;
    delete process.env.SG_UNSUBSCRIBE_URL;
    delete process.env.TWILIO_ACCOUNT_SID;
    delete process.env.TWILIO_AUTH_TOKEN;
    delete process.env.TWILIO_MESSAGING_SERVICE_SID;
    delete process.env.WEBHOOK_REGION;
  });

  afterAll(() => {
    require.cache[sharedPath].exports = origSharedExports;
    require.cache[firestorePath].exports = origFirestoreExports;
    require.cache[bigqueryPath].exports = origBigqueryExports;
    require.cache[sgMailPath].exports = origSgMailExports;
    require.cache[functionsAdminPath].exports = origFunctionsAdminExports;
    require.cache[secretManagerPath].exports = origSecretManagerExports;
    require.cache[twilioPath].exports = origTwilioExports;
    delete require.cache[notificationsPath];
  });

  describe("sendEmailLink", () => {
    it("should return 405 for non-POST requests", async () => {
      const req = { method: "GET" };
      const res = createResponseMock();

      await notificationsModule.sendEmailLink(req, res);

      expect(res.status).toHaveBeenCalledWith(405);
      expect(res.json.mock.calls[0][0].code).toBe(405);
    });

    it("should return 400 when email is missing", async () => {
      const req = {
        method: "POST",
        body: { continueUrl: "https://example.com" },
      };
      const res = createResponseMock();

      await notificationsModule.sendEmailLink(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
      const payload = res.json.mock.calls[0][0];
      expect(payload.errorCode).toBe("auth/missing-email");
      expect(payload.code).toBe(400);
      expect(payload.authFlowId).toBeTypeOf("string");
      expect(payload.authAttemptId).toBeTypeOf("string");
    });

    it("should return 400 when continueUrl is missing", async () => {
      const req = {
        method: "POST",
        body: { email: "user@example.com" },
      };
      const res = createResponseMock();

      await notificationsModule.sendEmailLink(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
      const payload = res.json.mock.calls[0][0];
      expect(payload.errorCode).toBe("auth/missing-continue-uri");
      expect(payload.code).toBe(400);
    });

    it("should return 400 when email format is invalid", async () => {
      const req = {
        method: "POST",
        body: { email: "bad-email", continueUrl: "https://example.com" },
      };
      const res = createResponseMock();

      await notificationsModule.sendEmailLink(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
      const payload = res.json.mock.calls[0][0];
      expect(payload.errorCode).toBe("auth/invalid-email");
    });

    it("should send email via Graph and return 202 with correlation fields", async () => {
      const req = {
        method: "POST",
        body: {
          email: "user@example.com",
          continueUrl: "https://app.example.com",
          preferredLanguage: "english",
          authFlowId: "auth_flow_client_1",
          authAttemptId: "auth_attempt_client_1",
          clientSendTs: "2026-02-12T10:00:00.000Z",
        },
      };
      const res = createResponseMock();

      mockGraphSecretLookups();

      firestoreMock.generateSignInWithEmailLink.mockResolvedValue(
        "https://auth.example.com/?continueUrl=https://app.example.com&lang=en",
      );

      sharedMock.parseResponseJson
        .mockResolvedValueOnce({ access_token: "graph-access-token" })
        .mockResolvedValueOnce(null);

      fetchStub
        .mockResolvedValueOnce(createFetchResponse({ ok: true, status: 200 }))
        .mockResolvedValueOnce(
          createFetchResponse({
            ok: true,
            status: 202,
            headers: {
              "request-id": "graph-request-123",
              "client-request-id": "graph-client-123",
            },
          }),
        );

      await notificationsModule.sendEmailLink(req, res);

      expect(fetchStub).toHaveBeenCalledTimes(2);
      const graphCall = fetchStub.mock.calls[1];
      expect(graphCall[0]).toContain("https://graph.microsoft.com/v1.0/users/");
      expect(graphCall[1].headers.Authorization).toBe("Bearer graph-access-token");
      expect(graphCall[1].headers["return-client-request-id"]).toBe("true");
      expect(graphCall[1].headers["client-request-id"]).toBeTypeOf("string");

      expect(res.status).toHaveBeenCalledWith(202);
      const payload = res.json.mock.calls[0][0];
      expect(payload.code).toBe(202);
      expect(payload.errorCode).toBe(null);
      expect(payload.provider).toBe("microsoft_graph");
      expect(payload.providerStatus).toBe("accepted");
      expect(payload.graphRequestId).toBe("graph-request-123");
      expect(payload.graphClientRequestId).toBe("graph-client-123");
      expect(payload.authFlowId).toBe("auth_flow_client_1");
      expect(payload.authAttemptId).toBe("auth_attempt_client_1");
      expect(payload.clientSendTs).toBe("2026-02-12T10:00:00.000Z");
    });

    it("should map Graph 400 invalid provider error to auth/invalid-email", async () => {
      const req = {
        method: "POST",
        body: {
          email: "user@example.com",
          continueUrl: "https://app.example.com",
        },
      };
      const res = createResponseMock();

      mockGraphSecretLookups();

      firestoreMock.generateSignInWithEmailLink.mockResolvedValue(
        "https://auth.example.com/?continueUrl=https://app.example.com",
      );

      sharedMock.parseResponseJson
        .mockResolvedValueOnce({ access_token: "graph-access-token" })
        .mockResolvedValueOnce({
          error: {
            code: "InvalidRecipients",
            message: "Recipient is invalid",
          },
        });

      fetchStub
        .mockResolvedValueOnce(createFetchResponse({ ok: true, status: 200 }))
        .mockResolvedValueOnce(
          createFetchResponse({
            ok: false,
            status: 400,
            headers: {
              "request-id": "graph-request-400",
              "client-request-id": "graph-client-400",
            },
          }),
        );

      await notificationsModule.sendEmailLink(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
      const payload = res.json.mock.calls[0][0];
      expect(payload.errorCode).toBe("auth/invalid-email");
      expect(payload.providerStatus).toBe("failed");
      expect(payload.providerErrorCode).toBe("InvalidRecipients");
      expect(payload.graphRequestId).toBe("graph-request-400");
    });

    it("should map Graph 400 non-invalid provider error to auth/operation-not-allowed", async () => {
      const req = {
        method: "POST",
        body: {
          email: "user@example.com",
          continueUrl: "https://app.example.com",
        },
      };
      const res = createResponseMock();

      mockGraphSecretLookups();

      firestoreMock.generateSignInWithEmailLink.mockResolvedValue(
        "https://auth.example.com/?continueUrl=https://app.example.com",
      );

      sharedMock.parseResponseJson
        .mockResolvedValueOnce({ access_token: "graph-access-token" })
        .mockResolvedValueOnce({
          error: {
            code: "MailboxNotEnabledForRESTAPI",
            message: "Mailbox is not enabled",
          },
        });

      fetchStub
        .mockResolvedValueOnce(createFetchResponse({ ok: true, status: 200 }))
        .mockResolvedValueOnce(
          createFetchResponse({
            ok: false,
            status: 400,
            headers: {
              "request-id": "graph-request-400b",
              "client-request-id": "graph-client-400b",
            },
          }),
        );

      await notificationsModule.sendEmailLink(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
      const payload = res.json.mock.calls[0][0];
      expect(payload.errorCode).toBe("auth/operation-not-allowed");
      expect(payload.providerErrorCode).toBe("MailboxNotEnabledForRESTAPI");
    });

    it("should map Graph 401 to auth/operation-not-allowed", async () => {
      const req = {
        method: "POST",
        body: {
          email: "user@example.com",
          continueUrl: "https://app.example.com",
        },
      };
      const res = createResponseMock();

      mockGraphSecretLookups();

      firestoreMock.generateSignInWithEmailLink.mockResolvedValue(
        "https://auth.example.com/?continueUrl=https://app.example.com",
      );

      sharedMock.parseResponseJson
        .mockResolvedValueOnce({ access_token: "graph-access-token" })
        .mockResolvedValueOnce({
          error: {
            code: "InvalidAuthenticationToken",
            message: "Access token is invalid",
          },
        });

      fetchStub
        .mockResolvedValueOnce(createFetchResponse({ ok: true, status: 200 }))
        .mockResolvedValueOnce(
          createFetchResponse({
            ok: false,
            status: 401,
            headers: {
              "request-id": "graph-request-401",
              "client-request-id": "graph-client-401",
            },
          }),
        );

      await notificationsModule.sendEmailLink(req, res);

      expect(res.status).toHaveBeenCalledWith(401);
      const payload = res.json.mock.calls[0][0];
      expect(payload.code).toBe(401);
      expect(payload.errorCode).toBe("auth/operation-not-allowed");
      expect(payload.providerErrorCode).toBe("InvalidAuthenticationToken");
    });

    it("should map Firebase link generation errors to 400 when code is invalid continue url", async () => {
      const req = {
        method: "POST",
        body: {
          email: "user@example.com",
          continueUrl: "https://app.example.com",
        },
      };
      const res = createResponseMock();

      const linkError = new Error("Invalid continue URL");
      linkError.code = "auth/invalid-continue-uri";
      firestoreMock.generateSignInWithEmailLink.mockRejectedValue(linkError);

      await notificationsModule.sendEmailLink(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
      const payload = res.json.mock.calls[0][0];
      expect(payload.errorCode).toBe("auth/invalid-continue-uri");
      expect(payload.code).toBe(400);
    });

    it("should map auth/too-many-requests to HTTP 429 in catch path", async () => {
      const req = {
        method: "POST",
        body: {
          email: "user@example.com",
          continueUrl: "https://app.example.com",
        },
      };
      const res = createResponseMock();

      const tooManyRequestsError = new Error("Too many requests");
      tooManyRequestsError.code = "auth/too-many-requests";
      firestoreMock.generateSignInWithEmailLink.mockRejectedValue(tooManyRequestsError);

      await notificationsModule.sendEmailLink(req, res);

      expect(res.status).toHaveBeenCalledWith(429);
      const payload = res.json.mock.calls[0][0];
      expect(payload.errorCode).toBe("auth/too-many-requests");
      expect(payload.code).toBe(429);
    });

    it("should return 502 when Graph token acquisition fails", async () => {
      const req = {
        method: "POST",
        body: {
          email: "user@example.com",
          continueUrl: "https://app.example.com",
        },
      };
      const res = createResponseMock();

      mockGraphSecretLookups();

      firestoreMock.generateSignInWithEmailLink.mockResolvedValue(
        "https://auth.example.com/?continueUrl=https://app.example.com",
      );

      sharedMock.parseResponseJson
        .mockResolvedValueOnce({ error: { code: "invalid_client" } });
      fetchStub.mockResolvedValueOnce(createFetchResponse({ ok: false, status: 401 }));

      await notificationsModule.sendEmailLink(req, res);

      expect(res.status).toHaveBeenCalledWith(502);
      const payload = res.json.mock.calls[0][0];
      expect(payload.code).toBe(502);
      expect(payload.errorCode).toBe("auth/operation-not-allowed");
      expect(payload.upstreamStatus).toBe(401);
      expect(payload.providerStatus).toBe("failed");
    });
  });

  // Baseline specs for email pipeline

  describe("sendEmail", () => {
    it("should await sgMail.send and propagate errors on rejection", async () => {
      const sendError = new Error("SendGrid API error");
      sgMailMock.send.mockRejectedValue(sendError);
      sharedMock.getSecret.mockResolvedValue("fake-api-key");
      sharedMock.developmentTier = "PROD";
      process.env.GCLOUD_SENDGRID_SECRET = "secret/sendgrid-key";

      await expect(
        notificationsModule.sendEmail("test@example.com", "Subject", "<p>Body</p>")
      ).rejects.toThrow("SendGrid API error");
    });

    it("should resolve successfully when sgMail.send succeeds", async () => {
      sgMailMock.send.mockResolvedValue([{ statusCode: 202 }]);
      sharedMock.getSecret.mockResolvedValue("fake-api-key");
      sharedMock.developmentTier = "PROD";
      process.env.GCLOUD_SENDGRID_SECRET = "secret/sendgrid-key";

      await expect(
        notificationsModule.sendEmail("test@example.com", "Subject", "<p>Body</p>")
      ).resolves.not.toThrow();

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      const msg = sgMailMock.send.mock.calls[0][0];
      expect(msg.to).toBe("test@example.com");
      expect(msg.subject).toBe("Subject");
      expect(msg.html).toBe("<p>Body</p>");
    });

    it("should include text (plaintext) property in the message", async () => {
      sgMailMock.send.mockResolvedValue([{ statusCode: 202 }]);
      sharedMock.getSecret.mockResolvedValue("fake-api-key");
      sharedMock.developmentTier = "PROD";
      process.env.GCLOUD_SENDGRID_SECRET = "secret/sendgrid-key";

      await notificationsModule.sendEmail("test@example.com", "Subject", "<p>Hello <b>world</b></p>");

      const msg = sgMailMock.send.mock.calls[0][0];
      expect(msg.text).toBeDefined();
      expect(msg.text).toBeTypeOf("string");
      expect(msg.text.length).toBeGreaterThan(0);
    });

    it("should retry setupSendGrid after initial failure", async () => {
      sharedMock.getSecret
        .mockRejectedValueOnce(new Error("Secret fetch failed"))
        .mockResolvedValueOnce("fake-api-key");
      sgMailMock.send.mockResolvedValue([{ statusCode: 202 }]);
      sharedMock.developmentTier = "PROD";
      process.env.GCLOUD_SENDGRID_SECRET = "secret/sendgrid-key";

      // First call should fail (setupSendGrid fails)
      await expect(
        notificationsModule.sendEmail("test@example.com", "Subject", "<p>Body</p>")
      ).rejects.toThrow("Secret fetch failed");

      // Second call should succeed (setupSendGrid retries because flag was reset)
      await expect(
        notificationsModule.sendEmail("test@example.com", "Subject", "<p>Body</p>")
      ).resolves.not.toThrow();
    });

    it("should send via SendGrid sandbox mode when configured", async () => {
      sgMailMock.send.mockResolvedValue([{ statusCode: 200 }]);
      sharedMock.getSecret.mockResolvedValue("fake-api-key");
      process.env.GCLOUD_PROJECT = "nih-nci-dceg-connect-stg-5519";
      sharedMock.developmentTier = "STAGE";
      setNotificationSettings({ nonProdEmailAllowlist: ["test@example.com"] });
      process.env.GCLOUD_SENDGRID_SECRET = "secret/sendgrid-key";

      await notificationsModule.sendEmail("test@example.com", "Subject", "<p>Body</p>");

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      const msg = sgMailMock.send.mock.calls[0][0];
      expect(msg.mail_settings?.sandbox_mode?.enable).toBe(true);
    });

    it("should honor sendgridDeliveryModeOverride from appSettings in non-prod", async () => {
      sgMailMock.send.mockResolvedValue([{ statusCode: 202 }]);
      sharedMock.getSecret.mockResolvedValue("fake-api-key");
      process.env.GCLOUD_PROJECT = "nih-nci-dceg-connect-stg-5519";
      sharedMock.developmentTier = "STAGE";
      setNotificationSettings({
        sendgridDeliveryModeOverride: "live",
        nonProdEmailAllowlist: ["test@example.com"],
      });
      process.env.GCLOUD_SENDGRID_SECRET = "secret/sendgrid-key";

      await notificationsModule.sendEmail("test@example.com", "Subject", "<p>Body</p>");

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      const msg = sgMailMock.send.mock.calls[0][0];
      expect(msg.mail_settings?.sandbox_mode).toBeUndefined();
    });

    it("should skip provider send in noop mode", async () => {
      process.env.GCLOUD_PROJECT = "nih-nci-dceg-connect-dev";
      sharedMock.developmentTier = "DEV";

      await expect(
        notificationsModule.sendEmail("test@example.com", "Subject", "<p>Body</p>")
      ).resolves.not.toThrow();

      expect(sgMailMock.send).not.toHaveBeenCalled();
    });

    it("should block non-allowlisted recipients in non-prod sandbox/live mode", async () => {
      sharedMock.getSecret.mockResolvedValue("fake-api-key");
      process.env.GCLOUD_PROJECT = "nih-nci-dceg-connect-stg-5519";
      sharedMock.developmentTier = "STAGE";
      setNotificationSettings({ nonProdEmailAllowlist: ["allowed@example.com"] });
      process.env.GCLOUD_SENDGRID_SECRET = "secret/sendgrid-key";

      await expect(
        notificationsModule.sendEmail("blocked@example.com", "Subject", "<p>Body</p>")
      ).rejects.toThrow("Blocked non-prod SendGrid sandbox send: 1 non-allowlisted recipient(s)");

      expect(sgMailMock.send).not.toHaveBeenCalled();
    });

    it("should include cc recipients in the non-prod allowlist check", async () => {
      sharedMock.getSecret.mockResolvedValue("fake-api-key");
      process.env.GCLOUD_PROJECT = "nih-nci-dceg-connect-stg-5519";
      sharedMock.developmentTier = "STAGE";
      setNotificationSettings({ nonProdEmailAllowlist: ["allowed@example.com"] });
      process.env.GCLOUD_SENDGRID_SECRET = "secret/sendgrid-key";

      await expect(
        notificationsModule.sendEmail("allowed@example.com", "Subject", "<p>Body</p>", "blocked@example.com")
      ).rejects.toThrow("Blocked non-prod SendGrid sandbox send: 1 non-allowlisted recipient(s)");

      expect(sgMailMock.send).not.toHaveBeenCalled();
    });

    it("should ignore invalid sendgridDeliveryModeOverride values", async () => {
      sgMailMock.send.mockResolvedValue([{ statusCode: 200 }]);
      sharedMock.getSecret.mockResolvedValue("fake-api-key");
      process.env.GCLOUD_PROJECT = "nih-nci-dceg-connect-stg-5519";
      sharedMock.developmentTier = "STAGE";
      setNotificationSettings({
        sendgridDeliveryModeOverride: "unsafe-live-ish",
        nonProdEmailAllowlist: ["test@example.com"],
      });
      process.env.GCLOUD_SENDGRID_SECRET = "secret/sendgrid-key";

      await notificationsModule.sendEmail("test@example.com", "Subject", "<p>Body</p>");

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      const msg = sgMailMock.send.mock.calls[0][0];
      expect(msg.mail_settings?.sandbox_mode?.enable).toBe(true);
    });

    it("should ignore sendgridDeliveryModeOverride in prod", async () => {
      sgMailMock.send.mockResolvedValue([{ statusCode: 202 }]);
      sharedMock.getSecret.mockResolvedValue("fake-api-key");
      process.env.GCLOUD_PROJECT = "nih-nci-dceg-connect-prod-6d04";
      sharedMock.developmentTier = "PROD";
      setNotificationSettings({ sendgridDeliveryModeOverride: "noop" });
      process.env.GCLOUD_SENDGRID_SECRET = "secret/sendgrid-key";

      await notificationsModule.sendEmail("test@example.com", "Subject", "<p>Body</p>");

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
    });
  });

  describe("sendScheduledNotifications", () => {
    const makeScheduledSpec = (overrides = {}) => ({
      id: "sched-spec-1",
      category: "reminder",
      attempt: "1st",
      primaryField: "d_821247024",
      time: { start: { day: 0, hour: 1, minute: 0 }, stop: { day: 0, hour: 0, minute: 0 } },
      notificationType: ["email"],
      emailField: "d_335767902",
      phoneField: "",
      firstNameField: "d_153098809",
      preferredNameField: "",
      email: {
        english: { subject: "Scheduled Subject", body: "<p>Hello {{firstName}}</p>" },
      },
      sms: {},
      conditions: JSON.stringify([["d_821247024", "equals", "197316935"]]),
      ...overrides,
    });

    const makeScheduledParticipant = (token) => ({
      Connect_ID: `C-${token}`,
      token,
      state: { uid: `uid-${token}` },
      d_335767902: `${token}@test.gov`,
      d_153098809: "Taylor",
      353358909: 0,
    });

    it("should enqueue threshold-promoted bulk specs as Cloud Tasks and mark them queued instead of complete", async () => {
      setNotificationSettings({ useCloudTasksBulk: true });
      sharedMock.getAdjustedTime.mockReturnValue(new Date("2026-01-01T00:00:00.000Z"));
      firestoreMock.getNotificationSpecsByScheduleOncePerDay.mockResolvedValue([
        makeScheduledSpec({ id: "spec-bulk-threshold", bulkRunSequence: 2 }),
      ]);
      bigqueryMock.countParticipantsForNotificationsBQ.mockResolvedValue(6000);
      bigqueryMock.getParticipantsForNotificationsBQ
        .mockResolvedValueOnce([makeScheduledParticipant("tok-bulk")])
        .mockResolvedValueOnce([]);

      const req = { method: "POST", body: { scheduleAt: "09:00" } };
      const res = createResponseMock();

      await notificationsModule.sendScheduledNotifications(req, res);

      expect(taskQueueMock.enqueue).toHaveBeenCalledTimes(1);
      const [payload, opts] = taskQueueMock.enqueue.mock.calls[0];
      expect(payload).toMatchObject({
        runId: `${payload.specId}-${payload.runDateKey}-run-3`,
        batchId: "default-batch-1",
        lane: "default",
        specId: "spec-bulk-threshold",
        runSequence: 3,
      });
      expect(firestoreMock.saveBulkNotificationRunPlan).toHaveBeenCalledTimes(1);
      const [{ runDoc, batchDocs }] = firestoreMock.saveBulkNotificationRunPlan.mock.calls[0];
      expect(runDoc).toMatchObject({
        id: `${payload.specId}-${payload.runDateKey}-run-3`,
        specId: "spec-bulk-threshold",
        runSequence: 3,
        plannedRecipientCount: 1,
        conditions: [{ field: "d_821247024", operator: "equals", value: "197316935" }],
      });
      expect(batchDocs).toHaveLength(1);
      expect(batchDocs[0]).toMatchObject({
        id: "default-batch-1",
        lane: "default",
        recipientCount: 1,
        scheduleDelaySeconds: 0,
      });
      expect(payload.runSequence).toBe(3);
      expect(opts.dispatchDeadlineSeconds).toBe(1800);
      expect(opts.scheduleDelaySeconds).toBe(0);
      expect(opts.id).toBe(`${payload.specId}-${payload.runDateKey}-run-3-default-batch-1`);
      expect(taskQueueSelectorMock).toHaveBeenCalledWith("processNotificationBatchBulkDefault");
      expect(firestoreMock.markNotificationSpecsQueuedForRun)
        .toHaveBeenCalledWith(
          ["spec-bulk-threshold"],
          payload.runDateKey,
          undefined,
          { "spec-bulk-threshold": 3 },
          { commitRunSequence: false },
        );
      expect(firestoreMock.markNotificationSpecsQueuedForRun.mock.invocationCallOrder[0])
        .toBeLessThan(taskQueueMock.enqueue.mock.invocationCallOrder[0]);
      expect(firestoreMock.markNotificationSpecsLastRun).not.toHaveBeenCalled();
      expect(sgMailMock.send).not.toHaveBeenCalled();
      expect(res.status).toHaveBeenCalledWith(200);
    });

    it("should stagger initial bulk tasks across queued specs", async () => {
      setNotificationSettings({ useCloudTasksBulk: true, targetRecipientsPerHour: 5000, targetRecipientsPerHourMicrosoft: 1500 });
      sharedMock.getAdjustedTime.mockReturnValue(new Date("2026-01-01T00:00:00.000Z"));
      firestoreMock.getNotificationSpecsByScheduleOncePerDay.mockResolvedValue([
        makeScheduledSpec({ id: "spec-bulk-a", category: "newsletter" }),
        makeScheduledSpec({ id: "spec-bulk-b", category: "newsletter" }),
      ]);
      bigqueryMock.countParticipantsForNotificationsBQ.mockResolvedValue(6000);
      bigqueryMock.getParticipantsForNotificationsBQ
        .mockResolvedValueOnce([makeScheduledParticipant("tok-a")])
        .mockResolvedValueOnce([{ ...makeScheduledParticipant("tok-b"), d_335767902: "tok-b@outlook.com" }])
        .mockResolvedValue([]);

      const req = { method: "POST", body: { scheduleAt: "09:00" } };
      const res = createResponseMock();

      await notificationsModule.sendScheduledNotifications(req, res);

      expect(taskQueueMock.enqueue).toHaveBeenCalledTimes(2);
      expect(taskQueueMock.enqueue.mock.calls[0][1].scheduleDelaySeconds).toBe(0);
      expect(taskQueueMock.enqueue.mock.calls[1][1].scheduleDelaySeconds).toBe(0);
      expect(taskQueueMock.enqueue.mock.calls[0][0].lane).toBe("default");
      expect(taskQueueMock.enqueue.mock.calls[1][0].lane).toBe("microsoft");
      expect(taskQueueMock.enqueue.mock.calls[0][0].runSequence).toBe(1);
      expect(taskQueueMock.enqueue.mock.calls[1][0].runSequence).toBe(1);
      expect(taskQueueSelectorMock).toHaveBeenCalledWith("processNotificationBatchBulkDefault");
      expect(taskQueueSelectorMock).toHaveBeenCalledWith("processNotificationBatchBulkMicrosoft");
      expect(firestoreMock.markNotificationSpecsQueuedForRun).toHaveBeenNthCalledWith(
        1,
        ["spec-bulk-a"],
        taskQueueMock.enqueue.mock.calls[0][0].runDateKey,
        undefined,
        { "spec-bulk-a": 1 },
        { commitRunSequence: false },
      );
      expect(firestoreMock.markNotificationSpecsQueuedForRun).toHaveBeenNthCalledWith(
        2,
        ["spec-bulk-b"],
        taskQueueMock.enqueue.mock.calls[1][0].runDateKey,
        undefined,
        { "spec-bulk-b": 1 },
        { commitRunSequence: false },
      );
      expect(firestoreMock.markNotificationSpecsLastRun).not.toHaveBeenCalled();
      expect(res.status).toHaveBeenCalledWith(200);
    });

    it("should materialize lane-specific planned batches with configured sizes and schedule delays", async () => {
      setNotificationSettings({
        useCloudTasksBulk: true,
        bulkDefaultBatchSize: 2,
        bulkMicrosoftBatchSize: 1,
        targetRecipientsPerHour: 3600,
        targetRecipientsPerHourMicrosoft: 1800,
      });
      sharedMock.getAdjustedTime.mockReturnValue(new Date("2026-01-01T00:00:00.000Z"));
      firestoreMock.getNotificationSpecsByScheduleOncePerDay.mockResolvedValue([
        makeScheduledSpec({ id: "spec-planned-lanes", category: "newsletter" }),
      ]);
      bigqueryMock.countParticipantsForNotificationsBQ.mockResolvedValue(5);
      bigqueryMock.getParticipantsForNotificationsBQ.mockResolvedValueOnce([
        makeScheduledParticipant("tok-default-1"),
        makeScheduledParticipant("tok-default-2"),
        makeScheduledParticipant("tok-default-3"),
        { ...makeScheduledParticipant("tok-ms-1"), d_335767902: "tok-ms-1@outlook.com" },
        { ...makeScheduledParticipant("tok-ms-2"), d_335767902: "tok-ms-2@hotmail.com" },
      ]);

      const req = { method: "POST", body: { scheduleAt: "09:00" } };
      const res = createResponseMock();

      await notificationsModule.sendScheduledNotifications(req, res);

      const [{ batchDocs }] = firestoreMock.saveBulkNotificationRunPlan.mock.calls[0];
      expect(batchDocs.map((batch) => ({
        id: batch.id,
        lane: batch.lane,
        recipientCount: batch.recipientCount,
        scheduleDelaySeconds: batch.scheduleDelaySeconds,
      }))).toEqual([
        { id: "default-batch-1", lane: "default", recipientCount: 2, scheduleDelaySeconds: 0 },
        { id: "default-batch-2", lane: "default", recipientCount: 1, scheduleDelaySeconds: 2 },
        { id: "microsoft-batch-1", lane: "microsoft", recipientCount: 1, scheduleDelaySeconds: 0 },
        { id: "microsoft-batch-2", lane: "microsoft", recipientCount: 1, scheduleDelaySeconds: 2 },
      ]);
      expect(batchDocs.some((batch) => Object.prototype.hasOwnProperty.call(batch, "scheduledFor"))).toBe(false);
      expect(taskQueueMock.enqueue).toHaveBeenCalledTimes(4);
      expect(taskQueueSelectorMock).toHaveBeenCalledWith("processNotificationBatchBulkDefault");
      expect(taskQueueSelectorMock).toHaveBeenCalledWith("processNotificationBatchBulkMicrosoft");
      expect(firestoreMock.markBulkNotificationBatchEnqueued).toHaveBeenCalledWith(expect.objectContaining({
        batchId: "default-batch-1",
        scheduledFor: expect.any(String),
      }));
      expect(res.status).toHaveBeenCalledWith(200);
    });

    it("should page through BigQuery while materializing a planned bulk run", async () => {
      setNotificationSettings({
        useCloudTasksBulk: true,
        notificationBatchLimit: 2,
        bulkDefaultBatchSize: 10,
      });
      sharedMock.getAdjustedTime.mockReturnValue(new Date("2026-01-01T00:00:00.000Z"));
      firestoreMock.getNotificationSpecsByScheduleOncePerDay.mockResolvedValue([
        makeScheduledSpec({ id: "spec-paged-plan", category: "newsletter" }),
      ]);
      bigqueryMock.countParticipantsForNotificationsBQ.mockResolvedValue(3);
      bigqueryMock.getParticipantsForNotificationsBQ
        .mockResolvedValueOnce([
          makeScheduledParticipant("tok-1"),
          makeScheduledParticipant("tok-2"),
        ])
        .mockResolvedValueOnce([
          makeScheduledParticipant("tok-3"),
        ]);

      const req = { method: "POST", body: { scheduleAt: "09:00" } };
      const res = createResponseMock();

      await notificationsModule.sendScheduledNotifications(req, res);

      expect(bigqueryMock.getParticipantsForNotificationsBQ).toHaveBeenCalledTimes(2);
      expect(bigqueryMock.getParticipantsForNotificationsBQ.mock.calls[0][0]).toMatchObject({
        notificationSpecId: "spec-paged-plan",
        limit: 2,
        previousToken: "",
      });
      expect(bigqueryMock.getParticipantsForNotificationsBQ.mock.calls[1][0]).toMatchObject({
        notificationSpecId: "spec-paged-plan",
        limit: 2,
        previousToken: "tok-2",
      });
      const [{ runDoc, batchDocs }] = firestoreMock.saveBulkNotificationRunPlan.mock.calls[0];
      expect(runDoc.plannedRecipientCount).toBe(3);
      expect(batchDocs).toHaveLength(1);
      expect(batchDocs[0].recipients.map((recipient) => recipient.token)).toEqual(["tok-1", "tok-2", "tok-3"]);
      expect(taskQueueMock.enqueue).toHaveBeenCalledTimes(1);
      expect(res.status).toHaveBeenCalledWith(200);
    });

    it("should store minimal planned recipients with redacted login details", async () => {
      const conceptIds = require("../../utils/fieldToConceptIdMapping");
      setNotificationSettings({ useCloudTasksBulk: true });
      sharedMock.redactPhoneLoginInfo.mockReturnValue("***-4567");
      sharedMock.redactEmailLoginInfo.mockReturnValue("a***@example.org");
      sharedMock.getAdjustedTime.mockReturnValue(new Date("2026-01-01T00:00:00.000Z"));
      firestoreMock.getNotificationSpecsByScheduleOncePerDay.mockResolvedValue([
        makeScheduledSpec({
          id: "spec-login-plan",
          category: "newsletter",
          email: {
            english: { subject: "Login", body: "<p>Hello <firstName>, <loginDetails></p>" },
          },
        }),
      ]);
      bigqueryMock.countParticipantsForNotificationsBQ.mockResolvedValue(1);
      bigqueryMock.getParticipantsForNotificationsBQ.mockResolvedValueOnce([
        {
          ...makeScheduledParticipant("tok-login"),
          state: { uid: "uid-tok-login", extra: "do-not-persist" },
          [conceptIds.preferredLanguage]: conceptIds.english,
          [conceptIds.signInMechanism]: "passwordAndPhone",
          [conceptIds.authenticationPhone]: "+15551234567",
          [conceptIds.authenticationEmail]: "auth-email@example.org",
        },
      ]);

      const req = { method: "POST", body: { scheduleAt: "09:00" } };
      const res = createResponseMock();

      await notificationsModule.sendScheduledNotifications(req, res);

      const [{ batchDocs }] = firestoreMock.saveBulkNotificationRunPlan.mock.calls[0];
      const [plannedRecipient] = batchDocs[0].recipients;
      expect(plannedRecipient).toEqual(expect.objectContaining({
        Connect_ID: "C-tok-login",
        token: "tok-login",
        state: { uid: "uid-tok-login" },
        [conceptIds.preferredLanguage]: conceptIds.english,
        d_153098809: "Taylor",
        d_335767902: "tok-login@test.gov",
        loginDetails: "***-4567, a***@example.org",
      }));
      expect(plannedRecipient).not.toHaveProperty("email");
      expect(plannedRecipient).not.toHaveProperty(`${conceptIds.signInMechanism}`);
      expect(plannedRecipient).not.toHaveProperty(`${conceptIds.authenticationPhone}`);
      expect(plannedRecipient).not.toHaveProperty(`${conceptIds.authenticationEmail}`);
      expect(plannedRecipient.state).not.toHaveProperty("extra");
      expect(res.status).toHaveBeenCalledWith(200);
    });

    it("should mark an empty planned bulk run successful without enqueuing a task", async () => {
      setNotificationSettings({ useCloudTasksBulk: true });
      sharedMock.getAdjustedTime.mockReturnValue(new Date("2026-01-01T00:00:00.000Z"));
      firestoreMock.getNotificationSpecsByScheduleOncePerDay.mockResolvedValue([
        makeScheduledSpec({ id: "spec-empty-plan", category: "newsletter" }),
      ]);
      bigqueryMock.countParticipantsForNotificationsBQ.mockResolvedValue(10);
      bigqueryMock.getParticipantsForNotificationsBQ.mockResolvedValueOnce([]);

      const req = { method: "POST", body: { scheduleAt: "09:00" } };
      const res = createResponseMock();

      await notificationsModule.sendScheduledNotifications(req, res);

      expect(firestoreMock.saveBulkNotificationRunPlan).toHaveBeenCalledTimes(1);
      const [{ runDoc, batchDocs }] = firestoreMock.saveBulkNotificationRunPlan.mock.calls[0];
      expect(runDoc.specId).toBe("spec-empty-plan");
      expect(runDoc.plannedRecipientCount).toBe(0);
      expect(batchDocs).toEqual([]);
      expect(taskQueueMock.enqueue).not.toHaveBeenCalled();
      expect(firestoreMock.markNotificationSpecsQueuedForRun).not.toHaveBeenCalled();
      expect(firestoreMock.finalizeBulkNotificationRunIfTerminal).toHaveBeenCalledWith(runDoc.id);
      expect(firestoreMock.markNotificationSpecsLastRun).not.toHaveBeenCalled();
      expect(res.status).toHaveBeenCalledWith(200);
    });

    it("should resume an existing planned run and enqueue only missing batches", async () => {
      setNotificationSettings({ useCloudTasksBulk: true });
      sharedMock.getAdjustedTime.mockReturnValue(new Date("2026-01-01T00:00:00.000Z"));
      firestoreMock.getNotificationSpecsByScheduleOncePerDay.mockResolvedValue([
        makeScheduledSpec({ id: "spec-resume-plan", category: "newsletter" }),
      ]);
      bigqueryMock.countParticipantsForNotificationsBQ.mockResolvedValue(10);
      const existingRunId = "spec-resume-plan-existing-run";
      firestoreMock.getBulkNotificationRun.mockResolvedValueOnce({
        id: existingRunId,
        specId: "spec-resume-plan",
        runDateKey: "2026-04-28",
        runSequence: 1,
        status: "enqueue_failed",
        batchIds: ["default-batch-1", "default-batch-2", "default-batch-3", "microsoft-batch-1"],
      });
      firestoreMock.getBulkNotificationRunBatches.mockResolvedValueOnce([
        { id: "default-batch-1", lane: "default", batchNumber: 1, status: "enqueued", scheduleDelaySeconds: 0 },
        { id: "default-batch-2", lane: "default", batchNumber: 2, status: "planned", scheduleDelaySeconds: 2 },
        { id: "default-batch-3", lane: "default", batchNumber: 3, status: "failed", scheduleDelaySeconds: 4 },
        { id: "microsoft-batch-1", lane: "microsoft", batchNumber: 1, status: "complete", scheduleDelaySeconds: 0 },
      ]);

      const req = { method: "POST", body: { scheduleAt: "09:00" } };
      const res = createResponseMock();

      await notificationsModule.sendScheduledNotifications(req, res);

      expect(firestoreMock.saveBulkNotificationRunPlan).not.toHaveBeenCalled();
      expect(bigqueryMock.getParticipantsForNotificationsBQ).not.toHaveBeenCalled();
      expect(taskQueueMock.enqueue).toHaveBeenCalledTimes(1);
      expect(taskQueueMock.enqueue.mock.calls[0][0]).toMatchObject({
        batchId: "default-batch-2",
        lane: "default",
        specId: "spec-resume-plan",
        runSequence: 1,
      });
      expect(firestoreMock.markBulkNotificationBatchEnqueued).toHaveBeenCalledWith(expect.objectContaining({
        runId: existingRunId,
        batchId: "default-batch-2",
        queueName: "processNotificationBatchBulkDefault",
      }));
      expect(firestoreMock.markNotificationSpecsQueuedForRun).toHaveBeenCalledWith(
        ["spec-resume-plan"],
        expect.any(String),
        undefined,
        { "spec-resume-plan": 1 },
        { commitRunSequence: false },
      );
      expect(res.status).toHaveBeenCalledWith(200);
    });

    it("should mark the run enqueue_failed and not queue the spec when enqueueing fails", async () => {
      setNotificationSettings({ useCloudTasksBulk: true });
      sharedMock.getAdjustedTime.mockReturnValue(new Date("2026-01-01T00:00:00.000Z"));
      firestoreMock.getNotificationSpecsByScheduleOncePerDay.mockResolvedValue([
        makeScheduledSpec({ id: "spec-enqueue-fails", category: "newsletter" }),
      ]);
      bigqueryMock.countParticipantsForNotificationsBQ.mockResolvedValue(1);
      bigqueryMock.getParticipantsForNotificationsBQ.mockResolvedValueOnce([
        makeScheduledParticipant("tok-fail-enqueue"),
      ]);
      taskQueueMock.enqueue.mockRejectedValueOnce(new Error("Cloud Tasks unavailable"));

      const req = { method: "POST", body: { scheduleAt: "09:00" } };
      const res = createResponseMock();

      await notificationsModule.sendScheduledNotifications(req, res);

      const [{ runDoc }] = firestoreMock.saveBulkNotificationRunPlan.mock.calls[0];
      expect(firestoreMock.markBulkNotificationRunEnqueueFailed).toHaveBeenCalledWith(
        runDoc.id,
        expect.objectContaining({ message: "Cloud Tasks unavailable" }),
      );
      expect(firestoreMock.markNotificationSpecsQueuedForRun).toHaveBeenCalledWith(
        ["spec-enqueue-fails"],
        expect.any(String),
        undefined,
        { "spec-enqueue-fails": 1 },
        { commitRunSequence: false },
      );
      expect(firestoreMock.clearNotificationSpecsQueuedRun).toHaveBeenCalledWith(
        ["spec-enqueue-fails"],
        expect.any(String),
        { "spec-enqueue-fails": 1 },
      );
      expect(firestoreMock.markNotificationSpecsLastRun).not.toHaveBeenCalled();
      expect(res.status).toHaveBeenCalledWith(500);
    });

    it("should treat ALREADY_EXISTS planned task IDs as successfully enqueued", async () => {
      setNotificationSettings({ useCloudTasksBulk: true });
      sharedMock.getAdjustedTime.mockReturnValue(new Date("2026-01-01T00:00:00.000Z"));
      firestoreMock.getNotificationSpecsByScheduleOncePerDay.mockResolvedValue([
        makeScheduledSpec({ id: "spec-existing-task", category: "newsletter" }),
      ]);
      bigqueryMock.countParticipantsForNotificationsBQ.mockResolvedValue(1);
      bigqueryMock.getParticipantsForNotificationsBQ.mockResolvedValueOnce([
        makeScheduledParticipant("tok-existing-task"),
      ]);
      taskQueueMock.enqueue.mockRejectedValueOnce(Object.assign(new Error("Task already exists"), { code: 6 }));

      const req = { method: "POST", body: { scheduleAt: "09:00" } };
      const res = createResponseMock();

      await notificationsModule.sendScheduledNotifications(req, res);

      expect(res.status).toHaveBeenCalledWith(200);
      expect(firestoreMock.markBulkNotificationRunEnqueueFailed).not.toHaveBeenCalled();
      expect(firestoreMock.markBulkNotificationBatchEnqueued).toHaveBeenCalledWith(expect.objectContaining({
        batchId: "default-batch-1",
        taskId: expect.stringContaining("spec-existing-task"),
        queueName: "processNotificationBatchBulkDefault",
      }));
      expect(firestoreMock.markNotificationSpecsQueuedForRun).toHaveBeenCalledTimes(1);
      expect(firestoreMock.markNotificationSpecsQueuedForRun).toHaveBeenCalledWith(
        ["spec-existing-task"],
        expect.any(String),
        undefined,
        { "spec-existing-task": 1 },
        { commitRunSequence: false },
      );
    });

    it("should run a CI-safe noop workflow from scheduler plan through both lane handlers", async () => {
      setNotificationSettings({
        useCloudTasksBulk: true,
        sendgridDeliveryModeOverride: "noop",
        bulkDefaultBatchSize: 2,
        bulkMicrosoftBatchSize: 1,
      });
      sharedMock.developmentTier = "DEV";
      process.env.GCLOUD_PROJECT = "nih-nci-dceg-connect-dev";
      process.env.GCLOUD_UNSUBSCRIBE_SECRET = "secret/unsub-key";
      sharedMock.getSecret.mockImplementation((key) =>
        Promise.resolve(key === "secret/unsub-key" ? "test-unsub-secret" : "fake-secret")
      );
      sharedMock.getAdjustedTime.mockReturnValue(new Date("2026-01-01T00:00:00.000Z"));

      const runs = new Map();
      const batches = new Map();
      firestoreMock.saveBulkNotificationRunPlan.mockImplementation(async ({ runDoc, batchDocs }) => {
        const storedRun = {
          ...runDoc,
          batchIds: batchDocs.map((batchDoc) => batchDoc.id),
          batchCount: batchDocs.length,
          status: "planned",
        };
        runs.set(runDoc.id, storedRun);
        batchDocs.forEach((batchDoc) => batches.set(`${runDoc.id}/${batchDoc.id}`, { ...batchDoc }));
        return { runId: runDoc.id, batchCount: batchDocs.length };
      });
      firestoreMock.markBulkNotificationBatchEnqueued.mockImplementation(async ({ runId, batchId, taskId, queueName }) => {
        Object.assign(batches.get(`${runId}/${batchId}`), { status: "enqueued", taskId, queueName });
        return true;
      });
      firestoreMock.markBulkNotificationRunQueued.mockImplementation(async (runId) => {
        Object.assign(runs.get(runId), { status: "queued" });
        return true;
      });
      firestoreMock.getBulkNotificationBatch.mockImplementation(async (runId, batchId) => ({
        run: runs.get(runId),
        batch: batches.get(`${runId}/${batchId}`),
      }));
      firestoreMock.markBulkNotificationBatchRunning.mockImplementation(async ({ runId, batchId, taskAttemptOwner }) => {
        Object.assign(batches.get(`${runId}/${batchId}`), { status: "running" });
        if (taskAttemptOwner) Object.assign(batches.get(`${runId}/${batchId}`), { taskAttemptOwner });
        Object.assign(runs.get(runId), { status: "running" });
        return true;
      });
      firestoreMock.markBulkNotificationBatchComplete.mockImplementation(async ({ runId, batchId, counts, unsuccessful }) => {
        Object.assign(batches.get(`${runId}/${batchId}`), { status: "complete", counts, unsuccessful });
        return true;
      });
      firestoreMock.finalizeBulkNotificationRunIfTerminal.mockImplementation(async (runId) => {
        const run = runs.get(runId);
        const runBatches = [...batches.values()].filter((batch) => batch.runId === runId);
        if (runBatches.length === run.batchCount && runBatches.every((batch) => batch.status === "complete")) {
          Object.assign(run, { status: "complete" });
          return { finalized: true, status: "complete" };
        }
        return { finalized: false };
      });

      const spec = makeScheduledSpec({ id: "spec-noop-workflow", category: "newsletter" });
      firestoreMock.getNotificationSpecsByScheduleOncePerDay.mockResolvedValue([spec]);
      bigqueryMock.countParticipantsForNotificationsBQ.mockResolvedValue(4);
      bigqueryMock.getParticipantsForNotificationsBQ.mockResolvedValueOnce([
        makeScheduledParticipant("valid-default"),
        { ...makeScheduledParticipant("filtered"), d_335767902: "noreply@nih.gov" },
        { ...makeScheduledParticipant("suppressed"), d_335767902: "suppressed@test.gov" },
        { ...makeScheduledParticipant("valid-ms"), d_335767902: "valid-ms@outlook.com" },
      ]);
      firestoreMock.getEmailSuppressions.mockResolvedValue(new Set(["suppressed@test.gov"]));

      const res = createResponseMock();
      await notificationsModule.sendScheduledNotifications({ method: "POST", body: { scheduleAt: "09:00" } }, res);

      expect(res.status).toHaveBeenCalledWith(200);
      expect(taskQueueMock.enqueue).toHaveBeenCalledTimes(3);
      const taskPayloads = taskQueueMock.enqueue.mock.calls.map(([payload]) => payload);
      expect(taskPayloads.map((payload) => `${payload.lane}:${payload.batchId}`)).toEqual([
        "default:default-batch-1",
        "default:default-batch-2",
        "microsoft:microsoft-batch-1",
      ]);

      for (const payload of taskPayloads) {
        const handler = payload.lane === "microsoft"
          ? notificationsModule.processNotificationBatchBulkMicrosoft
          : notificationsModule.processNotificationBatchBulkDefault;
        await handler({ data: payload });
      }

      expect(sgMailMock.send).not.toHaveBeenCalled();
      expect(firestoreMock.reserveNotificationBatch).toHaveBeenCalledTimes(2);
      expect(firestoreMock.markNotificationBatchAccepted).toHaveBeenCalledTimes(2);
      const storedRun = runs.get(taskPayloads[0].runId);
      expect(storedRun.status).toBe("complete");
      const completedBatches = [...batches.values()].sort((a, b) => a.id.localeCompare(b.id));
      expect(completedBatches).toEqual(expect.arrayContaining([
        expect.objectContaining({
          id: "default-batch-1",
          status: "complete",
          counts: expect.objectContaining({ planned: 2, sent: 1, filtered: 1 }),
        }),
        expect.objectContaining({
          id: "default-batch-2",
          status: "complete",
          counts: expect.objectContaining({ planned: 1, sent: 0, suppressed: 1 }),
        }),
        expect.objectContaining({
          id: "microsoft-batch-1",
          status: "complete",
          counts: expect.objectContaining({ planned: 1, sent: 1 }),
        }),
      ]));
    });

    it("should only mark successful inline specs when another spec fails", async () => {
      sharedMock.developmentTier = "PROD";
      sharedMock.getAdjustedTime.mockReturnValue(new Date("2026-01-01T00:00:00.000Z"));
      firestoreMock.getNotificationSpecsByScheduleOncePerDay.mockResolvedValue([
        makeScheduledSpec({ id: "spec-inline-ok" }),
        makeScheduledSpec({ id: "spec-inline-fail" }),
      ]);
      bigqueryMock.countParticipantsForNotificationsBQ.mockResolvedValue(0);
      bigqueryMock.getParticipantsForNotificationsBQ
        .mockResolvedValueOnce([makeScheduledParticipant("tok-ok")])
        .mockRejectedValueOnce(new Error("BigQuery page fetch failed"));

      const req = { method: "POST", body: { scheduleAt: "09:00" } };
      const res = createResponseMock();

      await notificationsModule.sendScheduledNotifications(req, res);

      expect(res.status).toHaveBeenCalledWith(500);
      expect(firestoreMock.markNotificationSpecsLastRun).toHaveBeenCalledWith(["spec-inline-ok"]);
      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
    });
  });

  describe("handleNotificationSpec", () => {
    const conceptIds = require("../../utils/fieldToConceptIdMapping");

    const makeNotificationSpec = (overrides = {}) => ({
      id: "spec-1",
      category: "reminder",
      attempt: "1st",
      primaryField: "d_821247024",
      time: { start: { day: 0, hour: 1, minute: 0 }, stop: { day: 0, hour: 0, minute: 0 } },
      notificationType: ["email"],
      emailField: "d_335767902",
      phoneField: "",
      firstNameField: "d_153098809",
      preferredNameField: "",
      email: {
        english: { subject: "Test Subject", body: "<p>Hello {{firstName}}</p>" },
      },
      sms: {},
      ...overrides,
    });

    const makeParticipant = (overrides = {}) => ({
      Connect_ID: "C100001",
      token: "tok-abc",
      state: { uid: "uid-123" },
      [conceptIds.preferredLanguage]: 0, // not mapped; falls back to "english"
      "d_335767902": "user@test.gov",
      "d_153098809": "Jane",
      ...overrides,
    });

    const setupHandleNotificationSpecMocks = async () => {
      // getAdjustedTime must return Date objects so .toISOString() works in getTimeParams
      sharedMock.getAdjustedTime.mockReturnValue(new Date("2026-01-01T00:00:00.000Z"));
      process.env.GCLOUD_PROJECT = "nih-nci-dceg-connect-prod-6d04";
      sharedMock.developmentTier = "PROD";
      // Resolve unsubscribe secret for bulk mail tests
      process.env.GCLOUD_UNSUBSCRIBE_SECRET = "secret/unsub-key";
      sharedMock.getSecret.mockImplementation((key) => {
        if (key === "secret/unsub-key") return Promise.resolve("test-unsub-secret-key-12345");
        return Promise.resolve("fake-secret");
      });
      setNotificationSettings({});
      await notificationsModule.resolveUnsubscribeSecret();
    };

    const runHandleNotificationSpec = async (spec, participants) => {
      bigqueryMock.getParticipantsForNotificationsBQ
        .mockResolvedValueOnce(participants)
        .mockResolvedValueOnce([]); // second call returns empty to end pagination

      return notificationsModule.handleNotificationSpec(spec);
    };

    beforeEach(async () => {
      await setupHandleNotificationSpecMocks();
    });

    // Plaintext
    it("should include plaintext alternative in email batch", async () => {
      const spec = makeNotificationSpec();
      const participants = [makeParticipant()];
      await runHandleNotificationSpec(spec, participants);

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      const emailBatch = sgMailMock.send.mock.calls[0][0];
      expect(emailBatch.text).toBeDefined();
      expect(emailBatch.text).toBeTypeOf("string");
    });

    // Mail-stream classifier
    it("should classify newsletter category as bulk", async () => {
      const spec = makeNotificationSpec({ category: "newsletter" });
      const participants = [makeParticipant()];
      await runHandleNotificationSpec(spec, participants);

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      const emailBatch = sgMailMock.send.mock.calls[0][0];
      const customArgs = emailBatch.personalizations[0].custom_args;
      expect(customArgs.mail_stream).toBe("bulk");
    });

    it("should classify eNewsletter category as bulk", async () => {
      const spec = makeNotificationSpec({ category: "eNewsletter" });
      const participants = [makeParticipant()];
      await runHandleNotificationSpec(spec, participants);

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      const customArgs = sgMailMock.send.mock.calls[0][0].personalizations[0].custom_args;
      expect(customArgs.mail_stream).toBe("bulk");
    });

    it("should use configured bulkMailCategories for bulk classification", async () => {
      setNotificationSettings({ bulkMailCategories: ["newsletter"] });
      const spec = makeNotificationSpec({ category: "newsletter" });
      const participants = [makeParticipant()];
      await runHandleNotificationSpec(spec, participants);

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      const customArgs = sgMailMock.send.mock.calls[0][0].personalizations[0].custom_args;
      expect(customArgs.mail_stream).toBe("bulk");
    });

    it("should classify non-newsletter with fewer than 5000 recipients as transactional", async () => {
      const spec = makeNotificationSpec({ category: "reminder" });
      const participants = [makeParticipant()];
      await runHandleNotificationSpec(spec, participants);

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      const customArgs = sgMailMock.send.mock.calls[0][0].personalizations[0].custom_args;
      expect(customArgs.mail_stream).toBe("transactional");
    });

    it("should upgrade to bulk when total recipients reach the configured bulkThreshold", async () => {
      setNotificationSettings({ bulkThreshold: 3 });
      const spec = makeNotificationSpec({ category: "reminder" });
      const participants = [
        makeParticipant({ Connect_ID: "C1", token: "t1", "d_335767902": "a@test.com" }),
        makeParticipant({ Connect_ID: "C2", token: "t2", "d_335767902": "b@test.com" }),
        makeParticipant({ Connect_ID: "C3", token: "t3", "d_335767902": "c@test.com" }),
      ];
      await runHandleNotificationSpec(spec, participants);

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      const customArgs = sgMailMock.send.mock.calls[0][0].personalizations[0].custom_args;
      expect(customArgs.mail_stream).toBe("bulk");
    });

    it("should use configured notificationBatchLimit for participant paging", async () => {
      setNotificationSettings({ notificationBatchLimit: 2 });
      const spec = makeNotificationSpec({ category: "newsletter" });
      const participantsPage1 = [
        makeParticipant({ Connect_ID: "C1", token: "t1", "d_335767902": "a@test.com" }),
        makeParticipant({ Connect_ID: "C2", token: "t2", "d_335767902": "b@test.com" }),
      ];
      const participantsPage2 = [
        makeParticipant({ Connect_ID: "C3", token: "t3", "d_335767902": "c@test.com" }),
      ];
      bigqueryMock.getParticipantsForNotificationsBQ
        .mockResolvedValueOnce(participantsPage1)
        .mockResolvedValueOnce(participantsPage2)
        .mockResolvedValueOnce([]);

      const summary = await notificationsModule.handleNotificationSpec(spec);

      expect(bigqueryMock.getParticipantsForNotificationsBQ.mock.calls[0][0].limit).toBe(2);
      expect(bigqueryMock.getParticipantsForNotificationsBQ.mock.calls[1][0].limit).toBe(2);
      expect(summary.emailsSent).toBe(3);
    });

    it("should add mail_stream to custom_args", async () => {
      const spec = makeNotificationSpec();
      const participants = [makeParticipant()];
      await runHandleNotificationSpec(spec, participants);

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      const customArgs = sgMailMock.send.mock.calls[0][0].personalizations[0].custom_args;
      expect(customArgs).toHaveProperty("mail_stream");
      // Also verify existing fields are preserved
      expect(customArgs).toHaveProperty("connect_id");
      expect(customArgs).toHaveProperty("token");
      expect(customArgs).toHaveProperty("notification_id");
      expect(customArgs).toHaveProperty("gcloud_project");
    });

    // Pre-send suppression
    it("should skip suppressed emails in personalization array", async () => {
      const spec = makeNotificationSpec();
      const participants = [
        makeParticipant({ Connect_ID: "C1", token: "t1", "d_335767902": "ok@test.com" }),
        makeParticipant({ Connect_ID: "C2", token: "t2", "d_335767902": "suppressed@test.com" }),
      ];
      firestoreMock.getEmailSuppressions.mockResolvedValue(new Set(["suppressed@test.com"]));
      bigqueryMock.getParticipantsForNotificationsBQ
        .mockResolvedValueOnce(participants)
        .mockResolvedValueOnce([]);
      const smsSendFn = vi.fn(async (smsRecord) => ({
        smsRecord: { ...smsRecord, messageSid: "SM_test" },
        isSuccess: true,
        isRateLimit: false,
      }));
      const smsSender = new SmsBatchSender({
        sendFn: smsSendFn,
        saveSuccessFn: async (records) => {
          await firestoreMock.markNotificationBatchAccepted(records, records[0]._providerAttemptOwner);
        },
        delayFn: sharedMock.delay,
      });

      await notificationsModule.handleNotificationSpec(spec, { smsBatchSender: smsSender });

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      const personalizations = sgMailMock.send.mock.calls[0][0].personalizations;
      expect(personalizations).toHaveLength(1);
      expect(personalizations[0].to).toBe("ok@test.com");
    });

    it("should skip filtered no-reply or malformed recipient emails before send", async () => {
      const spec = makeNotificationSpec();
      const participants = [
        makeParticipant({ Connect_ID: "C1", token: "t1", "d_335767902": "ok@test.com" }),
        makeParticipant({ Connect_ID: "C2", token: "t2", "d_335767902": "noreply@nih.gov" }),
      ];

      await runHandleNotificationSpec(spec, participants);

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      expect(firestoreMock.getEmailSuppressions).toHaveBeenCalledWith(["ok@test.com"], "transactional");
      const personalizations = sgMailMock.send.mock.calls[0][0].personalizations;
      expect(personalizations).toHaveLength(1);
      expect(personalizations[0].to).toBe("ok@test.com");
    });

    it("should still send SMS for participants with suppressed email", async () => {
      const conceptIds = require("../../utils/fieldToConceptIdMapping");
      const spec = makeNotificationSpec({
        notificationType: ["email", "sms"],
        phoneField: "d_388711124",
        sms: { english: { body: "Hello {{firstName}}" } },
      });
      const participants = [
        makeParticipant({
          Connect_ID: "C1",
          token: "t1",
          "d_335767902": "suppressed@test.com",
          "d_388711124": "5551234567",
          [conceptIds.canWeText]: conceptIds.yes,
        }),
      ];
      firestoreMock.getEmailSuppressions.mockResolvedValue(new Set(["suppressed@test.com"]));
      bigqueryMock.getParticipantsForNotificationsBQ
        .mockResolvedValueOnce(participants)
        .mockResolvedValueOnce([]);
      const smsSendFn = vi.fn(async (smsRecord) => ({
        smsRecord: { ...smsRecord, messageSid: "SM_test" },
        isSuccess: true,
        isRateLimit: false,
      }));
      const smsSender = new SmsBatchSender({
        sendFn: smsSendFn,
        saveSuccessFn: async (records) => {
          await firestoreMock.markNotificationBatchAccepted(records, records[0]._providerAttemptOwner);
        },
        delayFn: sharedMock.delay,
      });

      await notificationsModule.handleNotificationSpec(spec, { smsBatchSender: smsSender });

      // Email should not be sent (suppressed)
      expect(sgMailMock.send).not.toHaveBeenCalled();
      expect(firestoreMock.getEmailSuppressions).toHaveBeenCalled();
      expect(firestoreMock.reserveNotificationBatch).toHaveBeenCalledTimes(1);
      expect(firestoreMock.reserveNotificationBatch.mock.calls[0][0][0]).toEqual(expect.objectContaining({
        notificationType: "sms",
        token: "t1",
      }));
      expect(firestoreMock.markNotificationBatchProviderSendStarted).toHaveBeenCalledTimes(1);
      expect(firestoreMock.markNotificationBatchAccepted).toHaveBeenCalledTimes(1);
      expect(smsSendFn).toHaveBeenCalledTimes(1);
    });

    it("should log skippedBySuppression count", async () => {
      const consoleSpy = vi.spyOn(console, "log");
      const spec = makeNotificationSpec();
      const participants = [
        makeParticipant({ Connect_ID: "C1", token: "t1", "d_335767902": "suppressed@test.com" }),
      ];
      firestoreMock.getEmailSuppressions.mockResolvedValue(new Set(["suppressed@test.com"]));

      await runHandleNotificationSpec(spec, participants);

      const logCalls = consoleSpy.mock.calls.map(c => c[0]);
      const suppressionLog = logCalls.find(msg => typeof msg === "string" && msg.includes("suppressed"));
      expect(suppressionLog).toBeDefined();
    });

    // List-Unsubscribe headers
    it("should default List-Unsubscribe to the webhook handler for bulk mail", async () => {
      process.env.GCLOUD_PROJECT = "nih-nci-dceg-connect-dev";
      sharedMock.developmentTier = "STAGE";
      setNotificationSettings({ nonProdEmailAllowlist: ["user@test.gov"] });
      const spec = makeNotificationSpec({ category: "newsletter" });
      const participants = [makeParticipant()];
      await runHandleNotificationSpec(spec, participants);

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      const emailBatch = sgMailMock.send.mock.calls[0][0];
      // Headers are per-personalization (not batch-level) so each recipient gets their own unsubscribe URL
      expect(emailBatch.headers).toBeUndefined();
      const personalization = emailBatch.personalizations[0];
      expect(personalization.headers).toBeDefined();
      expect(personalization.headers["List-Unsubscribe"]).toBeDefined();
      const unsubscribeUrl = new URL(personalization.headers["List-Unsubscribe"].slice(1, -1));
      expect(unsubscribeUrl.origin + unsubscribeUrl.pathname)
        .toBe("https://us-central1-nih-nci-dceg-connect-dev.cloudfunctions.net/webhook");
      expect(unsubscribeUrl.searchParams.get("api")).toBe("email-unsubscribe");
      expect(unsubscribeUrl.searchParams.get("email")).toBe("user@test.gov");
      expect(unsubscribeUrl.searchParams.get("token")).toBe("tok-abc");
      expect(unsubscribeUrl.searchParams.get("sig")).toBeTruthy();
      expect(personalization.headers["List-Unsubscribe"]).not.toContain("email-unsubscribe?email=");
      expect(personalization.headers["List-Unsubscribe-Post"]).toBe("List-Unsubscribe=One-Click");
    });

    it("should respect SG_UNSUBSCRIBE_URL when provided", async () => {
      process.env.GCLOUD_PROJECT = "nih-nci-dceg-connect-dev";
      sharedMock.developmentTier = "STAGE";
      setNotificationSettings({ nonProdEmailAllowlist: ["user@test.gov"] });
      process.env.SG_UNSUBSCRIBE_URL = "https://myconnect.cancer.gov/unsubscribe";
      const spec = makeNotificationSpec({ category: "newsletter" });
      const participants = [makeParticipant()];
      await runHandleNotificationSpec(spec, participants);

      const emailBatch = sgMailMock.send.mock.calls[0][0];
      const personalization = emailBatch.personalizations[0];
      expect(personalization.headers["List-Unsubscribe"])
        .toContain("https://myconnect.cancer.gov/unsubscribe");
    });

    it("should NOT add unsubscribe headers for transactional mail", async () => {
      const spec = makeNotificationSpec({ category: "reminder" });
      const participants = [makeParticipant()];
      await runHandleNotificationSpec(spec, participants);

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      const emailBatch = sgMailMock.send.mock.calls[0][0];
      expect(emailBatch.headers).toBeUndefined();
      expect(emailBatch.asm).toBeUndefined();
      expect(emailBatch.tracking_settings).toBeUndefined();
      // Per-personalization headers should also be absent for transactional
      const personalization = emailBatch.personalizations[0];
      expect(personalization.headers).toBeUndefined();
    });

    it("should include subscription_tracking only for bulk mail when ASM is not configured", async () => {
      const spec = makeNotificationSpec({ category: "newsletter" });
      const participants = [makeParticipant()];
      await runHandleNotificationSpec(spec, participants);

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      const emailBatch = sgMailMock.send.mock.calls[0][0];
      expect(emailBatch.tracking_settings).toBeDefined();
      expect(emailBatch.tracking_settings.subscription_tracking.enable).toBe(true);
    });

    it("should use SendGrid ASM group unsubscribe footer for bulk mail when configured", async () => {
      setNotificationSettings({
        sendgridBulkAsmGroupId: "22391",
        sendgridBulkAsmGroupsToDisplay: "22391,99999",
      });
      const spec = makeNotificationSpec({ category: "newsletter" });
      const participants = [makeParticipant()];
      await runHandleNotificationSpec(spec, participants);

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      const emailBatch = sgMailMock.send.mock.calls[0][0];
      expect(emailBatch.asm).toEqual({
        group_id: 22391,
        groups_to_display: [22391, 99999],
      });
      expect(emailBatch.tracking_settings).toEqual({
        subscription_tracking: {
          enable: false,
        },
      });
      expect(emailBatch.html).toContain('<%asm_group_unsubscribe_raw_url%>');
      expect(emailBatch.html).not.toContain("<% click here %>");
      expect(emailBatch.personalizations[0].headers["List-Unsubscribe"])
        .toContain("api=email-unsubscribe");
      expect(emailBatch.personalizations[0].headers["List-Unsubscribe-Post"])
        .toBe("List-Unsubscribe=One-Click");
    });

    it("should fall back to subscription tracking when ASM group config is invalid", async () => {
      setNotificationSettings({
        sendgridBulkAsmGroupId: "not-a-number",
        sendgridBulkAsmGroupsToDisplay: [22391],
      });
      const spec = makeNotificationSpec({ category: "newsletter" });
      const participants = [makeParticipant()];
      await runHandleNotificationSpec(spec, participants);

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      const emailBatch = sgMailMock.send.mock.calls[0][0];
      expect(emailBatch.asm).toBeUndefined();
      expect(emailBatch.tracking_settings.subscription_tracking.enable).toBe(true);
    });

    // Retry logic
    it("should retry on 429 with exponential backoff", async () => {
      const error429 = new Error("Too Many Requests");
      error429.code = 429;
      sgMailMock.send
        .mockRejectedValueOnce(error429)
        .mockRejectedValueOnce(error429)
        .mockResolvedValueOnce([{ statusCode: 202 }]);

      const delayCallsBefore = sharedMock.delay.mock.calls.length;
      const spec = makeNotificationSpec();
      await runHandleNotificationSpec(spec, [makeParticipant()]);

      expect(sgMailMock.send).toHaveBeenCalledTimes(3);
      const delayCallsAfter = sharedMock.delay.mock.calls.length;
      expect(delayCallsAfter - delayCallsBefore).toBeGreaterThanOrEqual(2);
      expect(firestoreMock.markNotificationBatchAccepted).toHaveBeenCalledTimes(1);
    });

    it("should not retry on 5xx because provider acceptance is ambiguous", async () => {
      const error500 = new Error("Internal Server Error");
      error500.code = 500;
      sgMailMock.send.mockRejectedValueOnce(error500);

      const spec = makeNotificationSpec();
      await expect(runHandleNotificationSpec(spec, [makeParticipant()])).rejects.toThrow(
        "Failed sending emails for spec-1(reminder, 1st).",
      );

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      expect(firestoreMock.markNotificationBatchProviderAcceptanceUnknown).toHaveBeenCalledTimes(1);
      expect(firestoreMock.markNotificationBatchFailed).not.toHaveBeenCalled();
    });

    it("should NOT retry on 4xx (non-429) errors", async () => {
      const error400 = new Error("Bad Request");
      error400.code = 400;
      sgMailMock.send.mockRejectedValueOnce(error400);

      const spec = makeNotificationSpec();
      await expect(runHandleNotificationSpec(spec, [makeParticipant()])).rejects.toThrow(
        "Failed sending emails for spec-1(reminder, 1st).",
      );

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      expect(firestoreMock.markNotificationBatchFailed).toHaveBeenCalledTimes(1);
    });

    it("should stop retrying after the configured sendRetryMax attempts", async () => {
      setNotificationSettings({ sendRetryMax: 2 });
      const error429 = new Error("Too Many Requests");
      error429.code = 429;
      sgMailMock.send.mockRejectedValue(error429);

      const spec = makeNotificationSpec();
      await expect(runHandleNotificationSpec(spec, [makeParticipant()])).rejects.toThrow(
        "Failed sending emails for spec-1(reminder, 1st).",
      );

      // initial attempt + 2 retries = 3 total
      expect(sgMailMock.send).toHaveBeenCalledTimes(3);
      expect(firestoreMock.markNotificationBatchFailed).toHaveBeenCalledTimes(1);
    });

    it("should move ambiguous provider errors to acceptance_unknown instead of retrying", async () => {
      setNotificationSettings({ sendRetryMax: 1 });
      const error500 = new Error("Server Error");
      error500.code = 500;
      sgMailMock.send.mockRejectedValue(error500); // always fails

      const spec = makeNotificationSpec();
      const participants = [makeParticipant()];
      await expect(runHandleNotificationSpec(spec, participants)).rejects.toThrow(
        "Failed sending emails for spec-1(reminder, 1st).",
      );

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      expect(firestoreMock.markNotificationBatchProviderAcceptanceUnknown).toHaveBeenCalledTimes(1);
      expect(firestoreMock.markNotificationBatchFailed).not.toHaveBeenCalled();
    });

    it("should throw when marking accepted fails (not silently swallow)", async () => {
      sgMailMock.send.mockResolvedValueOnce([{ statusCode: 202 }]);
      firestoreMock.markNotificationBatchAccepted.mockRejectedValueOnce(new Error("Firestore write failed"));

      const spec = makeNotificationSpec();
      await expect(runHandleNotificationSpec(spec, [makeParticipant()])).rejects.toThrow("Firestore write failed");

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
    });

    it("should mark notification batch failed when provider send fails", async () => {
      sgMailMock.send.mockRejectedValue(Object.assign(new Error("SendGrid API error"), { statusCode: 400 }));

      const spec = makeNotificationSpec({ category: "newsletter" });
      const fullBatch = Array.from({ length: 1000 }, (_, i) => makeParticipant({
        Connect_ID: `C${i}`, token: `t${i}`, "d_335767902": `u${i}@test.com`,
      }));
      const secondBatch = [makeParticipant({ Connect_ID: "extra", token: "textra", "d_335767902": "extra@test.com" })];

      bigqueryMock.getParticipantsForNotificationsBQ
        .mockResolvedValueOnce(fullBatch)
        .mockResolvedValueOnce(secondBatch)
        .mockResolvedValueOnce([]);

      setupHandleNotificationSpecMocks();
      await expect(notificationsModule.handleNotificationSpec(spec)).rejects.toThrow("Failed sending emails");

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      expect(firestoreMock.markNotificationBatchFailed).toHaveBeenCalledTimes(1);
    });

    it("should reserve notification records before marking them accepted", async () => {
      const spec = makeNotificationSpec();
      await runHandleNotificationSpec(spec, [makeParticipant()]);

      expect(firestoreMock.reserveNotificationBatch).toHaveBeenCalledTimes(1);
      expect(firestoreMock.markNotificationBatchAccepted).toHaveBeenCalledTimes(1);
      expect(firestoreMock.markNotificationBatchFailed).not.toHaveBeenCalled();
      expect(firestoreMock.reserveNotificationBatch.mock.invocationCallOrder[0]).toBeLessThan(
        sgMailMock.send.mock.invocationCallOrder[0],
      );
      expect(sgMailMock.send.mock.invocationCallOrder[0]).toBeLessThan(
        firestoreMock.markNotificationBatchAccepted.mock.invocationCallOrder[0],
      );
    });

    // Bulk send distribution
    it("should apply inter-batch delay for bulk sends when more batches remain", async () => {
      setNotificationSettings({ targetRecipientsPerHour: 10000 });
      const spec = makeNotificationSpec({ category: "newsletter" });

      // Simulate a full batch (1000 participants) so hasNext=true, triggering the delay
      const fullBatch = Array.from({ length: 1000 }, (_, i) => makeParticipant({
        Connect_ID: `C${i}`, token: `t${i}`, "d_335767902": `u${i}@test.com`,
      }));
      bigqueryMock.getParticipantsForNotificationsBQ
        .mockResolvedValueOnce(fullBatch)
        .mockResolvedValueOnce([]); // second page empty

      const delayCallsBefore = sharedMock.delay.mock.calls.length;
      await notificationsModule.handleNotificationSpec(spec);

      const delayCalls = sharedMock.delay.mock.calls.slice(delayCallsBefore);
      expect(delayCalls).toContainEqual([360000]);
    });

    it("should NOT apply inter-batch delay after the last batch", async () => {
      const spec = makeNotificationSpec({ category: "newsletter" });
      const participants = [makeParticipant()]; // < 1000, so hasNext=false
      const delayCallsBefore = sharedMock.delay.mock.calls.length;
      await runHandleNotificationSpec(spec, participants);

      // No inter-batch delay since this is the last (and only) batch
      const delayCalls = sharedMock.delay.mock.calls.slice(delayCallsBefore);
      const hasBulkDelay = delayCalls.some(([ms]) => typeof ms === "number" && ms >= 1000);
      expect(hasBulkDelay).toBe(false);
    });

    it("should derive inter-batch delay from the configured targetRecipientsPerHour", async () => {
      setNotificationSettings({ targetRecipientsPerHour: 5000 });

      const spec = makeNotificationSpec({ category: "newsletter" });
      const fullBatch = Array.from({ length: 1000 }, (_, i) => makeParticipant({
        Connect_ID: `C${i}`, token: `t${i}`, "d_335767902": `u${i}@test.com`,
      }));
      bigqueryMock.getParticipantsForNotificationsBQ
        .mockResolvedValueOnce(fullBatch)
        .mockResolvedValueOnce([]);

      const delayCallsBefore = sharedMock.delay.mock.calls.length;
      await notificationsModule.handleNotificationSpec(spec);

      const delayCalls = sharedMock.delay.mock.calls.slice(delayCallsBefore);
      expect(delayCalls).toContainEqual([720000]);
    });

    it("should pace Microsoft-family recipients with the Microsoft-specific bulk target", async () => {
      setNotificationSettings({ targetRecipientsPerHour: 5000, targetRecipientsPerHourMicrosoft: 1500 });

      const spec = makeNotificationSpec({ category: "newsletter" });
      const fullBatch = Array.from({ length: 1000 }, (_, i) => makeParticipant({
        Connect_ID: `C${i}`,
        token: `t${i}`,
        "d_335767902": `u${i}@outlook.com`,
      }));
      bigqueryMock.getParticipantsForNotificationsBQ
        .mockResolvedValueOnce(fullBatch)
        .mockResolvedValueOnce([]);

      const delayCallsBefore = sharedMock.delay.mock.calls.length;
      const summary = await notificationsModule.handleNotificationSpec(spec);

      const delayCalls = sharedMock.delay.mock.calls.slice(delayCallsBefore);
      expect(delayCalls).toContainEqual([2400000]);
      expect(summary.bulkLaneSentCounts.microsoft).toBe(1000);
      expect(summary.bulkLaneSentCounts.default).toBe(0);
      expect(summary.recommendedNextBulkDelayMs).toBe(2400000);
    });

    it("should pace mixed Microsoft-family and non-Microsoft recipients using the slower lane", async () => {
      setNotificationSettings({ targetRecipientsPerHour: 5000, targetRecipientsPerHourMicrosoft: 1500 });

      const spec = makeNotificationSpec({ category: "newsletter" });
      const mixedBatch = Array.from({ length: 1000 }, (_, i) => makeParticipant({
        Connect_ID: `C${i}`,
        token: `t${i}`,
        "d_335767902": i < 500 ? `u${i}@outlook.com` : `u${i}@test.gov`,
      }));
      bigqueryMock.getParticipantsForNotificationsBQ
        .mockResolvedValueOnce(mixedBatch)
        .mockResolvedValueOnce([]);

      const delayCallsBefore = sharedMock.delay.mock.calls.length;
      const summary = await notificationsModule.handleNotificationSpec(spec);

      const delayCalls = sharedMock.delay.mock.calls.slice(delayCallsBefore);
      expect(delayCalls).toContainEqual([1200000]);
      expect(summary.bulkLaneSentCounts).toEqual({ default: 500, microsoft: 500 });
      expect(summary.recommendedNextBulkDelayMs).toBe(1200000);
    });

    it("should use configured microsoftBulkDomains for bulk lane classification", async () => {
      setNotificationSettings({ microsoftBulkDomains: ["test.gov"] });

      const spec = makeNotificationSpec({ category: "newsletter" });
      const participants = [makeParticipant({ "d_335767902": "user@test.gov" })];
      const summary = await runHandleNotificationSpec(spec, participants);

      expect(summary.bulkLaneSentCounts.microsoft).toBe(1);
      expect(summary.bulkLaneSentCounts.default).toBe(0);
    });

    // Per-spec send summaries
    it("should return a structured summary object", async () => {
      const spec = makeNotificationSpec();
      const participants = [makeParticipant()];
      const summary = await runHandleNotificationSpec(spec, participants);

      expect(summary).toBeDefined();
      expect(summary.specId).toBe("spec-1");
      expect(summary.mailStream).toBe("transactional");
      expect(summary.totalRecipients).toBe(1);
      expect(summary.emailsSent).toBeTypeOf("number");
      expect(summary.bulkLaneSentCounts).toEqual({ default: 1, microsoft: 0 });
      expect(summary.recommendedNextBulkDelayMs).toBe(0);
      expect(summary.smsSent).toBeTypeOf("number");
      expect(summary.suppressed).toBe(0);
    });

    it("should include suppressed count in summary", async () => {
      const spec = makeNotificationSpec();
      const participants = [
        makeParticipant({ Connect_ID: "C1", token: "t1", "d_335767902": "ok@test.com" }),
        makeParticipant({ Connect_ID: "C2", token: "t2", "d_335767902": "suppressed@test.com" }),
      ];
      firestoreMock.getEmailSuppressions.mockResolvedValue(new Set(["suppressed@test.com"]));

      const summary = await runHandleNotificationSpec(spec, participants);
      expect(summary.suppressed).toBe(1);
      expect(summary.emailsSent).toBe(1);
    });

    it("should report zero emails when all suppressed", async () => {
      const spec = makeNotificationSpec();
      const participants = [
        makeParticipant({ Connect_ID: "C1", token: "t1", "d_335767902": "supp@test.com" }),
      ];
      firestoreMock.getEmailSuppressions.mockResolvedValue(new Set(["supp@test.com"]));

      const summary = await runHandleNotificationSpec(spec, participants);
      expect(summary.emailsSent).toBe(0);
      expect(summary.suppressed).toBe(1);
    });
  });

  describe("sendInstantNotification", () => {
    const setupInstantNotificationMocks = () => {
      sharedMock.developmentTier = "PROD";
      sharedMock.getSecret.mockResolvedValue("fake-api-key");
      process.env.GCLOUD_SENDGRID_SECRET = "secret/sendgrid-key";
      firestoreMock.getNotificationSpecByCategoryAndAttempt.mockResolvedValue({
        id: "instant-spec-1",
        email: {
          english: { subject: "Instant Subject", body: "<p>Hello {{firstName}}</p>" },
        },
      });
      firestoreMock.checkIsNotificationSent.mockResolvedValue(false);
    };

    const makeInstantRequestData = (overrides = {}) => ({
      category: "verificationReminder",
      attempt: "1st",
      connectId: "C100001",
      token: "tok-abc",
      uid: "uid-123",
      email: "user@test.gov",
      preferredLanguage: "english",
      substitutions: { firstName: "Jane", loginDetails: "" },
      ...overrides,
    });

    // Plaintext
    it("should include plaintext alternative", async () => {
      setupInstantNotificationMocks();
      await notificationsModule.sendInstantNotification(makeInstantRequestData());

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      const emailData = sgMailMock.send.mock.calls[0][0];
      expect(emailData.text).toBeDefined();
      expect(emailData.text).toBeTypeOf("string");
    });

    // mail_stream always transactional for instant
    it("should include mail_stream 'transactional' in custom_args", async () => {
      setupInstantNotificationMocks();
      await notificationsModule.sendInstantNotification(makeInstantRequestData());

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      const customArgs = sgMailMock.send.mock.calls[0][0].personalizations[0].custom_args;
      expect(customArgs.mail_stream).toBe("transactional");
    });

    // Suppression
    it("should return early for suppressed email", async () => {
      setupInstantNotificationMocks();
      firestoreMock.isEmailSuppressed.mockResolvedValue(true);

      await notificationsModule.sendInstantNotification(makeInstantRequestData());

      // Should NOT send email when suppressed
      expect(sgMailMock.send).not.toHaveBeenCalled();
      expect(firestoreMock.reserveNotificationBatch).not.toHaveBeenCalled();
    });

    it("should return early for filtered internal or malformed recipient email", async () => {
      setupInstantNotificationMocks();

      await notificationsModule.sendInstantNotification(
        makeInstantRequestData({ email: "noreply@nih.gov" })
      );

      expect(sgMailMock.send).not.toHaveBeenCalled();
      expect(firestoreMock.isEmailSuppressed).not.toHaveBeenCalled();
      expect(firestoreMock.reserveNotificationBatch).not.toHaveBeenCalled();
    });

    it("should proceed normally for non-suppressed email", async () => {
      setupInstantNotificationMocks();
      firestoreMock.isEmailSuppressed.mockResolvedValue(false);

      await notificationsModule.sendInstantNotification(makeInstantRequestData());

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      expect(firestoreMock.reserveNotificationBatch).toHaveBeenCalledTimes(1);
      expect(firestoreMock.markNotificationBatchAccepted).toHaveBeenCalledTimes(1);
      expect(firestoreMock.markNotificationBatchFailed).not.toHaveBeenCalled();
    });

    it("should use configured notificationReservationMs when reserving instant notifications", async () => {
      setupInstantNotificationMocks();
      setNotificationSettings({ notificationReservationMs: 12345 });

      await notificationsModule.sendInstantNotification(makeInstantRequestData());

      expect(firestoreMock.reserveNotificationBatch).toHaveBeenCalledTimes(1);
      expect(firestoreMock.reserveNotificationBatch.mock.calls[0][2]).toBe(12345);
    });

    it("should skip sending instant notifications when no records are reserved", async () => {
      setupInstantNotificationMocks();
      firestoreMock.reserveNotificationBatch.mockResolvedValueOnce({
        recordsToSend: [],
      });

      await notificationsModule.sendInstantNotification(makeInstantRequestData());

      expect(sgMailMock.send).not.toHaveBeenCalled();
      expect(firestoreMock.markNotificationBatchAccepted).not.toHaveBeenCalled();
      expect(firestoreMock.markNotificationBatchFailed).not.toHaveBeenCalled();
    });

    it("should reserve instant notification records before marking them accepted", async () => {
      setupInstantNotificationMocks();

      await notificationsModule.sendInstantNotification(makeInstantRequestData());

      expect(firestoreMock.reserveNotificationBatch).toHaveBeenCalledTimes(1);
      expect(firestoreMock.markNotificationBatchAccepted).toHaveBeenCalledTimes(1);
      expect(firestoreMock.markNotificationBatchFailed).not.toHaveBeenCalled();
      expect(firestoreMock.reserveNotificationBatch.mock.invocationCallOrder[0]).toBeLessThan(
        sgMailMock.send.mock.invocationCallOrder[0],
      );
      expect(sgMailMock.send.mock.invocationCallOrder[0]).toBeLessThan(
        firestoreMock.markNotificationBatchAccepted.mock.invocationCallOrder[0],
      );
    });

    // No tracking_settings for transactional
    it("should not include tracking_settings (always transactional)", async () => {
      setupInstantNotificationMocks();
      await notificationsModule.sendInstantNotification(makeInstantRequestData());

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      const emailData = sgMailMock.send.mock.calls[0][0];
      expect(emailData.tracking_settings).toBeUndefined();
      expect(emailData.asm).toBeUndefined();
      expect(emailData.headers).toBeUndefined();
    });
  });

  // Planned bulk Cloud Task handlers
  describe("planned bulk notification batch handlers", () => {
    const makePlannedBulkSpec = (overrides = {}) => ({
      id: "planned-bulk-spec",
      category: "newsletter",
      attempt: "1st",
      primaryField: "d_821247024",
      time: { start: { day: 0, hour: 1, minute: 0 }, stop: { day: 0, hour: 0, minute: 0 } },
      notificationType: ["email"],
      emailField: "d_335767902",
      firstNameField: "d_153098809",
      email: {
        english: { subject: "Newsletter", body: "<p>Hello {{firstName}}</p>" },
      },
      sms: {},
      conditions: JSON.stringify([["d_821247024", "equals", "197316935"]]),
      ...overrides,
    });

    const makePlannedRecipient = (token, email = `${token}@test.gov`) => ({
      Connect_ID: `C-${token}`,
      token,
      state: { uid: `uid-${token}` },
      d_335767902: email,
      d_153098809: "Alex",
      353358909: 0,
    });

    const mockPlannedBatch = ({
      spec = makePlannedBulkSpec(),
      recipients = [],
      lane = "default",
      runId = "planned-bulk-spec-2026-04-01-run-1",
      batchId = `${lane}-batch-1`,
      batchStatus,
    } = {}) => {
      firestoreMock.getBulkNotificationBatch.mockResolvedValue({
        run: {
          id: runId,
          specId: spec.id,
          runDateKey: "2026-04-01",
          runSequence: 1,
          notificationSpec: spec,
          timeParams: {
            startTimeStr: "2026-01-01T00:00:00.000Z",
            stopTimeStr: "2026-01-01T00:00:00.000Z",
            timeField: "d_821247024",
          },
          conditions: [["d_821247024", "equals", "197316935"]],
        },
        batch: {
          id: batchId,
          lane,
          batchNumber: 1,
          recipientCount: recipients.length,
          recipients,
          unsuccessful: {},
          ...(batchStatus ? { status: batchStatus } : {}),
        },
      });
    };

    it("should reject planned bulk payloads sent to the wrong lane handler", async () => {
      await expect(notificationsModule.processNotificationBatchBulkDefault({
        data: {
          runId: "run-1",
          batchId: "microsoft-batch-1",
          lane: "microsoft",
        },
      })).rejects.toThrow("expected lane default, received microsoft");

      expect(firestoreMock.getBulkNotificationBatch).not.toHaveBeenCalled();
    });

    it("should reject planned bulk tasks whose payload specId does not match the run spec", async () => {
      const spec = makePlannedBulkSpec({ id: "stored-spec" });
      mockPlannedBatch({ spec, recipients: [makePlannedRecipient("tok-mismatch")] });

      await expect(notificationsModule.processNotificationBatchBulkDefault({
        data: {
          runId: "stored-spec-2026-04-01-run-1",
          batchId: "default-batch-1",
          lane: "default",
          specId: "payload-spec",
          runDateKey: "2026-04-01",
          runSequence: 1,
        },
      })).rejects.toThrow("task spec payload-spec does not match run spec stored-spec");

      expect(sgMailMock.send).not.toHaveBeenCalled();
      expect(firestoreMock.markBulkNotificationBatchRunning).not.toHaveBeenCalled();
    });

    it("should skip already-complete planned bulk batches without resending", async () => {
      const spec = makePlannedBulkSpec();
      mockPlannedBatch({
        spec,
        recipients: [makePlannedRecipient("complete")],
        batchStatus: "complete",
      });

      const result = await notificationsModule.processNotificationBatchBulkDefault({
        data: {
          runId: "planned-bulk-spec-2026-04-01-run-1",
          batchId: "default-batch-1",
          lane: "default",
          specId: spec.id,
          runDateKey: "2026-04-01",
          runSequence: 1,
        },
      });

      expect(result).toMatchObject({
        skipped: true,
        skipReason: "batch_already_complete",
      });
      expect(sgMailMock.send).not.toHaveBeenCalled();
      expect(firestoreMock.reserveNotificationBatch).not.toHaveBeenCalled();
      expect(firestoreMock.markBulkNotificationBatchRunning).not.toHaveBeenCalled();
      expect(firestoreMock.finalizeBulkNotificationRunIfTerminal).toHaveBeenCalledWith("planned-bulk-spec-2026-04-01-run-1");
    });

    it("should skip already-failed planned bulk batches without resending", async () => {
      const spec = makePlannedBulkSpec();
      mockPlannedBatch({
        spec,
        recipients: [makePlannedRecipient("failed")],
        batchStatus: "failed",
      });

      const result = await notificationsModule.processNotificationBatchBulkDefault({
        data: {
          runId: "planned-bulk-spec-2026-04-01-run-1",
          batchId: "default-batch-1",
          lane: "default",
          specId: spec.id,
          runDateKey: "2026-04-01",
          runSequence: 1,
        },
      });

      expect(result).toMatchObject({
        skipped: true,
        skipReason: "batch_already_failed",
      });
      expect(sgMailMock.send).not.toHaveBeenCalled();
      expect(firestoreMock.reserveNotificationBatch).not.toHaveBeenCalled();
      expect(firestoreMock.markBulkNotificationBatchRunning).not.toHaveBeenCalled();
      expect(firestoreMock.finalizeBulkNotificationRunIfTerminal).toHaveBeenCalledWith("planned-bulk-spec-2026-04-01-run-1");
    });

    it("should skip a planned bulk batch when the running claim is rejected", async () => {
      const spec = makePlannedBulkSpec();
      mockPlannedBatch({
        spec,
        recipients: [makePlannedRecipient("claimed")],
      });
      firestoreMock.markBulkNotificationBatchRunning.mockResolvedValueOnce(false);

      const result = await notificationsModule.processNotificationBatchBulkDefault({
        data: {
          runId: "planned-bulk-spec-2026-04-01-run-1",
          batchId: "default-batch-1",
          lane: "default",
          specId: spec.id,
          runDateKey: "2026-04-01",
          runSequence: 1,
        },
      });

      expect(result).toMatchObject({
        skipped: true,
        skipReason: "batch_claim_rejected",
      });
      expect(sgMailMock.send).not.toHaveBeenCalled();
      expect(firestoreMock.reserveNotificationBatch).not.toHaveBeenCalled();
      expect(firestoreMock.finalizeBulkNotificationRunIfTerminal).toHaveBeenCalledWith("planned-bulk-spec-2026-04-01-run-1");
    });

    it("should throw for an active duplicate planned batch attempt so Cloud Tasks retries later", async () => {
      const spec = makePlannedBulkSpec();
      mockPlannedBatch({
        spec,
        recipients: [makePlannedRecipient("active-duplicate")],
        batchStatus: "running",
      });
      firestoreMock.markBulkNotificationBatchRunning.mockResolvedValueOnce(false);

      await expect(notificationsModule.processNotificationBatchBulkDefault({
        data: {
          runId: "planned-bulk-spec-2026-04-01-run-1",
          batchId: "default-batch-1",
          lane: "default",
          specId: spec.id,
          runDateKey: "2026-04-01",
          runSequence: 1,
        },
      })).rejects.toMatchObject({ code: "ACTIVE_BULK_BATCH_ATTEMPT" });

      expect(sgMailMock.send).not.toHaveBeenCalled();
      expect(firestoreMock.reserveNotificationBatch).not.toHaveBeenCalled();
      expect(firestoreMock.finalizeBulkNotificationRunIfTerminal).not.toHaveBeenCalled();
    });

    it("should render planned login details from redacted payloads without raw auth fields", async () => {
      process.env.GCLOUD_PROJECT = "nih-nci-dceg-connect-prod-6d04";
      sharedMock.developmentTier = "PROD";
      process.env.GCLOUD_UNSUBSCRIBE_SECRET = "secret/unsub-key";
      sharedMock.getSecret.mockImplementation((key) =>
        Promise.resolve(key === "secret/unsub-key" ? "test-unsub-secret" : "fake-secret")
      );
      const conceptIds = require("../../utils/fieldToConceptIdMapping");
      const spec = makePlannedBulkSpec({
        id: "planned-login-spec",
        email: {
          english: { subject: "Login", body: "<p>Hello <firstName>, <loginDetails></p>" },
        },
      });
      mockPlannedBatch({
        spec,
        recipients: [{
          ...makePlannedRecipient("login", "login@test.gov"),
          loginDetails: "***-4567",
        }],
        runId: "planned-login-spec-2026-04-01-run-1",
      });

      await notificationsModule.processNotificationBatchBulkDefault({
        data: {
          runId: "planned-login-spec-2026-04-01-run-1",
          batchId: "default-batch-1",
          lane: "default",
          specId: spec.id,
          runDateKey: "2026-04-01",
          runSequence: 1,
        },
      });

      const personalization = sgMailMock.send.mock.calls[0][0].personalizations[0];
      expect(personalization.substitutions.loginDetails).toBe("***-4567");
      expect(personalization.custom_args).not.toHaveProperty(`${conceptIds.signInMechanism}`);
      expect(personalization.custom_args).not.toHaveProperty(`${conceptIds.authenticationPhone}`);
      expect(personalization.custom_args).not.toHaveProperty(`${conceptIds.authenticationEmail}`);
    });

    it("should complete planned DEV bulk batches in noop mode without calling SendGrid", async () => {
      sharedMock.developmentTier = "DEV";
      process.env.GCLOUD_PROJECT = "nih-nci-dceg-connect-dev";
      setNotificationSettings({ sendgridDeliveryModeOverride: "noop" });
      process.env.GCLOUD_UNSUBSCRIBE_SECRET = "secret/unsub-key";
      sharedMock.getSecret.mockImplementation((key) =>
        Promise.resolve(key === "secret/unsub-key" ? "test-unsub-secret" : "fake-secret")
      );
      const spec = makePlannedBulkSpec({ id: "planned-noop-spec" });
      const recipients = [makePlannedRecipient("noop", "noop@test.gov")];
      mockPlannedBatch({ spec, recipients, runId: "planned-noop-spec-2026-04-01-run-1" });

      await notificationsModule.processNotificationBatchBulkDefault({
        data: {
          runId: "planned-noop-spec-2026-04-01-run-1",
          batchId: "default-batch-1",
          lane: "default",
          specId: spec.id,
          runDateKey: "2026-04-01",
          runSequence: 1,
        },
      });

      expect(sgMailMock.send).not.toHaveBeenCalled();
      expect(firestoreMock.reserveNotificationBatch).toHaveBeenCalledTimes(1);
      expect(firestoreMock.markNotificationBatchProviderSendStarted).toHaveBeenCalledTimes(1);
      expect(firestoreMock.markNotificationBatchAccepted).toHaveBeenCalledTimes(1);
      expect(firestoreMock.markBulkNotificationBatchComplete).toHaveBeenCalledWith(expect.objectContaining({
        runId: "planned-noop-spec-2026-04-01-run-1",
        batchId: "default-batch-1",
        counts: expect.objectContaining({ planned: 1, sent: 1 }),
      }));
    });

    it("should fail before provider send when non-prod (STAGE)sandbox planned bulk recipients are not allowlisted", async () => {
      sharedMock.developmentTier = "STAGE";
      process.env.GCLOUD_PROJECT = "nih-nci-dceg-connect-stg-5519";
      setNotificationSettings({});
      process.env.GCLOUD_UNSUBSCRIBE_SECRET = "secret/unsub-key";
      sharedMock.getSecret.mockImplementation((key) =>
        Promise.resolve(key === "secret/unsub-key" ? "test-unsub-secret" : "fake-secret")
      );
      const spec = makePlannedBulkSpec({ id: "planned-sandbox-blocked" });
      mockPlannedBatch({
        spec,
        recipients: [makePlannedRecipient("blocked", "blocked@test.gov")],
        runId: "planned-sandbox-blocked-2026-04-01-run-1",
      });

      await expect(notificationsModule.processNotificationBatchBulkDefault({
        data: {
          runId: "planned-sandbox-blocked-2026-04-01-run-1",
          batchId: "default-batch-1",
          lane: "default",
          specId: spec.id,
          runDateKey: "2026-04-01",
          runSequence: 1,
        },
      })).rejects.toThrow("Failed sending emails for planned-sandbox-blocked");

      expect(sgMailMock.send).not.toHaveBeenCalled();
      expect(firestoreMock.markNotificationBatchFailed).toHaveBeenCalledTimes(1);
      expect(firestoreMock.markBulkNotificationBatchFailed).not.toHaveBeenCalled();
      expect(firestoreMock.finalizeBulkNotificationRunIfTerminal).not.toHaveBeenCalled();
    });

    it("should send planned bulk tasks in sandbox mode (STAGE) when recipients are allowlisted", async () => {
      sharedMock.developmentTier = "STAGE";
      process.env.GCLOUD_PROJECT = "nih-nci-dceg-connect-stg-5519";
      setNotificationSettings({ nonProdEmailAllowlist: ["allowed@test.gov"] });
      process.env.GCLOUD_UNSUBSCRIBE_SECRET = "secret/unsub-key";
      sharedMock.getSecret.mockImplementation((key) =>
        Promise.resolve(key === "secret/unsub-key" ? "test-unsub-secret" : "fake-secret")
      );
      const spec = makePlannedBulkSpec({ id: "planned-sandbox-spec" });
      mockPlannedBatch({
        spec,
        recipients: [makePlannedRecipient("allowed", "allowed@test.gov")],
        runId: "planned-sandbox-spec-2026-04-01-run-1",
      });

      await notificationsModule.processNotificationBatchBulkDefault({
        data: {
          runId: "planned-sandbox-spec-2026-04-01-run-1",
          batchId: "default-batch-1",
          lane: "default",
          specId: spec.id,
          runDateKey: "2026-04-01",
          runSequence: 1,
        },
      });

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      expect(sgMailMock.send.mock.calls[0][0].mail_settings?.sandbox_mode?.enable).toBe(true);
      expect(firestoreMock.markBulkNotificationBatchComplete).toHaveBeenCalledTimes(1);
    });

    it("should send a planned default batch and record filtered and suppressed recipients", async () => {
      process.env.GCLOUD_PROJECT = "nih-nci-dceg-connect-prod-6d04";
      sharedMock.developmentTier = "PROD";
      process.env.GCLOUD_UNSUBSCRIBE_SECRET = "secret/unsub-key";
      sharedMock.getSecret.mockImplementation((key) =>
        Promise.resolve(key === "secret/unsub-key" ? "test-unsub-secret" : "fake-secret")
      );
      const spec = makePlannedBulkSpec();
      const recipients = [
        makePlannedRecipient("valid", "valid@test.gov"),
        makePlannedRecipient("suppressed", "suppressed@test.gov"),
        makePlannedRecipient("filtered", "noreply@nih.gov"),
      ];
      mockPlannedBatch({ spec, recipients });
      firestoreMock.getEmailSuppressions.mockResolvedValue(new Set(["suppressed@test.gov"]));

      await notificationsModule.processNotificationBatchBulkDefault({
        data: {
          runId: "planned-bulk-spec-2026-04-01-run-1",
          batchId: "default-batch-1",
          lane: "default",
          specId: spec.id,
          runDateKey: "2026-04-01",
          runSequence: 1,
        },
      });

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      expect(sgMailMock.send.mock.calls[0][0].personalizations).toHaveLength(1);
      expect(firestoreMock.markBulkNotificationBatchRunning).toHaveBeenCalledWith(expect.objectContaining({
        runId: "planned-bulk-spec-2026-04-01-run-1",
        batchId: "default-batch-1",
        taskAttemptOwner: "planned-bulk-spec-2026-04-01-run-1-default-batch-1-attempt-0",
        attemptDurationMs: 600000,
      }));
      expect(firestoreMock.markBulkNotificationBatchComplete).toHaveBeenCalledTimes(1);
      const completeArg = firestoreMock.markBulkNotificationBatchComplete.mock.calls[0][0];
      expect(completeArg.taskAttemptOwner).toBe("planned-bulk-spec-2026-04-01-run-1-default-batch-1-attempt-0");
      expect(completeArg.counts).toMatchObject({
        planned: 3,
        sent: 1,
        filtered: 1,
        suppressed: 1,
      });
      expect(completeArg.unsuccessful.filtered[0]).toMatchObject({ token: "filtered", reason: "filtered_address" });
      expect(completeArg.unsuccessful.filtered[0]).not.toHaveProperty("email");
      expect(completeArg.unsuccessful.filtered[0]).not.toHaveProperty("connectId");
      expect(completeArg.unsuccessful.suppressed[0]).toMatchObject({ token: "suppressed", reason: "suppressed" });
      expect(completeArg.unsuccessful.suppressed[0]).not.toHaveProperty("email");
      expect(firestoreMock.finalizeBulkNotificationRunIfTerminal).toHaveBeenCalledWith("planned-bulk-spec-2026-04-01-run-1");
    });

    it("should record provider acceptance unknown details when a planned batch final attempt fails ambiguously", async () => {
      process.env.GCLOUD_PROJECT = "nih-nci-dceg-connect-prod-6d04";
      sharedMock.developmentTier = "PROD";
      process.env.GCLOUD_UNSUBSCRIBE_SECRET = "secret/unsub-key";
      sharedMock.getSecret.mockImplementation((key) =>
        Promise.resolve(key === "secret/unsub-key" ? "test-unsub-secret" : "fake-secret")
      );
      const spec = makePlannedBulkSpec({ id: "planned-unknown-spec" });
      const recipients = [makePlannedRecipient("unknown", "unknown@test.gov")];
      mockPlannedBatch({ spec, recipients, runId: "planned-unknown-spec-2026-04-01-run-1" });
      sgMailMock.send.mockRejectedValueOnce(Object.assign(new Error("SendGrid unavailable"), { statusCode: 503 }));

      await expect(notificationsModule.processNotificationBatchBulkDefault({
        retryCount: 4,
        data: {
          runId: "planned-unknown-spec-2026-04-01-run-1",
          batchId: "default-batch-1",
          lane: "default",
          specId: spec.id,
          runDateKey: "2026-04-01",
          runSequence: 1,
        },
      })).rejects.toThrow("Failed sending emails");

      expect(firestoreMock.markBulkNotificationBatchFailed).toHaveBeenCalledTimes(1);
      const failedArg = firestoreMock.markBulkNotificationBatchFailed.mock.calls[0][0];
      expect(failedArg.taskAttemptOwner).toBe("planned-unknown-spec-2026-04-01-run-1-default-batch-1-attempt-4");
      expect(failedArg.counts).toMatchObject({
        planned: 1,
        sent: 0,
        providerFailed: 0,
        providerUnknown: 1,
      });
      expect(failedArg.unsuccessful.providerUnknown[0]).toMatchObject({
        token: "unknown",
        reason: "provider_acceptance_unknown",
      });
      expect(failedArg.unsuccessful.providerUnknown[0]).not.toHaveProperty("email");
    });

    it("should count accepted planned recipients before a later final batch failure", async () => {
      const conceptIds = require("../../utils/fieldToConceptIdMapping");
      process.env.GCLOUD_PROJECT = "nih-nci-dceg-connect-prod-6d04";
      sharedMock.developmentTier = "PROD";
      sharedMock.cidToLangMapper[conceptIds.english] = "english";
      sharedMock.cidToLangMapper[conceptIds.spanish] = "spanish";
      process.env.GCLOUD_UNSUBSCRIBE_SECRET = "secret/unsub-key";
      sharedMock.getSecret.mockImplementation((key) =>
        Promise.resolve(key === "secret/unsub-key" ? "test-unsub-secret" : "fake-secret")
      );
      const spec = makePlannedBulkSpec({
        id: "planned-partial-success",
        email: {
          english: { subject: "English", body: "<p>Hello {{firstName}}</p>" },
          spanish: { subject: "Spanish", body: "<p>Hola {{firstName}}</p>" },
        },
      });
      mockPlannedBatch({
        spec,
        recipients: [
          makePlannedRecipient("english", "english@test.gov"),
          {
            ...makePlannedRecipient("spanish", "spanish@test.gov"),
            [conceptIds.preferredLanguage]: conceptIds.spanish,
          },
        ],
        runId: "planned-partial-success-2026-04-01-run-1",
      });
      sgMailMock.send
        .mockResolvedValueOnce([{ statusCode: 202 }])
        .mockRejectedValueOnce(Object.assign(new Error("SendGrid unavailable"), { statusCode: 503 }));

      await expect(notificationsModule.processNotificationBatchBulkDefault({
        retryCount: 4,
        data: {
          runId: "planned-partial-success-2026-04-01-run-1",
          batchId: "default-batch-1",
          lane: "default",
          specId: spec.id,
          runDateKey: "2026-04-01",
          runSequence: 1,
        },
      })).rejects.toThrow("Failed sending emails");

      expect(sgMailMock.send).toHaveBeenCalledTimes(2);
      const failedArg = firestoreMock.markBulkNotificationBatchFailed.mock.calls[0][0];
      expect(failedArg.counts).toMatchObject({
        planned: 2,
        sent: 1,
        providerUnknown: 1,
      });
      expect(failedArg.unsuccessful.providerUnknown[0]).toMatchObject({
        token: "spanish",
        reason: "provider_acceptance_unknown",
      });
      expect(failedArg.unsuccessful.providerUnknown[0]).not.toHaveProperty("email");
    });

    it("should not mark a planned batch failed before the final Cloud Tasks attempt", async () => {
      process.env.GCLOUD_PROJECT = "nih-nci-dceg-connect-prod-6d04";
      sharedMock.developmentTier = "PROD";
      process.env.GCLOUD_UNSUBSCRIBE_SECRET = "secret/unsub-key";
      sharedMock.getSecret.mockImplementation((key) =>
        Promise.resolve(key === "secret/unsub-key" ? "test-unsub-secret" : "fake-secret")
      );
      const spec = makePlannedBulkSpec({ id: "planned-retry-spec" });
      mockPlannedBatch({
        spec,
        recipients: [makePlannedRecipient("retry", "retry@test.gov")],
        runId: "planned-retry-spec-2026-04-01-run-1",
      });
      sgMailMock.send.mockRejectedValueOnce(Object.assign(new Error("SendGrid unavailable"), { statusCode: 503 }));

      await expect(notificationsModule.processNotificationBatchBulkDefault({
        retryCount: 1,
        data: {
          runId: "planned-retry-spec-2026-04-01-run-1",
          batchId: "default-batch-1",
          lane: "default",
          specId: spec.id,
          runDateKey: "2026-04-01",
          runSequence: 1,
        },
      })).rejects.toThrow("Failed sending emails");

      expect(firestoreMock.markBulkNotificationBatchFailed).not.toHaveBeenCalled();
      expect(firestoreMock.finalizeBulkNotificationRunIfTerminal).not.toHaveBeenCalled();
    });

    it("should record provider-failed details when a final planned task attempt gets a provider rejection", async () => {
      process.env.GCLOUD_PROJECT = "nih-nci-dceg-connect-prod-6d04";
      sharedMock.developmentTier = "PROD";
      process.env.GCLOUD_UNSUBSCRIBE_SECRET = "secret/unsub-key";
      sharedMock.getSecret.mockImplementation((key) =>
        Promise.resolve(key === "secret/unsub-key" ? "test-unsub-secret" : "fake-secret")
      );
      const spec = makePlannedBulkSpec({ id: "planned-provider-failed" });
      mockPlannedBatch({
        spec,
        recipients: [makePlannedRecipient("failed", "failed@test.gov")],
        runId: "planned-provider-failed-2026-04-01-run-1",
      });
      sgMailMock.send.mockRejectedValueOnce(Object.assign(new Error("Bad request"), { statusCode: 400 }));

      await expect(notificationsModule.processNotificationBatchBulkDefault({
        retryCount: 4,
        data: {
          runId: "planned-provider-failed-2026-04-01-run-1",
          batchId: "default-batch-1",
          lane: "default",
          specId: spec.id,
          runDateKey: "2026-04-01",
          runSequence: 1,
        },
      })).rejects.toThrow("Failed sending emails");

      expect(firestoreMock.markNotificationBatchFailed).toHaveBeenCalledTimes(1);
      expect(firestoreMock.markNotificationBatchProviderAcceptanceUnknown).not.toHaveBeenCalled();
      const failedArg = firestoreMock.markBulkNotificationBatchFailed.mock.calls[0][0];
      expect(failedArg.counts).toMatchObject({
        planned: 1,
        sent: 0,
        providerFailed: 1,
        providerUnknown: 0,
      });
      expect(failedArg.unsuccessful.providerFailed[0]).toMatchObject({
        token: "failed",
        reason: "provider_failed",
      });
      expect(failedArg.unsuccessful.providerFailed[0]).not.toHaveProperty("email");
      expect(firestoreMock.finalizeBulkNotificationRunIfTerminal).toHaveBeenCalledWith("planned-provider-failed-2026-04-01-run-1");
    });

    it("should record provider-unknown when provider accepted but acceptance state writes fail", async () => {
      process.env.GCLOUD_PROJECT = "nih-nci-dceg-connect-prod-6d04";
      sharedMock.developmentTier = "PROD";
      process.env.GCLOUD_UNSUBSCRIBE_SECRET = "secret/unsub-key";
      sharedMock.getSecret.mockImplementation((key) =>
        Promise.resolve(key === "secret/unsub-key" ? "test-unsub-secret" : "fake-secret")
      );
      const spec = makePlannedBulkSpec({ id: "planned-accepted-write-failed" });
      mockPlannedBatch({
        spec,
        recipients: [makePlannedRecipient("accepted-write-failed", "accepted-write-failed@test.gov")],
        runId: "planned-accepted-write-failed-2026-04-01-run-1",
      });
      firestoreMock.markNotificationBatchAccepted.mockResolvedValueOnce(0);

      await expect(notificationsModule.processNotificationBatchBulkDefault({
        retryCount: 4,
        data: {
          runId: "planned-accepted-write-failed-2026-04-01-run-1",
          batchId: "default-batch-1",
          lane: "default",
          specId: spec.id,
          runDateKey: "2026-04-01",
          runSequence: 1,
        },
      })).rejects.toThrow("acceptance state write");

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      expect(firestoreMock.markNotificationBatchProviderAcceptanceUnknown).toHaveBeenCalledTimes(1);
      const failedArg = firestoreMock.markBulkNotificationBatchFailed.mock.calls[0][0];
      expect(failedArg.counts).toMatchObject({
        planned: 1,
        sent: 0,
        providerUnknown: 1,
      });
      expect(failedArg.unsuccessful.providerUnknown[0]).toMatchObject({
        token: "accepted-write-failed",
        reason: "provider_acceptance_unknown",
      });
      expect(failedArg.unsuccessful.providerUnknown[0]).not.toHaveProperty("email");
    });

    it("should recover a permanently failed queued batch on the next scheduler run without re-sending accepted recipients", async () => {
      process.env.GCLOUD_PROJECT = "nih-nci-dceg-connect-prod-6d04";
      sharedMock.developmentTier = "PROD";
      setNotificationSettings({ useCloudTasksBulk: true });
      process.env.GCLOUD_UNSUBSCRIBE_SECRET = "secret/unsub-key";
      sharedMock.getSecret.mockImplementation((key) =>
        Promise.resolve(key === "secret/unsub-key" ? "test-unsub-secret" : "fake-secret")
      );
      sharedMock.getAdjustedTime.mockReturnValue(new Date("2026-01-01T00:00:00.000Z"));

      const spec = {
        id: "sched-recovery-spec",
        category: "newsletter",
        attempt: "1st",
        primaryField: "d_821247024",
        time: { start: { day: 0, hour: 1, minute: 0 }, stop: { day: 0, hour: 0, minute: 0 } },
        notificationType: ["email"],
        emailField: "d_335767902",
        phoneField: "",
        firstNameField: "d_153098809",
        preferredNameField: "",
        email: {
          english: { subject: "Scheduled Subject", body: "<p>Hello {{firstName}}</p>" },
        },
        sms: {},
        conditions: JSON.stringify([["d_821247024", "equals", "197316935"]]),
      };
      const participant = {
        Connect_ID: "C-tok-recovery",
        token: "tok-recovery",
        state: { uid: "uid-tok-recovery" },
        d_335767902: "tok-recovery@test.gov",
        d_153098809: "Taylor",
        353358909: 0,
      };
      firestoreMock.getNotificationSpecsByScheduleOncePerDay
        .mockResolvedValueOnce([spec])
        .mockResolvedValueOnce([{ ...spec, bulkRunSequence: 1 }]);
      bigqueryMock.countParticipantsForNotificationsBQ.mockResolvedValue(1);
      bigqueryMock.getParticipantsForNotificationsBQ
        .mockResolvedValueOnce([participant])
        .mockResolvedValueOnce([participant])
        .mockResolvedValue([]);

      const schedulerReq = { method: "POST", body: { scheduleAt: "15:00" } };
      const firstSchedulerRes = createResponseMock();
      await notificationsModule.sendScheduledNotifications(schedulerReq, firstSchedulerRes);

      expect(taskQueueMock.enqueue).toHaveBeenCalledTimes(1);
      const [firstTaskPayload, firstTaskOpts] = taskQueueMock.enqueue.mock.calls[0];
      expect(firstTaskPayload.batchId).toBe("default-batch-1");
      expect(firstTaskPayload.runSequence).toBe(1);

      sgMailMock.send.mockRejectedValueOnce(new Error("Provider send failed"));
      firestoreMock.getBulkNotificationBatch.mockResolvedValueOnce({
        run: {
          id: firstTaskPayload.runId,
          specId: spec.id,
          runDateKey: firstTaskPayload.runDateKey,
          runSequence: 1,
          notificationSpec: spec,
          timeParams: {
            startTimeStr: "2026-01-01T00:00:00.000Z",
            stopTimeStr: "2026-01-01T00:00:00.000Z",
            timeField: "d_821247024",
          },
          conditions: [["d_821247024", "equals", "197316935"]],
        },
        batch: {
          id: firstTaskPayload.batchId,
          lane: "default",
          batchNumber: 1,
          recipientCount: 1,
          recipients: [participant],
          unsuccessful: {},
        },
      });

      await expect(notificationsModule.processNotificationBatchBulkDefault({
        id: firstTaskOpts.id,
        retryCount: 4,
        data: firstTaskPayload,
      })).rejects.toThrow("Failed sending emails");

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      expect(firestoreMock.markBulkNotificationBatchFailed).toHaveBeenCalledTimes(1);
      expect(firestoreMock.finalizeBulkNotificationRunIfTerminal).toHaveBeenCalledWith(firstTaskPayload.runId);

      const secondSchedulerRes = createResponseMock();
      await notificationsModule.sendScheduledNotifications(schedulerReq, secondSchedulerRes);

      expect(taskQueueMock.enqueue).toHaveBeenCalledTimes(2);
      const [recoveryTaskPayload, recoveryTaskOpts] = taskQueueMock.enqueue.mock.calls[1];
      expect(recoveryTaskPayload.batchId).toBe("default-batch-1");
      expect(recoveryTaskPayload.runSequence).toBe(2);
      expect(recoveryTaskOpts.id).not.toBe(firstTaskOpts.id);
      expect(recoveryTaskOpts.id).toContain("-run-2-default-batch-1");

      sgMailMock.send.mockClear();
      firestoreMock.markNotificationBatchAccepted.mockClear();
      firestoreMock.markNotificationBatchFailed.mockClear();
      firestoreMock.getBulkNotificationBatch.mockResolvedValueOnce({
        run: {
          id: recoveryTaskPayload.runId,
          specId: spec.id,
          runDateKey: recoveryTaskPayload.runDateKey,
          runSequence: 2,
          notificationSpec: spec,
          timeParams: {
            startTimeStr: "2026-01-01T00:00:00.000Z",
            stopTimeStr: "2026-01-01T00:00:00.000Z",
            timeField: "d_821247024",
          },
          conditions: [["d_821247024", "equals", "197316935"]],
        },
        batch: {
          id: recoveryTaskPayload.batchId,
          lane: "default",
          batchNumber: 1,
          recipientCount: 1,
          recipients: [participant],
          unsuccessful: {},
        },
      });

      await notificationsModule.processNotificationBatchBulkDefault({
        id: recoveryTaskOpts.id,
        data: recoveryTaskPayload,
      });

      expect(sgMailMock.send).toHaveBeenCalledTimes(1);
      expect(firestoreMock.markNotificationBatchAccepted).toHaveBeenCalledTimes(1);
      expect(firestoreMock.markNotificationBatchFailed).not.toHaveBeenCalled();
      expect(firestoreMock.markBulkNotificationBatchComplete).toHaveBeenCalledTimes(1);
    });
  });
});

let recordIdCounter = 0;
const makeSmsRecord = (specId, language = "english", token = "token-1") => ({
  id: `record-${++recordIdCounter}`,
  notificationSpecificationsID: specId,
  language,
  token,
  phone: "+15551234567",
  notification: {
    title: "Message Title",
    body: "This is a test message.",
    time: new Date().toISOString(),
  },
});

const successResult = (smsRecord) => ({
  smsRecord: { ...smsRecord, messageSid: "SM_fake_sid" },
  isSuccess: true,
  isRateLimit: false,
});

const failResult = (smsRecord) => ({
  smsRecord,
  isSuccess: false,
  isRateLimit: false,
});

const rateLimitResult = (smsRecord) => ({
  smsRecord,
  isSuccess: false,
  isRateLimit: true,
});

describe("SmsBatchSender", () => {
  let sendFn;
  let saveFn;
  let delayFn;

  const createSender = (opts = {}) =>
    new SmsBatchSender({
      batchSize: opts.batchSize ?? 10,
      maxRetries: opts.maxRetries,
      sendFn,
      saveFn,
      delayFn,
    });

  beforeEach(() => {
    sendFn = vi.fn();
    saveFn = vi.fn().mockResolvedValue(undefined);
    delayFn = vi.fn().mockResolvedValue(undefined);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe("getSentCounts / getFailedCounts", () => {
    it("should return zero counts for unknown specId", () => {
      const sender = createSender();
      expect(sender.getSentCounts("unknown")).toEqual({
        english: 0,
        spanish: 0,
      });
      expect(sender.getFailedCounts("unknown")).toEqual({
        english: 0,
        spanish: 0,
      });
    });

    it("should return a copy so callers cannot mutate internal state", () => {
      const sender = createSender();
      const counts = sender.getSentCounts("spec1");
      counts.english = 999;
      expect(sender.getSentCounts("spec1")).toEqual({ english: 0, spanish: 0 });
    });
  });

  describe("isSpecFinished", () => {
    it("should return false for unknown specId", () => {
      const sender = createSender();
      expect(sender.isSpecFinished("spec1")).toBe(false);
    });
  });

  describe("basic send flow", () => {
    it("should send all records and track sent counts", async () => {
      sendFn.mockImplementation((record) => Promise.resolve(successResult(record)));

      const sender = createSender();
      const records = [
        makeSmsRecord("spec1", "english"),
        makeSmsRecord("spec1", "spanish"),
        makeSmsRecord("spec1", "english"),
      ];
      sender.addToQueue(records);
      sender.markSpecEnd("spec1");

      const result = await sender.waitForSpec("spec1");
      expect(result.sentCounts).toEqual({ english: 2, spanish: 1 });
      expect(result.failedCounts).toEqual({ english: 0, spanish: 0 });
      expect(sendFn.mock.calls.length).toBe(3);
      expect(saveFn.mock.calls.length).toBe(1);
      expect(saveFn.mock.calls[0][0]).toHaveLength(3);
    });

    it("should handle an empty queue with only an end marker", async () => {
      const sender = createSender();
      sender.markSpecEnd("spec1");

      const result = await sender.waitForSpec("spec1");
      expect(result.sentCounts).toEqual({ english: 0, spanish: 0 });
      expect(result.failedCounts).toEqual({ english: 0, spanish: 0 });
      expect(sendFn.mock.calls.length).toBe(0);
      expect(saveFn.mock.calls.length).toBe(0);
    });
  });

  describe("failure handling", () => {
    it("should track failed counts for non-rate-limit errors", async () => {
      sendFn.mockImplementation((record) => Promise.resolve(failResult(record)));

      const sender = createSender();
      sender.addToQueue([
        makeSmsRecord("spec1", "english"),
        makeSmsRecord("spec1", "spanish"),
      ]);
      sender.markSpecEnd("spec1");

      const result = await sender.waitForSpec("spec1");
      expect(result.sentCounts).toEqual({ english: 0, spanish: 0 });
      expect(result.failedCounts).toEqual({ english: 1, spanish: 1 });
      expect(saveFn.mock.calls.length).toBe(1);
    });

    it("should not increment sent counts when saveNotificationBatch throws", async () => {
      sendFn.mockImplementation((record) => Promise.resolve(successResult(record)));
      saveFn.mockRejectedValue(new Error("Firestore write failed"));

      const consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
      const sender = createSender();
      sender.addToQueue([makeSmsRecord("spec1", "english")]);
      sender.markSpecEnd("spec1");

      const result = await sender.waitForSpec("spec1");
      expect(result.sentCounts).toEqual({ english: 0, spanish: 0 });
      expect(result.failedCounts).toEqual({ english: 0, spanish: 0 });
      expect(consoleErrorSpy).toHaveBeenCalledTimes(1);
      consoleErrorSpy.mockRestore();
    });
  });

  describe("rate limit retry", () => {
    it("should re-queue rate-limited records and retry them", async () => {
      const record = makeSmsRecord("spec1", "english");
      let callCount = 0;
      sendFn.mockImplementation((r) => {
        callCount++;
        if (callCount === 1) return Promise.resolve(rateLimitResult(r));
        return Promise.resolve(successResult(r));
      });

      const sender = createSender();
      sender.addToQueue([record]);
      sender.markSpecEnd("spec1");

      const result = await sender.waitForSpec("spec1");
      expect(result.sentCounts).toEqual({ english: 1, spanish: 0 });
      expect(sendFn.mock.calls.length).toBe(2);
    });

    it("should count as failed after exceeding maxRetries", async () => {
      sendFn.mockImplementation((r) => Promise.resolve(rateLimitResult(r)));
      const consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

      const sender = createSender({ batchSize: 10, maxRetries: 2 });
      sender.addToQueue([makeSmsRecord("spec1", "english")]);
      sender.markSpecEnd("spec1");

      const result = await sender.waitForSpec("spec1");
      expect(result.sentCounts).toEqual({ english: 0, spanish: 0 });
      expect(result.failedCounts).toEqual({ english: 1, spanish: 0 });
      // 1 initial + 2 retries = 3 total send attempts
      expect(sendFn.mock.calls.length).toBe(3);
      consoleErrorSpy.mockRestore();
    });

    it("should succeed on retry within maxRetries limit", async () => {
      let callCount = 0;
      sendFn.mockImplementation((r) => {
        callCount++;
        if (callCount <= 2) return Promise.resolve(rateLimitResult(r));
        return Promise.resolve(successResult(r));
      });

      const sender = createSender({ batchSize: 10, maxRetries: 3 });
      sender.addToQueue([makeSmsRecord("spec1", "english")]);
      sender.markSpecEnd("spec1");

      const result = await sender.waitForSpec("spec1");
      expect(result.sentCounts).toEqual({ english: 1, spanish: 0 });
      expect(result.failedCounts).toEqual({ english: 0, spanish: 0 });
    });

    it("should preserve end marker when rate-limited records are re-queued", async () => {
      let callCount = 0;
      sendFn.mockImplementation((r) => {
        callCount++;
        if (callCount <= 2) return Promise.resolve(rateLimitResult(r));
        return Promise.resolve(successResult(r));
      });

      const sender = createSender();
      sender.addToQueue([
        makeSmsRecord("spec1", "english"),
        makeSmsRecord("spec1", "spanish"),
      ]);
      sender.markSpecEnd("spec1");

      const result = await sender.waitForSpec("spec1");
      expect(result.sentCounts).toEqual({ english: 1, spanish: 1 });
      expect(sender.isSpecFinished("spec1")).toBe(true);
    });
  });

  describe("mixed results", () => {
    it("should handle a mix of success, failure, and rate-limit in one batch", async () => {
      const records = [
        makeSmsRecord("spec1", "english", "token-success"),
        makeSmsRecord("spec1", "spanish", "token-fail"),
        makeSmsRecord("spec1", "english", "token-ratelimit"),
      ];

      let rateLimitRetried = false;
      sendFn.mockImplementation((r) => {
        if (r.token === "token-success") return Promise.resolve(successResult(r));
        if (r.token === "token-fail") return Promise.resolve(failResult(r));
        if (r.token === "token-ratelimit") {
          if (!rateLimitRetried) {
            rateLimitRetried = true;
            return Promise.resolve(rateLimitResult(r));
          }
          return Promise.resolve(successResult(r));
        }
      });

      const sender = createSender();
      sender.addToQueue(records);
      sender.markSpecEnd("spec1");

      const result = await sender.waitForSpec("spec1");
      expect(result.sentCounts).toEqual({ english: 2, spanish: 0 });
      expect(result.failedCounts).toEqual({ english: 0, spanish: 1 });
    });
  });

  describe("batching", () => {
    it("should respect batchSize and process in multiple batches", async () => {
      sendFn.mockImplementation((r) => Promise.resolve(successResult(r)));

      const sender = createSender({ batchSize: 2 });
      const records = [
        makeSmsRecord("spec1", "english", "a"),
        makeSmsRecord("spec1", "english", "b"),
        makeSmsRecord("spec1", "english", "c"),
      ];
      sender.addToQueue(records);
      sender.markSpecEnd("spec1");

      const result = await sender.waitForSpec("spec1");
      expect(result.sentCounts).toEqual({ english: 3, spanish: 0 });
      // saveFn called once per batch that has successful records
      expect(saveFn.mock.calls.length).toBe(2);
    });
  });

  describe("multiple specs", () => {
    it("should track counts independently per specId", async () => {
      sendFn.mockImplementation((r) => {
        if (r.notificationSpecificationsID === "spec2") {
          return Promise.resolve(failResult(r));
        }
        return Promise.resolve(successResult(r));
      });

      const sender = createSender();
      sender.addToQueue([
        makeSmsRecord("spec1", "english"),
        makeSmsRecord("spec2", "english"),
        makeSmsRecord("spec1", "spanish"),
      ]);
      sender.markSpecEnd("spec1");
      sender.markSpecEnd("spec2");

      const [result1, result2] = await Promise.all([
        sender.waitForSpec("spec1"),
        sender.waitForSpec("spec2"),
      ]);

      expect(result1.sentCounts).toEqual({ english: 1, spanish: 1 });
      expect(result1.failedCounts).toEqual({ english: 0, spanish: 0 });
      expect(result2.sentCounts).toEqual({ english: 0, spanish: 0 });
      expect(result2.failedCounts).toEqual({ english: 1, spanish: 0 });
    });

    it("should finish specs independently", async () => {
      sendFn.mockImplementation((r) => Promise.resolve(successResult(r)));

      const sender = createSender();
      sender.addToQueue([makeSmsRecord("spec1", "english")]);
      sender.markSpecEnd("spec1");

      await sender.waitForSpec("spec1");
      expect(sender.isSpecFinished("spec1")).toBe(true);
      expect(sender.isSpecFinished("spec2")).toBe(false);
    });
  });

  describe("progress logging", () => {
    let consoleLogSpy;

    beforeEach(() => {
      vi.useFakeTimers({ now: Date.now() });
      consoleLogSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    });

    afterEach(() => {
      vi.useRealTimers();
      consoleLogSpy.mockRestore();
    });

    it("should log progress for in-flight specs after 30 seconds", async () => {
      sendFn.mockImplementation((r) => Promise.resolve(successResult(r)));

      const sender = createSender({ batchSize: 1 });
      sender.addToQueue([
        makeSmsRecord("spec1", "english", "a"),
        makeSmsRecord("spec1", "english", "b"),
      ]);
      sender.markSpecEnd("spec1");

      vi.advanceTimersByTime(31000);
      await sender.waitForSpec("spec1");

      const progressLogs = consoleLogSpy.mock.calls.filter((args) =>
        args[0]?.includes?.("SMS in progress"),
      );
      expect(progressLogs.length).toBeGreaterThan(0);
      expect(progressLogs[0][0]).toContain("spec1");
      expect(progressLogs[0][0]).toContain("sent");
      expect(progressLogs[0][0]).toContain("failed");
    });

    it("should skip finished specs in progress log", async () => {
      sendFn.mockImplementation((r) => Promise.resolve(successResult(r)));

      const sender = createSender();
      sender.addToQueue([makeSmsRecord("spec1", "english")]);
      sender.markSpecEnd("spec1");

      // spec1 finishes, then start spec2 after 30s
      await sender.waitForSpec("spec1");
      vi.advanceTimersByTime(31000);

      sender.addToQueue([makeSmsRecord("spec2", "english")]);
      sender.markSpecEnd("spec2");
      await sender.waitForSpec("spec2");

      const progressLogs = consoleLogSpy.mock.calls.filter((args) =>
        args[0]?.includes?.("SMS in progress"),
      );
      const spec1Logs = progressLogs.filter((args) => args[0].includes("spec1"));
      expect(spec1Logs.length).toBe(0);
    });
  });

  describe("delay behavior", () => {
    it("should call delayFn for rate limiting between batches", async () => {
      sendFn.mockImplementation((r) => Promise.resolve(successResult(r)));

      const sender = createSender({ batchSize: 1 });
      sender.addToQueue([
        makeSmsRecord("spec1", "english", "a"),
        makeSmsRecord("spec1", "english", "b"),
      ]);
      sender.markSpecEnd("spec1");

      await sender.waitForSpec("spec1");
      expect(delayFn).toHaveBeenCalled();
    });
  });
});
