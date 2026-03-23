require("../../utils/shared");
require("../../utils/firestore");

const sharedMock = {
  getResponseJSON: vi.fn((message, code) => ({ message, code })),
  validEmailFormat: /^[^@\s]+@[^@\s]+\.[^@\s]+$/,
  getTemplateForEmailLink: vi.fn().mockReturnValue("<p>email body</p>"),
  getSecret: vi.fn(),
  parseResponseJson: vi.fn(),
  safeJSONParse: vi.fn((str) => JSON.parse(str)),
  setHeadersDomainRestricted: vi.fn(),
  setHeaders: vi.fn(),
  logIPAddress: vi.fn(),
  redactEmailLoginInfo: vi.fn(),
  redactPhoneLoginInfo: vi.fn(),
  nihMailbox: "noreply@example.com",
  cidToLangMapper: {},
  unsubscribeTextObj: {},
  getAdjustedTime: vi.fn(),
  parseRequestBody: (body) => {
    if (!body) return {};
    const parsed = typeof body === "string" ? JSON.parse(body) : body;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
    return parsed;
  },
  delay: vi.fn().mockResolvedValue(undefined),
};

const firestoreMock = {
  generateSignInWithEmailLink: vi.fn(),
  getNotificationSpecById: vi.fn(),
  getNotificationSpecByCategoryAndAttempt: vi.fn(),
  getNotificationSpecsByScheduleOncePerDay: vi.fn(),
  saveNotificationBatch: vi.fn(),
  storeNotification: vi.fn(),
  checkIsNotificationSent: vi.fn(),
  updateSmsPermission: vi.fn(),
};

const sharedPath = require.resolve("../../utils/shared");
const firestorePath = require.resolve("../../utils/firestore");
const notificationsPath = require.resolve("../../utils/notifications");

const origSharedExports = require.cache[sharedPath].exports;
const origFirestoreExports = require.cache[firestorePath].exports;

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

  beforeEach(() => {
    originalFetch = global.fetch;

    // Reset all mocks
    sharedMock.getResponseJSON.mockReset().mockImplementation((message, code) => ({ message, code }));
    sharedMock.getTemplateForEmailLink.mockReset().mockReturnValue("<p>email body</p>");
    sharedMock.getSecret.mockReset();
    sharedMock.parseResponseJson.mockReset();
    sharedMock.safeJSONParse.mockReset().mockImplementation((str) => JSON.parse(str));

    firestoreMock.generateSignInWithEmailLink.mockReset();

    // Install mocks into require.cache
    require.cache[sharedPath].exports = sharedMock;
    require.cache[firestorePath].exports = firestoreMock;

    // Clear module under test
    delete require.cache[notificationsPath];

    // Setup env vars
    process.env.APP_REGISTRATION_CLIENT_ID = "secret/client-id";
    process.env.APP_REGISTRATION_CLIENT_SECRET = "secret/client-secret";
    process.env.APP_REGISTRATION_TENANT_ID = "secret/tenant-id";

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
  });

  afterAll(() => {
    require.cache[sharedPath].exports = origSharedExports;
    require.cache[firestorePath].exports = origFirestoreExports;
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
      expect(saveFn.mock.calls.length).toBe(0);
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
