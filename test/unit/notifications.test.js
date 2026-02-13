const { expect } = require("chai");
const sinon = require("sinon");
const { SmsBatchSender } = require("../../utils/notifications");

const sharedModule = require("../../utils/shared");
const firestoreModule = require("../../utils/firestore");

describe("Notifications Unit Tests", () => {
  let notificationsModule;

  const createResponseMock = () => ({
    status: sinon.stub().returnsThis(),
    json: sinon.stub().returnsThis(),
  });

  const createFetchResponse = ({
    ok = true,
    status = 200,
    headers = {},
  } = {}) => ({
    ok,
    status,
    headers: {
      get: (name) => headers[name] || null,
    },
  });

  beforeEach(() => {
    delete require.cache[require.resolve("../../utils/notifications")];

    process.env.APP_REGISTRATION_CLIENT_ID = "secret/client-id";
    process.env.APP_REGISTRATION_CLIENT_SECRET = "secret/client-secret";
    process.env.APP_REGISTRATION_TENANT_ID = "secret/tenant-id";

    sinon
      .stub(sharedModule, "getResponseJSON")
      .callsFake((message, code) => ({ message, code }));
    sinon
      .stub(sharedModule, "validEmailFormat")
      .value(/^[^@\s]+@[^@\s]+\.[^@\s]+$/);
    sinon
      .stub(sharedModule, "getTemplateForEmailLink")
      .returns("<p>email body</p>");
    sinon.stub(sharedModule, "getSecret");
    sinon.stub(sharedModule, "parseResponseJson");
    sinon.stub(sharedModule, "safeJSONParse").callsFake((str) => JSON.parse(str));

    sinon.stub(firestoreModule, "generateSignInWithEmailLink");

    global.fetch = sinon.stub();

    notificationsModule = require("../../utils/notifications");
  });

  afterEach(() => {
    sinon.restore();
    delete global.fetch;
    delete require.cache[require.resolve("../../utils/notifications")];
    delete process.env.APP_REGISTRATION_CLIENT_ID;
    delete process.env.APP_REGISTRATION_CLIENT_SECRET;
    delete process.env.APP_REGISTRATION_TENANT_ID;
  });

  describe("sendEmailLink", () => {
    it("should return 405 for non-POST requests", async () => {
      const req = { method: "GET" };
      const res = createResponseMock();

      await notificationsModule.sendEmailLink(req, res);

      expect(res.status.calledWith(405)).to.be.true;
      expect(res.json.firstCall.args[0].code).to.equal(405);
    });

    it("should return 400 when email is missing", async () => {
      const req = {
        method: "POST",
        body: { continueUrl: "https://example.com" },
      };
      const res = createResponseMock();

      await notificationsModule.sendEmailLink(req, res);

      expect(res.status.calledWith(400)).to.be.true;
      const payload = res.json.firstCall.args[0];
      expect(payload.errorCode).to.equal("auth/missing-email");
      expect(payload.code).to.equal(400);
      expect(payload.authFlowId).to.be.a("string");
      expect(payload.authAttemptId).to.be.a("string");
    });

    it("should return 400 when continueUrl is missing", async () => {
      const req = {
        method: "POST",
        body: { email: "user@example.com" },
      };
      const res = createResponseMock();

      await notificationsModule.sendEmailLink(req, res);

      expect(res.status.calledWith(400)).to.be.true;
      const payload = res.json.firstCall.args[0];
      expect(payload.errorCode).to.equal("auth/missing-continue-uri");
      expect(payload.code).to.equal(400);
    });

    it("should return 400 when email format is invalid", async () => {
      const req = {
        method: "POST",
        body: { email: "bad-email", continueUrl: "https://example.com" },
      };
      const res = createResponseMock();

      await notificationsModule.sendEmailLink(req, res);

      expect(res.status.calledWith(400)).to.be.true;
      const payload = res.json.firstCall.args[0];
      expect(payload.errorCode).to.equal("auth/invalid-email");
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

      sharedModule.getSecret
        .withArgs(process.env.APP_REGISTRATION_CLIENT_ID)
        .resolves("client-id")
        .withArgs(process.env.APP_REGISTRATION_CLIENT_SECRET)
        .resolves("client-secret")
        .withArgs(process.env.APP_REGISTRATION_TENANT_ID)
        .resolves("tenant-id");

      firestoreModule.generateSignInWithEmailLink.resolves(
        "https://auth.example.com/?continueUrl=https://app.example.com&lang=en",
      );

      sharedModule.parseResponseJson
        .onFirstCall()
        .resolves({ access_token: "graph-access-token" });
      sharedModule.parseResponseJson.onSecondCall().resolves(null);

      global.fetch.onFirstCall().resolves(createFetchResponse({ ok: true, status: 200 }));
      global.fetch.onSecondCall().resolves(
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

      expect(global.fetch.calledTwice).to.be.true;
      const graphCall = global.fetch.secondCall;
      expect(graphCall.args[0]).to.include(
        "https://graph.microsoft.com/v1.0/users/",
      );
      expect(graphCall.args[1].headers.Authorization).to.equal(
        "Bearer graph-access-token",
      );
      expect(graphCall.args[1].headers["return-client-request-id"]).to.equal(
        "true",
      );
      expect(graphCall.args[1].headers["client-request-id"]).to.be.a("string");

      expect(res.status.calledWith(202)).to.be.true;
      const payload = res.json.firstCall.args[0];
      expect(payload.code).to.equal(202);
      expect(payload.errorCode).to.equal(null);
      expect(payload.provider).to.equal("microsoft_graph");
      expect(payload.providerStatus).to.equal("accepted");
      expect(payload.graphRequestId).to.equal("graph-request-123");
      expect(payload.graphClientRequestId).to.equal("graph-client-123");
      expect(payload.authFlowId).to.equal("auth_flow_client_1");
      expect(payload.authAttemptId).to.equal("auth_attempt_client_1");
      expect(payload.clientSendTs).to.equal("2026-02-12T10:00:00.000Z");
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

      sharedModule.getSecret
        .withArgs(process.env.APP_REGISTRATION_CLIENT_ID)
        .resolves("client-id")
        .withArgs(process.env.APP_REGISTRATION_CLIENT_SECRET)
        .resolves("client-secret")
        .withArgs(process.env.APP_REGISTRATION_TENANT_ID)
        .resolves("tenant-id");

      firestoreModule.generateSignInWithEmailLink.resolves(
        "https://auth.example.com/?continueUrl=https://app.example.com",
      );

      sharedModule.parseResponseJson
        .onFirstCall()
        .resolves({ access_token: "graph-access-token" });
      sharedModule.parseResponseJson.onSecondCall().resolves({
        error: {
          code: "InvalidRecipients",
          message: "Recipient is invalid",
        },
      });

      global.fetch.onFirstCall().resolves(createFetchResponse({ ok: true, status: 200 }));
      global.fetch.onSecondCall().resolves(
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

      expect(res.status.calledWith(400)).to.be.true;
      const payload = res.json.firstCall.args[0];
      expect(payload.errorCode).to.equal("auth/invalid-email");
      expect(payload.providerStatus).to.equal("failed");
      expect(payload.providerErrorCode).to.equal("InvalidRecipients");
      expect(payload.graphRequestId).to.equal("graph-request-400");
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

      sharedModule.getSecret
        .withArgs(process.env.APP_REGISTRATION_CLIENT_ID)
        .resolves("client-id")
        .withArgs(process.env.APP_REGISTRATION_CLIENT_SECRET)
        .resolves("client-secret")
        .withArgs(process.env.APP_REGISTRATION_TENANT_ID)
        .resolves("tenant-id");

      firestoreModule.generateSignInWithEmailLink.resolves(
        "https://auth.example.com/?continueUrl=https://app.example.com",
      );

      sharedModule.parseResponseJson
        .onFirstCall()
        .resolves({ access_token: "graph-access-token" });
      sharedModule.parseResponseJson.onSecondCall().resolves({
        error: {
          code: "MailboxNotEnabledForRESTAPI",
          message: "Mailbox is not enabled",
        },
      });

      global.fetch.onFirstCall().resolves(createFetchResponse({ ok: true, status: 200 }));
      global.fetch.onSecondCall().resolves(
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

      expect(res.status.calledWith(400)).to.be.true;
      const payload = res.json.firstCall.args[0];
      expect(payload.errorCode).to.equal("auth/operation-not-allowed");
      expect(payload.providerErrorCode).to.equal("MailboxNotEnabledForRESTAPI");
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

      sharedModule.getSecret
        .withArgs(process.env.APP_REGISTRATION_CLIENT_ID)
        .resolves("client-id")
        .withArgs(process.env.APP_REGISTRATION_CLIENT_SECRET)
        .resolves("client-secret")
        .withArgs(process.env.APP_REGISTRATION_TENANT_ID)
        .resolves("tenant-id");

      firestoreModule.generateSignInWithEmailLink.resolves(
        "https://auth.example.com/?continueUrl=https://app.example.com",
      );

      sharedModule.parseResponseJson
        .onFirstCall()
        .resolves({ access_token: "graph-access-token" });
      sharedModule.parseResponseJson.onSecondCall().resolves({
        error: {
          code: "InvalidAuthenticationToken",
          message: "Access token is invalid",
        },
      });

      global.fetch.onFirstCall().resolves(createFetchResponse({ ok: true, status: 200 }));
      global.fetch.onSecondCall().resolves(
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

      expect(res.status.calledWith(401)).to.be.true;
      const payload = res.json.firstCall.args[0];
      expect(payload.code).to.equal(401);
      expect(payload.errorCode).to.equal("auth/operation-not-allowed");
      expect(payload.providerErrorCode).to.equal("InvalidAuthenticationToken");
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
      firestoreModule.generateSignInWithEmailLink.rejects(linkError);

      await notificationsModule.sendEmailLink(req, res);

      expect(res.status.calledWith(400)).to.be.true;
      const payload = res.json.firstCall.args[0];
      expect(payload.errorCode).to.equal("auth/invalid-continue-uri");
      expect(payload.code).to.equal(400);
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
      firestoreModule.generateSignInWithEmailLink.rejects(tooManyRequestsError);

      await notificationsModule.sendEmailLink(req, res);

      expect(res.status.calledWith(429)).to.be.true;
      const payload = res.json.firstCall.args[0];
      expect(payload.errorCode).to.equal("auth/too-many-requests");
      expect(payload.code).to.equal(429);
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

      sharedModule.getSecret
        .withArgs(process.env.APP_REGISTRATION_CLIENT_ID)
        .resolves("client-id")
        .withArgs(process.env.APP_REGISTRATION_CLIENT_SECRET)
        .resolves("client-secret")
        .withArgs(process.env.APP_REGISTRATION_TENANT_ID)
        .resolves("tenant-id");

      firestoreModule.generateSignInWithEmailLink.resolves(
        "https://auth.example.com/?continueUrl=https://app.example.com",
      );

      sharedModule.parseResponseJson
        .onFirstCall()
        .resolves({ error: { code: "invalid_client" } });
      global.fetch.onFirstCall().resolves(createFetchResponse({ ok: false, status: 401 }));

      await notificationsModule.sendEmailLink(req, res);

      expect(res.status.calledWith(502)).to.be.true;
      const payload = res.json.firstCall.args[0];
      expect(payload.code).to.equal(502);
      expect(payload.errorCode).to.equal("auth/operation-not-allowed");
      expect(payload.upstreamStatus).to.equal(401);
      expect(payload.providerStatus).to.equal("failed");
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
    sendFn = sinon.stub();
    saveFn = sinon.stub().resolves();
    delayFn = sinon.stub().resolves();
  });

  afterEach(() => {
    sinon.restore();
  });

  describe("getSentCounts / getFailedCounts", () => {
    it("should return zero counts for unknown specId", () => {
      const sender = createSender();
      expect(sender.getSentCounts("unknown")).to.deep.equal({
        english: 0,
        spanish: 0,
      });
      expect(sender.getFailedCounts("unknown")).to.deep.equal({
        english: 0,
        spanish: 0,
      });
    });

    it("should return a copy so callers cannot mutate internal state", () => {
      const sender = createSender();
      const counts = sender.getSentCounts("spec1");
      counts.english = 999;
      expect(sender.getSentCounts("spec1")).to.deep.equal({ english: 0, spanish: 0 });
    });
  });

  describe("isSpecFinished", () => {
    it("should return false for unknown specId", () => {
      const sender = createSender();
      expect(sender.isSpecFinished("spec1")).to.be.false;
    });
  });

  describe("basic send flow", () => {
    it("should send all records and track sent counts", async () => {
      sendFn.callsFake((record) => Promise.resolve(successResult(record)));

      const sender = createSender();
      const records = [
        makeSmsRecord("spec1", "english"),
        makeSmsRecord("spec1", "spanish"),
        makeSmsRecord("spec1", "english"),
      ];
      sender.addToQueue(records);
      sender.markSpecEnd("spec1");

      const result = await sender.waitForSpec("spec1");
      expect(result.sentCounts).to.deep.equal({ english: 2, spanish: 1 });
      expect(result.failedCounts).to.deep.equal({ english: 0, spanish: 0 });
      expect(sendFn.callCount).to.equal(3);
      expect(saveFn.callCount).to.equal(1);
      expect(saveFn.firstCall.args[0]).to.have.lengthOf(3);
    });

    it("should handle an empty queue with only an end marker", async () => {
      const sender = createSender();
      sender.markSpecEnd("spec1");

      const result = await sender.waitForSpec("spec1");
      expect(result.sentCounts).to.deep.equal({ english: 0, spanish: 0 });
      expect(result.failedCounts).to.deep.equal({ english: 0, spanish: 0 });
      expect(sendFn.callCount).to.equal(0);
      expect(saveFn.callCount).to.equal(0);
    });
  });

  describe("failure handling", () => {
    it("should track failed counts for non-rate-limit errors", async () => {
      sendFn.callsFake((record) => Promise.resolve(failResult(record)));

      const sender = createSender();
      sender.addToQueue([
        makeSmsRecord("spec1", "english"),
        makeSmsRecord("spec1", "spanish"),
      ]);
      sender.markSpecEnd("spec1");

      const result = await sender.waitForSpec("spec1");
      expect(result.sentCounts).to.deep.equal({ english: 0, spanish: 0 });
      expect(result.failedCounts).to.deep.equal({ english: 1, spanish: 1 });
      expect(saveFn.callCount).to.equal(0);
    });

    it("should not increment sent counts when saveNotificationBatch throws", async () => {
      sendFn.callsFake((record) => Promise.resolve(successResult(record)));
      saveFn.rejects(new Error("Firestore write failed"));

      const consoleStub = sinon.stub(console, "error");
      const sender = createSender();
      sender.addToQueue([makeSmsRecord("spec1", "english")]);
      sender.markSpecEnd("spec1");

      const result = await sender.waitForSpec("spec1");
      expect(result.sentCounts).to.deep.equal({ english: 0, spanish: 0 });
      expect(result.failedCounts).to.deep.equal({ english: 0, spanish: 0 });
      expect(consoleStub.calledOnce).to.be.true;
      consoleStub.restore();
    });
  });

  describe("rate limit retry", () => {
    it("should re-queue rate-limited records and retry them", async () => {
      const record = makeSmsRecord("spec1", "english");
      let callCount = 0;
      sendFn.callsFake((r) => {
        callCount++;
        if (callCount === 1) return Promise.resolve(rateLimitResult(r));
        return Promise.resolve(successResult(r));
      });

      const sender = createSender();
      sender.addToQueue([record]);
      sender.markSpecEnd("spec1");

      const result = await sender.waitForSpec("spec1");
      expect(result.sentCounts).to.deep.equal({ english: 1, spanish: 0 });
      expect(sendFn.callCount).to.equal(2);
    });

    it("should count as failed after exceeding maxRetries", async () => {
      sendFn.callsFake((r) => Promise.resolve(rateLimitResult(r)));
      const consoleStub = sinon.stub(console, "error");

      const sender = createSender({ batchSize: 10, maxRetries: 2 });
      sender.addToQueue([makeSmsRecord("spec1", "english")]);
      sender.markSpecEnd("spec1");

      const result = await sender.waitForSpec("spec1");
      expect(result.sentCounts).to.deep.equal({ english: 0, spanish: 0 });
      expect(result.failedCounts).to.deep.equal({ english: 1, spanish: 0 });
      // 1 initial + 2 retries = 3 total send attempts
      expect(sendFn.callCount).to.equal(3);
      consoleStub.restore();
    });

    it("should succeed on retry within maxRetries limit", async () => {
      let callCount = 0;
      sendFn.callsFake((r) => {
        callCount++;
        if (callCount <= 2) return Promise.resolve(rateLimitResult(r));
        return Promise.resolve(successResult(r));
      });

      const sender = createSender({ batchSize: 10, maxRetries: 3 });
      sender.addToQueue([makeSmsRecord("spec1", "english")]);
      sender.markSpecEnd("spec1");

      const result = await sender.waitForSpec("spec1");
      expect(result.sentCounts).to.deep.equal({ english: 1, spanish: 0 });
      expect(result.failedCounts).to.deep.equal({ english: 0, spanish: 0 });
    });

    it("should preserve end marker when rate-limited records are re-queued", async () => {
      let callCount = 0;
      sendFn.callsFake((r) => {
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
      expect(result.sentCounts).to.deep.equal({ english: 1, spanish: 1 });
      expect(sender.isSpecFinished("spec1")).to.be.true;
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
      sendFn.callsFake((r) => {
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
      expect(result.sentCounts).to.deep.equal({ english: 2, spanish: 0 });
      expect(result.failedCounts).to.deep.equal({ english: 0, spanish: 1 });
    });
  });

  describe("batching", () => {
    it("should respect batchSize and process in multiple batches", async () => {
      sendFn.callsFake((r) => Promise.resolve(successResult(r)));

      const sender = createSender({ batchSize: 2 });
      const records = [
        makeSmsRecord("spec1", "english", "a"),
        makeSmsRecord("spec1", "english", "b"),
        makeSmsRecord("spec1", "english", "c"),
      ];
      sender.addToQueue(records);
      sender.markSpecEnd("spec1");

      const result = await sender.waitForSpec("spec1");
      expect(result.sentCounts).to.deep.equal({ english: 3, spanish: 0 });
      // saveFn called once per batch that has successful records
      expect(saveFn.callCount).to.equal(2);
    });
  });

  describe("multiple specs", () => {
    it("should track counts independently per specId", async () => {
      sendFn.callsFake((r) => {
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

      expect(result1.sentCounts).to.deep.equal({ english: 1, spanish: 1 });
      expect(result1.failedCounts).to.deep.equal({ english: 0, spanish: 0 });
      expect(result2.sentCounts).to.deep.equal({ english: 0, spanish: 0 });
      expect(result2.failedCounts).to.deep.equal({ english: 1, spanish: 0 });
    });

    it("should finish specs independently", async () => {
      sendFn.callsFake((r) => Promise.resolve(successResult(r)));

      const sender = createSender();
      sender.addToQueue([makeSmsRecord("spec1", "english")]);
      sender.markSpecEnd("spec1");

      await sender.waitForSpec("spec1");
      expect(sender.isSpecFinished("spec1")).to.be.true;
      expect(sender.isSpecFinished("spec2")).to.be.false;
    });
  });

  describe("progress logging", () => {
    let clock;
    let consoleLogStub;

    beforeEach(() => {
      clock = sinon.useFakeTimers({ now: Date.now() });
      consoleLogStub = sinon.stub(console, "log");
    });

    afterEach(() => {
      clock.restore();
      consoleLogStub.restore();
    });

    it("should log progress for in-flight specs after 30 seconds", async () => {
      sendFn.callsFake((r) => Promise.resolve(successResult(r)));

      const sender = createSender({ batchSize: 1 });
      sender.addToQueue([
        makeSmsRecord("spec1", "english", "a"),
        makeSmsRecord("spec1", "english", "b"),
      ]);
      sender.markSpecEnd("spec1");

      clock.tick(31000);
      await sender.waitForSpec("spec1");

      const progressLogs = consoleLogStub.args.filter((args) =>
        args[0]?.includes?.("SMS in progress"),
      );
      expect(progressLogs.length).to.be.greaterThan(0);
      expect(progressLogs[0][0]).to.include("spec1");
      expect(progressLogs[0][0]).to.include("sent");
      expect(progressLogs[0][0]).to.include("failed");
    });

    it("should skip finished specs in progress log", async () => {
      sendFn.callsFake((r) => Promise.resolve(successResult(r)));

      const sender = createSender();
      sender.addToQueue([makeSmsRecord("spec1", "english")]);
      sender.markSpecEnd("spec1");

      // spec1 finishes, then start spec2 after 30s
      await sender.waitForSpec("spec1");
      clock.tick(31000);

      sender.addToQueue([makeSmsRecord("spec2", "english")]);
      sender.markSpecEnd("spec2");
      await sender.waitForSpec("spec2");

      const progressLogs = consoleLogStub.args.filter((args) =>
        args[0]?.includes?.("SMS in progress"),
      );
      const spec1Logs = progressLogs.filter((args) => args[0].includes("spec1"));
      expect(spec1Logs.length).to.equal(0);
    });
  });

  describe("delay behavior", () => {
    it("should call delayFn for rate limiting between batches", async () => {
      sendFn.callsFake((r) => Promise.resolve(successResult(r)));

      const sender = createSender({ batchSize: 1 });
      sender.addToQueue([
        makeSmsRecord("spec1", "english", "a"),
        makeSmsRecord("spec1", "english", "b"),
      ]);
      sender.markSpecEnd("spec1");

      await sender.waitForSpec("spec1");
      expect(delayFn.called).to.be.true;
    });
  });
});
