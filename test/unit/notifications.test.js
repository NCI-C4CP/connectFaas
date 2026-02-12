const { expect } = require("chai");
const sinon = require("sinon");

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

    sinon.stub(sharedModule, "getResponseJSON").callsFake((message, code) => ({ message, code }));
    sinon.stub(sharedModule, "validEmailFormat").value(/^[^@\s]+@[^@\s]+\.[^@\s]+$/);
    sinon.stub(sharedModule, "getTemplateForEmailLink").returns("<p>email body</p>");
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
        .withArgs(process.env.APP_REGISTRATION_CLIENT_ID).resolves("client-id")
        .withArgs(process.env.APP_REGISTRATION_CLIENT_SECRET).resolves("client-secret")
        .withArgs(process.env.APP_REGISTRATION_TENANT_ID).resolves("tenant-id");

      firestoreModule.generateSignInWithEmailLink.resolves(
        "https://auth.example.com/?continueUrl=https://app.example.com&lang=en"
      );

      sharedModule.parseResponseJson.onFirstCall().resolves({ access_token: "graph-access-token" });
      sharedModule.parseResponseJson.onSecondCall().resolves(null);

      global.fetch.onFirstCall().resolves(
        createFetchResponse({ ok: true, status: 200 })
      );
      global.fetch.onSecondCall().resolves(
        createFetchResponse({
          ok: true,
          status: 202,
          headers: {
            "request-id": "graph-request-123",
            "client-request-id": "graph-client-123",
          },
        })
      );

      await notificationsModule.sendEmailLink(req, res);

      expect(global.fetch.calledTwice).to.be.true;
      const graphCall = global.fetch.secondCall;
      expect(graphCall.args[0]).to.include("https://graph.microsoft.com/v1.0/users/");
      expect(graphCall.args[1].headers.Authorization).to.equal("Bearer graph-access-token");
      expect(graphCall.args[1].headers["return-client-request-id"]).to.equal("true");
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
        .withArgs(process.env.APP_REGISTRATION_CLIENT_ID).resolves("client-id")
        .withArgs(process.env.APP_REGISTRATION_CLIENT_SECRET).resolves("client-secret")
        .withArgs(process.env.APP_REGISTRATION_TENANT_ID).resolves("tenant-id");

      firestoreModule.generateSignInWithEmailLink.resolves("https://auth.example.com/?continueUrl=https://app.example.com");

      sharedModule.parseResponseJson.onFirstCall().resolves({ access_token: "graph-access-token" });
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
        })
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
        .withArgs(process.env.APP_REGISTRATION_CLIENT_ID).resolves("client-id")
        .withArgs(process.env.APP_REGISTRATION_CLIENT_SECRET).resolves("client-secret")
        .withArgs(process.env.APP_REGISTRATION_TENANT_ID).resolves("tenant-id");

      firestoreModule.generateSignInWithEmailLink.resolves("https://auth.example.com/?continueUrl=https://app.example.com");

      sharedModule.parseResponseJson.onFirstCall().resolves({ access_token: "graph-access-token" });
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
        })
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
        .withArgs(process.env.APP_REGISTRATION_CLIENT_ID).resolves("client-id")
        .withArgs(process.env.APP_REGISTRATION_CLIENT_SECRET).resolves("client-secret")
        .withArgs(process.env.APP_REGISTRATION_TENANT_ID).resolves("tenant-id");

      firestoreModule.generateSignInWithEmailLink.resolves("https://auth.example.com/?continueUrl=https://app.example.com");

      sharedModule.parseResponseJson.onFirstCall().resolves({ access_token: "graph-access-token" });
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
        })
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
        .withArgs(process.env.APP_REGISTRATION_CLIENT_ID).resolves("client-id")
        .withArgs(process.env.APP_REGISTRATION_CLIENT_SECRET).resolves("client-secret")
        .withArgs(process.env.APP_REGISTRATION_TENANT_ID).resolves("tenant-id");

      firestoreModule.generateSignInWithEmailLink.resolves("https://auth.example.com/?continueUrl=https://app.example.com");

      sharedModule.parseResponseJson.onFirstCall().resolves({ error: { code: "invalid_client" } });
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

  describe("lookupEmailDeliveryStatus", () => {
    it("should return 405 for non-POST requests", async () => {
      const req = { method: "GET" };
      const res = createResponseMock();

      await notificationsModule.lookupEmailDeliveryStatus(req, res);

      expect(res.status.calledWith(405)).to.be.true;
      expect(res.json.firstCall.args[0].code).to.equal(405);
    });

    it("should return 400 for invalid recipient", async () => {
      const req = {
        method: "POST",
        body: { recipient: "bad-email" },
      };
      const res = createResponseMock();

      await notificationsModule.lookupEmailDeliveryStatus(req, res);

      expect(res.status.calledWith(400)).to.be.true;
      const payload = res.json.firstCall.args[0];
      expect(payload.errorCode).to.equal("invalid_recipient");
    });

    it("should return 400 for invalid datetime", async () => {
      const req = {
        method: "POST",
        body: {
          recipient: "user@example.com",
          startDateTime: "not-a-date",
          endDateTime: "2026-02-12T00:00:00.000Z",
        },
      };
      const res = createResponseMock();

      await notificationsModule.lookupEmailDeliveryStatus(req, res);

      expect(res.status.calledWith(400)).to.be.true;
      expect(res.json.firstCall.args[0].errorCode).to.equal("invalid_datetime");
    });

    it("should return 400 for invalid date window", async () => {
      const req = {
        method: "POST",
        body: {
          recipient: "user@example.com",
          startDateTime: "2025-01-01T00:00:00.000Z",
          endDateTime: "2026-02-12T00:00:00.000Z",
        },
      };
      const res = createResponseMock();

      await notificationsModule.lookupEmailDeliveryStatus(req, res);

      expect(res.status.calledWith(400)).to.be.true;
      expect(res.json.firstCall.args[0].errorCode).to.equal("invalid_trace_window");
    });

    it("should return traces and metadata on success", async () => {
      const req = {
        method: "POST",
        body: {
          recipient: "user@example.com",
          startDateTime: "2026-02-10T00:00:00.000Z",
          endDateTime: "2026-02-12T00:00:00.000Z",
          subjectContains: "Connect",
          authAttemptId: "auth_attempt_client_7",
        },
      };
      const res = createResponseMock();

      sharedModule.getSecret
        .withArgs(process.env.APP_REGISTRATION_CLIENT_ID).resolves("client-id")
        .withArgs(process.env.APP_REGISTRATION_CLIENT_SECRET).resolves("client-secret")
        .withArgs(process.env.APP_REGISTRATION_TENANT_ID).resolves("tenant-id");

      sharedModule.parseResponseJson.onFirstCall().resolves({ access_token: "graph-access-token" });
      sharedModule.parseResponseJson.onSecondCall().resolves({
        value: [{ id: "trace-1" }, { id: "trace-2" }],
        "@odata.nextLink": "https://graph.microsoft.com/beta/admin/exchange/tracing/messageTraces?$skiptoken=abc",
      });

      global.fetch.onFirstCall().resolves(createFetchResponse({ ok: true, status: 200 }));
      global.fetch.onSecondCall().resolves(
        createFetchResponse({
          ok: true,
          status: 200,
          headers: {
            "request-id": "trace-request-200",
            "client-request-id": "trace-client-200",
          },
        })
      );

      await notificationsModule.lookupEmailDeliveryStatus(req, res);

      expect(global.fetch.calledTwice).to.be.true;
      const traceUrl = new URL(global.fetch.secondCall.args[0]);
      expect(traceUrl.pathname).to.equal("/beta/admin/exchange/tracing/messageTraces");
      const filterClause = traceUrl.searchParams.get("$filter");
      expect(filterClause).to.include("recipientAddress eq 'user@example.com'");

      expect(res.status.calledWith(200)).to.be.true;
      const payload = res.json.firstCall.args[0];
      expect(payload.code).to.equal(200);
      expect(payload.errorCode).to.equal(null);
      expect(payload.count).to.equal(2);
      expect(payload.authAttemptId).to.equal("auth_attempt_client_7");
      expect(payload.graphRequestId).to.equal("trace-request-200");
      expect(payload.graphClientRequestId).to.equal("trace-client-200");
      expect(payload.data).to.have.length(2);
    });

    it("should return message_trace_lookup_failed when Graph trace fails", async () => {
      const req = {
        method: "POST",
        body: {
          recipient: "user@example.com",
          startDateTime: "2026-02-10T00:00:00.000Z",
          endDateTime: "2026-02-11T00:00:00.000Z",
        },
      };
      const res = createResponseMock();

      sharedModule.getSecret
        .withArgs(process.env.APP_REGISTRATION_CLIENT_ID).resolves("client-id")
        .withArgs(process.env.APP_REGISTRATION_CLIENT_SECRET).resolves("client-secret")
        .withArgs(process.env.APP_REGISTRATION_TENANT_ID).resolves("tenant-id");

      sharedModule.parseResponseJson.onFirstCall().resolves({ access_token: "graph-access-token" });
      sharedModule.parseResponseJson.onSecondCall().resolves({
        error: { code: "TooManyRequests", message: "Rate limited" },
      });

      global.fetch.onFirstCall().resolves(createFetchResponse({ ok: true, status: 200 }));
      global.fetch.onSecondCall().resolves(
        createFetchResponse({
          ok: false,
          status: 429,
          headers: {
            "request-id": "trace-request-429",
            "client-request-id": "trace-client-429",
          },
        })
      );

      await notificationsModule.lookupEmailDeliveryStatus(req, res);

      expect(res.status.calledWith(429)).to.be.true;
      const payload = res.json.firstCall.args[0];
      expect(payload.errorCode).to.equal("message_trace_lookup_failed");
      expect(payload.providerErrorCode).to.equal("TooManyRequests");
      expect(payload.graphRequestId).to.equal("trace-request-429");
    });

    it("should return 502 when Graph token acquisition fails in lookup", async () => {
      const req = {
        method: "POST",
        body: {
          recipient: "user@example.com",
          startDateTime: "2026-02-10T00:00:00.000Z",
          endDateTime: "2026-02-11T00:00:00.000Z",
        },
      };
      const res = createResponseMock();

      sharedModule.getSecret
        .withArgs(process.env.APP_REGISTRATION_CLIENT_ID).resolves("client-id")
        .withArgs(process.env.APP_REGISTRATION_CLIENT_SECRET).resolves("client-secret")
        .withArgs(process.env.APP_REGISTRATION_TENANT_ID).resolves("tenant-id");

      sharedModule.parseResponseJson.onFirstCall().resolves({});
      global.fetch.onFirstCall().resolves(createFetchResponse({ ok: true, status: 200 }));

      await notificationsModule.lookupEmailDeliveryStatus(req, res);

      expect(res.status.calledWith(502)).to.be.true;
      const payload = res.json.firstCall.args[0];
      expect(payload.code).to.equal(502);
      expect(payload.errorCode).to.equal("auth/operation-not-allowed");
      expect(global.fetch.calledOnce).to.be.true;
    });
  });
});
