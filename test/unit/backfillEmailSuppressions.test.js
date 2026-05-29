const {
  SENDGRID_API_BASE_URL,
  resolveSendGridSuppressionType,
  getSendGridApiBaseUrl,
  getSendGridSuppressionRequestConfig,
  normalizePageLimit,
  buildSendGridRequestUrl,
  buildSendGridRequestHeaders,
  requestSendGridJson,
  fetchSendGridSuppressionRows,
  extractSendGridSuppressionEmail,
  classifySuppressionRow,
  classifySendGridSuppressionRows,
  lookupParticipantTokensByEmail,
  buildSuppressionDocs,
} = require("../../scripts/backfillEmailSuppressions");

const createJsonResponse = (body, status = 200) => ({
  ok: status >= 200 && status < 300,
  status,
  text: vi.fn().mockResolvedValue(typeof body === "string" ? body : JSON.stringify(body)),
});

describe("backfillEmailSuppressions", () => {
  describe("SendGrid API config", () => {
    it("should resolve exact supported SendGrid suppression types", () => {
      expect(resolveSendGridSuppressionType("bounces")).toBe("bounces");
      expect(resolveSendGridSuppressionType("spam_reports")).toBe("spam_reports");
      expect(resolveSendGridSuppressionType("invalid_emails")).toBe("invalid_emails");
      expect(resolveSendGridSuppressionType("global_unsubscribes")).toBe("global_unsubscribes");
      expect(resolveSendGridSuppressionType("legacy_global_unsubscribes")).toBe("legacy_global_unsubscribes");
      expect(resolveSendGridSuppressionType("group_unsubscribes")).toBe("group_unsubscribes");
      expect(resolveSendGridSuppressionType(" BOUNCES ")).toBe("bounces");
      expect(resolveSendGridSuppressionType("bounce")).toBe("");
      expect(resolveSendGridSuppressionType("hard_bounce")).toBe("");
      expect(resolveSendGridSuppressionType("spam-report")).toBe("");
      expect(resolveSendGridSuppressionType("invalid email")).toBe("");
      expect(resolveSendGridSuppressionType("unsubscribe")).toBe("");
      expect(resolveSendGridSuppressionType("legacy_global_unsubscribe")).toBe("");
      expect(resolveSendGridSuppressionType("group_unsubscribe")).toBe("");
      expect(resolveSendGridSuppressionType("block")).toBe("");
      expect(resolveSendGridSuppressionType("unknown")).toBe("");
    });

    it("should use the US SendGrid API base URL unless explicitly overridden", () => {
      expect(getSendGridApiBaseUrl()).toBe(SENDGRID_API_BASE_URL);
      expect(getSendGridApiBaseUrl({ apiBaseUrl: "https://test.sendgrid.local" })).toBe("https://test.sendgrid.local");
    });

    it("should map paginated suppression types to SendGrid endpoints", () => {
      expect(getSendGridSuppressionRequestConfig({ type: "legacy_global_unsubscribes" })).toEqual({
        type: "legacy_global_unsubscribes",
        endpoint: "/v3/suppression/unsubscribes",
        paginated: true,
        supportsTimeRange: false,
      });
      expect(getSendGridSuppressionRequestConfig({ type: "bounces" }).endpoint).toBe("/v3/suppression/bounces");
      expect(getSendGridSuppressionRequestConfig({ type: "invalid_emails" }).endpoint).toBe("/v3/suppression/invalid_emails");
      expect(getSendGridSuppressionRequestConfig({ type: "spam_reports" }).endpoint).toBe("/v3/suppression/spam_reports");
      expect(getSendGridSuppressionRequestConfig({ type: "unknown" })).toBeNull();
    });

    it("should require a group id for ASM group suppression endpoints", () => {
      expect(getSendGridSuppressionRequestConfig({ type: "group_unsubscribes" })).toBeNull();
      expect(getSendGridSuppressionRequestConfig({ type: "group_unsubscribes", groupId: "22391" })).toEqual({
        type: "group_unsubscribes",
        endpoint: "/v3/asm/groups/22391/suppressions",
        paginated: false,
        supportsTimeRange: false,
      });
    });

    it("should normalize SendGrid API page limits into the documented range", () => {
      expect(normalizePageLimit()).toBe(500);
      expect(normalizePageLimit("50")).toBe(50);
      expect(normalizePageLimit("0")).toBe(1);
      expect(normalizePageLimit("999")).toBe(500);
      expect(normalizePageLimit("not-a-number")).toBe(500);
    });

    it("should build SendGrid request URLs and headers", () => {
      const url = buildSendGridRequestUrl({
        apiBaseUrl: "https://api.sendgrid.test",
        endpoint: "/v3/suppression/bounces",
        query: { limit: 250, offset: 500, start_time: 1710000000 },
      });
      const parsedUrl = new URL(url);

      expect(parsedUrl.origin).toBe("https://api.sendgrid.test");
      expect(parsedUrl.pathname).toBe("/v3/suppression/bounces");
      expect(parsedUrl.searchParams.get("limit")).toBe("250");
      expect(parsedUrl.searchParams.get("offset")).toBe("500");
      expect(parsedUrl.searchParams.get("start_time")).toBe("1710000000");
      expect(buildSendGridRequestHeaders({ apiKey: "sg-key", onBehalfOf: "subuser-a" })).toEqual({
        Accept: "application/json",
        Authorization: "Bearer sg-key",
        "on-behalf-of": "subuser-a",
      });
    });
  });

  describe("SendGrid API requests", () => {
    it("should request JSON with bearer auth", async () => {
      const fetchImpl = vi.fn().mockResolvedValue(createJsonResponse([{ email: "a@b.com" }]));

      const result = await requestSendGridJson({
        apiKey: "sg-key",
        endpoint: "/v3/suppression/bounces",
        query: { limit: 1, offset: 0 },
        onBehalfOf: "subuser-a",
        fetchImpl,
      });

      expect(result).toEqual([{ email: "a@b.com" }]);
      expect(fetchImpl).toHaveBeenCalledTimes(1);
      const [url, options] = fetchImpl.mock.calls[0];
      expect(new URL(url).pathname).toBe("/v3/suppression/bounces");
      expect(options).toMatchObject({
        method: "GET",
        headers: {
          Accept: "application/json",
          Authorization: "Bearer sg-key",
          "on-behalf-of": "subuser-a",
        },
      });
    });

    it("should throw a useful error for failed SendGrid API responses", async () => {
      const fetchImpl = vi.fn().mockResolvedValue(createJsonResponse({ errors: [{ message: "bad key" }] }, 401));

      await expect(requestSendGridJson({
        apiKey: "sg-key",
        endpoint: "/v3/suppression/bounces",
        fetchImpl,
      })).rejects.toMatchObject({
        message: expect.stringContaining("SendGrid suppression API request failed (401)"),
        status: 401,
        endpoint: "/v3/suppression/bounces",
      });
    });

    it("should reject non-JSON SendGrid API responses", async () => {
      const fetchImpl = vi.fn().mockResolvedValue(createJsonResponse("not-json"));

      await expect(requestSendGridJson({
        apiKey: "sg-key",
        endpoint: "/v3/suppression/bounces",
        fetchImpl,
      })).rejects.toThrow("non-JSON response");
    });
  });

  describe("fetchSendGridSuppressionRows", () => {
    it("should page through limit/offset SendGrid suppression APIs", async () => {
      const fetchImpl = vi
        .fn()
        .mockResolvedValueOnce(createJsonResponse([
          { email: "one@example.com" },
          { email: "two@example.com" },
        ]))
        .mockResolvedValueOnce(createJsonResponse([
          { email: "three@example.com" },
        ]));
      const onPage = vi.fn();

      const rows = await fetchSendGridSuppressionRows({
        type: "bounces",
        apiKey: "sg-key",
        apiBaseUrl: "https://api.sendgrid.test",
        pageLimit: 2,
        startTime: 1710000000,
        endTime: 1710003600,
        fetchImpl,
        onPage,
      });

      expect(rows.map((row) => row.email)).toEqual(["one@example.com", "two@example.com", "three@example.com"]);
      expect(fetchImpl).toHaveBeenCalledTimes(2);

      const firstUrl = new URL(fetchImpl.mock.calls[0][0]);
      expect(firstUrl.pathname).toBe("/v3/suppression/bounces");
      expect(firstUrl.searchParams.get("limit")).toBe("2");
      expect(firstUrl.searchParams.get("offset")).toBe("0");
      expect(firstUrl.searchParams.get("start_time")).toBe("1710000000");
      expect(firstUrl.searchParams.get("end_time")).toBe("1710003600");

      const secondUrl = new URL(fetchImpl.mock.calls[1][0]);
      expect(secondUrl.searchParams.get("offset")).toBe("2");
      expect(onPage).toHaveBeenNthCalledWith(1, {
        endpoint: "/v3/suppression/bounces",
        offset: 0,
        rows: 2,
        totalRows: 2,
      });
      expect(onPage).toHaveBeenNthCalledWith(2, {
        endpoint: "/v3/suppression/bounces",
        offset: 2,
        rows: 1,
        totalRows: 3,
      });
    });

    it("should not add time filters to global unsubscribe requests", async () => {
      const fetchImpl = vi.fn().mockResolvedValue(createJsonResponse([{ email: "one@example.com" }]));

      await fetchSendGridSuppressionRows({
        type: "global_unsubscribes",
        apiKey: "sg-key",
        pageLimit: 500,
        startTime: 1710000000,
        endTime: 1710003600,
        fetchImpl,
      });

      const requestUrl = new URL(fetchImpl.mock.calls[0][0]);
      expect(requestUrl.pathname).toBe("/v3/suppression/unsubscribes");
      expect(requestUrl.searchParams.get("limit")).toBe("500");
      expect(requestUrl.searchParams.get("offset")).toBe("0");
      expect(requestUrl.searchParams.has("start_time")).toBe(false);
      expect(requestUrl.searchParams.has("end_time")).toBe(false);
    });

    it("should reject ASM group suppressions without an explicit group id", async () => {
      const fetchImpl = vi.fn();

      await expect(fetchSendGridSuppressionRows({
        type: "group_unsubscribes",
        apiKey: "sg-key",
        fetchImpl,
      })).rejects.toThrow("requires groupId");

      expect(fetchImpl).not.toHaveBeenCalled();
    });

    it("should fetch a specific ASM group suppression list as email strings", async () => {
      const fetchImpl = vi.fn().mockResolvedValue(createJsonResponse([
        "one@example.com",
        "two@example.com",
      ]));

      const rows = await fetchSendGridSuppressionRows({
        type: "group_unsubscribes",
        groupId: "22391",
        apiKey: "sg-key",
        fetchImpl,
      });

      expect(rows).toEqual(["one@example.com", "two@example.com"]);
      expect(new URL(fetchImpl.mock.calls[0][0]).pathname).toBe("/v3/asm/groups/22391/suppressions");
      expect(fetchImpl).toHaveBeenCalledTimes(1);
    });

    it("should reject undocumented SendGrid response container shapes", async () => {
      const fetchImpl = vi.fn().mockResolvedValue(createJsonResponse({
        results: [{ email: "one@example.com" }],
      }));

      await expect(fetchSendGridSuppressionRows({
        type: "bounces",
        apiKey: "sg-key",
        fetchImpl,
      })).rejects.toThrow("Unexpected SendGrid suppression API response shape");
    });
  });

  describe("classifySuppressionRow", () => {
    it("should classify hard bounce as suppressBulk:true, suppressTransactional:true", () => {
      const result = classifySuppressionRow({ email: "a@b.com", type: "bounces" });
      expect(result).toEqual({
        email: "a@b.com",
        reason: "hard_bounce",
        suppressBulk: true,
        suppressTransactional: true,
      });
    });

    it("should classify spam_report as suppressBulk:true, suppressTransactional:true", () => {
      const result = classifySuppressionRow({ email: "a@b.com", type: "spam_reports" });
      expect(result).toEqual({
        email: "a@b.com",
        reason: "spam_report",
        suppressBulk: true,
        suppressTransactional: true,
      });
    });

    it("should classify SendGrid-native plural suppression labels", () => {
      expect(classifySuppressionRow({ email: "a@b.com", type: "spam_reports" })).toEqual({
        email: "a@b.com",
        reason: "spam_report",
        suppressBulk: true,
        suppressTransactional: true,
      });
      expect(classifySuppressionRow({ email: "a@b.com", type: "invalid_emails" })).toEqual({
        email: "a@b.com",
        reason: "invalid_email",
        suppressBulk: true,
        suppressTransactional: true,
      });
      expect(classifySuppressionRow({ email: "a@b.com", type: "global_unsubscribes" })).toEqual({
        email: "a@b.com",
        reason: "global_unsubscribe",
        suppressBulk: true,
        suppressTransactional: true,
      });
      expect(classifySuppressionRow({ email: "a@b.com", type: "legacy_global_unsubscribes" })).toEqual({
        email: "a@b.com",
        reason: "legacy_global_unsubscribe",
        suppressBulk: true,
        suppressTransactional: true,
      });
      expect(classifySuppressionRow({ email: "a@b.com", type: "group_unsubscribes" })).toEqual({
        email: "a@b.com",
        reason: "unsubscribed",
        suppressBulk: true,
        suppressTransactional: false,
      });
    });

    it("should classify invalid as suppressBulk:true, suppressTransactional:true", () => {
      const result = classifySuppressionRow({ email: "a@b.com", type: "invalid_emails" });
      expect(result).toEqual({
        email: "a@b.com",
        reason: "invalid_email",
        suppressBulk: true,
        suppressTransactional: true,
      });
    });

    it("should classify unsubscribe as suppressBulk:true, suppressTransactional:true", () => {
      const result = classifySuppressionRow({ email: "a@b.com", type: "global_unsubscribes" });
      expect(result).toEqual({
        email: "a@b.com",
        reason: "global_unsubscribe",
        suppressBulk: true,
        suppressTransactional: true,
      });
    });

    it("should not classify aliases or SendGrid blocks as supported backfill types", () => {
      expect(classifySuppressionRow({ email: "a@b.com", type: "bounce" })).toBeNull();
      expect(classifySuppressionRow({ email: "a@b.com", type: "unsubscribe" })).toBeNull();
      expect(classifySuppressionRow({ email: "a@b.com", type: "block" })).toBeNull();
    });

    it("should normalize email to lowercase", () => {
      const result = classifySuppressionRow({ email: " User@Example.COM ", type: "bounces" });
      expect(result.email).toBe("user@example.com");
    });

    it("should return null for unknown type", () => {
      const result = classifySuppressionRow({ email: "a@b.com", type: "unknown_thing" });
      expect(result).toBeNull();
    });
  });

  describe("classifySendGridSuppressionRows", () => {
    it("should classify API object and string rows and skip malformed rows", () => {
      const result = classifySendGridSuppressionRows([
        { email: " User@Example.COM " },
        "second@example.com",
        { email: "missing-at-sign" },
        {},
      ], "bounces");

      expect(result).toEqual({
        classified: [
          {
            email: "user@example.com",
            reason: "hard_bounce",
            suppressBulk: true,
            suppressTransactional: true,
          },
          {
            email: "second@example.com",
            reason: "hard_bounce",
            suppressBulk: true,
            suppressTransactional: true,
          },
        ],
        skipped: 2,
      });
    });

    it("should extract normalized emails from supported API row shapes", () => {
      expect(extractSendGridSuppressionEmail(" User@Example.COM ")).toBe("user@example.com");
      expect(extractSendGridSuppressionEmail({ email: "Other@Example.COM" })).toBe("other@example.com");
      expect(extractSendGridSuppressionEmail({})).toBe("");
      expect(extractSendGridSuppressionEmail(null)).toBe("");
    });
  });

  describe("buildSuppressionDocs", () => {
    it("should build Firestore doc data from classified rows", () => {
      const rows = [
        { email: "a@b.com", reason: "hard_bounce", suppressBulk: true, suppressTransactional: true },
        { email: "c@d.com", reason: "global_unsubscribe", suppressBulk: true, suppressTransactional: true },
      ];
      const docs = buildSuppressionDocs(rows);
      expect(docs).toHaveLength(2);
      expect(docs[0]).toMatchObject({
        normalizedEmail: "a@b.com",
        status: "suppressed",
        reason: "hard_bounce",
        suppressBulk: true,
        suppressTransactional: true,
        manualOverride: false,
      });
      expect(docs[0].lastEventAt).toBeDefined();
      expect(docs[1].normalizedEmail).toBe("c@d.com");
      expect(docs[1].suppressTransactional).toBe(true);
    });

    it("should include linked participant tokens when provided", () => {
      const rows = [
        { email: "a@b.com", reason: "hard_bounce", suppressBulk: true, suppressTransactional: true },
      ];
      const docs = buildSuppressionDocs(rows, new Map([["a@b.com", "tok-123"]]));

      expect(docs[0].token).toBe("tok-123");
    });

    it("should deduplicate by email, keeping last occurrence", () => {
      const rows = [
        { email: "a@b.com", reason: "telemetry_only", suppressBulk: false, suppressTransactional: false },
        { email: "a@b.com", reason: "hard_bounce", suppressBulk: true, suppressTransactional: true },
      ];
      const docs = buildSuppressionDocs(rows);
      expect(docs).toHaveLength(1);
      expect(docs[0].reason).toBe("hard_bounce");
    });

    it("should omit false suppression flags so backfills cannot clear an earlier suppression", () => {
      const rows = [
        { email: "a@b.com", reason: "telemetry_only", suppressBulk: false, suppressTransactional: false },
      ];
      const docs = buildSuppressionDocs(rows);
      expect(docs[0]).not.toHaveProperty("suppressBulk");
      expect(docs[0]).not.toHaveProperty("suppressTransactional");
    });

    it("should return empty array for empty input", () => {
      expect(buildSuppressionDocs([])).toEqual([]);
    });

    it("should drop filtered internal or malformed addresses from built docs", () => {
      const rows = [
        { email: "noreply@nih.gov", reason: "hard_bounce", suppressBulk: true, suppressTransactional: true },
        { email: "bad@domain.com.com", reason: "global_unsubscribe", suppressBulk: true, suppressTransactional: true },
      ];

      expect(buildSuppressionDocs(rows)).toEqual([]);
    });
  });

  describe("lookupParticipantTokensByEmail", () => {
    const createDbMock = (handlers = {}) => ({
      collection: vi.fn().mockImplementation((collectionName) => {
        if (collectionName !== "participants") {
          throw new Error(`Unexpected collection: ${collectionName}`);
        }

        return {
          where: vi.fn().mockImplementation((field, op, value) => {
            const response = handlers[value] || { empty: true, size: 0, docs: [] };
            return {
              select: vi.fn().mockReturnValue({
                limit: vi.fn().mockReturnValue({
                  get: vi.fn().mockResolvedValue(response),
                }),
              }),
            };
          }),
        };
      }),
    });

    it("should link tokens for uniquely matched participant emails", async () => {
      const db = createDbMock({
        "user@example.com": {
          empty: false,
          size: 1,
          docs: [{ data: () => ({ token: "tok-123" }) }],
        },
      });

      const result = await lookupParticipantTokensByEmail(db, ["  User@Example.COM  "]);

      expect(result.tokenByEmail.get("user@example.com")).toBe("tok-123");
      expect(result.matchedEmails).toEqual(["user@example.com"]);
      expect(result.unmatchedEmails).toEqual([]);
      expect(result.ambiguousEmails).toEqual([]);
    });

    it("should leave token unset when no participant matches the email", async () => {
      const db = createDbMock();

      const result = await lookupParticipantTokensByEmail(db, ["missing@example.com"]);

      expect(result.tokenByEmail.size).toBe(0);
      expect(result.unmatchedEmails).toEqual(["missing@example.com"]);
    });

    it("should leave token unset and warn when an email matches multiple participants", async () => {
      const db = createDbMock({
        "shared@example.com": {
          empty: false,
          size: 2,
          docs: [
            { data: () => ({ token: "tok-1" }) },
            { data: () => ({ token: "tok-2" }) },
          ],
        },
      });
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

      const result = await lookupParticipantTokensByEmail(db, ["shared@example.com"]);

      expect(result.tokenByEmail.size).toBe(0);
      expect(result.ambiguousEmails).toEqual(["shared@example.com"]);
      expect(warnSpy).toHaveBeenCalled();
    });
  });
});
