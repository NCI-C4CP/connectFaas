const { BigQuery } = require("@google-cloud/bigquery");

const bigqueryPath = require.resolve("../../utils/bigquery");

describe("bigquery notification helpers", () => {
  let bigqueryModule;
  let querySpy;

  beforeEach(() => {
    vi.restoreAllMocks();
    delete require.cache[bigqueryPath];
    querySpy = vi.spyOn(BigQuery.prototype, "query");
    bigqueryModule = require("../../utils/bigquery");
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete require.cache[bigqueryPath];
  });

  it("should skip the notification participant query when required filters are missing", async () => {
    const result = await bigqueryModule.getParticipantsForNotificationsBQ({
      notificationSpecId: "spec-1",
      conditions: [],
    });

    expect(result).toEqual([]);
    expect(querySpy).not.toHaveBeenCalled();
  });

  it("should distinguish misconfigured spec (-2) from query failure (-1) and successful zero count (0)", async () => {
    // Empty conditions = misconfigured spec, return -2 without running a query.
    const misconfigured = await bigqueryModule.countParticipantsForNotificationsBQ({
      notificationSpecId: "spec-x",
      conditions: [],
    });
    expect(misconfigured).toBe(-2);

    // Query that fails returns -1.
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    querySpy.mockRejectedValueOnce(new Error("BQ unavailable"));
    const failed = await bigqueryModule.countParticipantsForNotificationsBQ({
      notificationSpecId: "spec-x",
      conditions: [["821247024", "equals", 1]],
    });
    expect(failed).toBe(-1);
    consoleSpy.mockRestore();

    // Successful zero-result query returns 0.
    querySpy.mockResolvedValueOnce([[]]);
    const zero = await bigqueryModule.countParticipantsForNotificationsBQ({
      notificationSpecId: "spec-x",
      conditions: [["821247024", "equals", 1]],
    });
    expect(zero).toBe(0);
  });

  it("should build the participant query with shared eligibility conditions, parameterized values, and pagination", async () => {
    querySpy.mockResolvedValueOnce([[
      {
        token: "tok-2",
        state_DOT_uid: "uid-2",
        d_335767902: "user@example.com",
      },
    ]]);

    const result = await bigqueryModule.getParticipantsForNotificationsBQ({
      notificationSpecId: "spec-1",
      conditions: [
        ["state.uid", "equals", "uid-2"],
        "d_821247024 IS NOT NULL",
        ["335767902", "notequals", "skip@example.com"],
      ],
      startTimeStr: "2026-04-14T15:00:00.000Z",
      stopTimeStr: "2026-04-14T14:00:00.000Z",
      timeField: "821247024",
      fieldsToFetch: ["token", "state.uid", "335767902"],
      previousToken: "tok-1",
      limit: 50,
    });

    expect(querySpy).toHaveBeenCalledTimes(1);
    const { query, params } = querySpy.mock.calls[0][0];
    expect(query).toContain("SELECT token AS token, state.uid AS state_DOT_uid, d_335767902 AS d_335767902");
    expect(query).toContain("notificationSpecificationsID = @notificationSpecId");
    expect(query).toContain("state.uid = @cond_0");
    expect(query).toContain("(d_821247024 IS NOT NULL)");
    expect(query).toContain("d_335767902 != @cond_1");
    expect(query).toContain("d_821247024 < @startTimeStr");
    expect(query).toContain("d_821247024 >= @stopTimeStr");
    expect(query).toContain("IFNULL(isSent, TRUE) = TRUE");
    expect(query).toContain("processingState = 'send_failed'");
    expect(query).toContain("AND isSent IS NULL");
    expect(query).toContain("token > @previousToken");
    expect(query).toContain("ORDER BY token LIMIT 50");

    expect(params).toMatchObject({
      notificationSpecId: "spec-1",
      cond_0: "uid-2",
      cond_1: "skip@example.com",
      startTimeStr: "2026-04-14T15:00:00.000Z",
      stopTimeStr: "2026-04-14T14:00:00.000Z",
      previousToken: "tok-1",
    });

    expect(result).toEqual([{
      token: "tok-2",
      state: { uid: "uid-2" },
      335767902: "user@example.com",
    }]);
  });

  it("should build the count query without pagination clauses and return a numeric count", async () => {
    querySpy.mockResolvedValueOnce([[{ cnt: "42" }]]);

    const count = await bigqueryModule.countParticipantsForNotificationsBQ({
      notificationSpecId: "spec-2",
      conditions: [["821247024", "greater", 5]],
      startTimeStr: "2026-04-14T15:00:00.000Z",
      stopTimeStr: "2026-04-14T14:00:00.000Z",
      timeField: "821247024",
    });

    expect(count).toBe(42);
    expect(querySpy).toHaveBeenCalledTimes(1);
    const { query, params } = querySpy.mock.calls[0][0];
    expect(query).toContain("SELECT COUNT(*) AS cnt");
    expect(query).toContain("FROM `Connect.participants`");
    expect(query).toContain("d_821247024 > @cond_0");
    expect(query).toContain("IFNULL(isSent, TRUE) = TRUE");
    expect(query).toContain("AND isSent IS NULL");
    expect(query).not.toContain("ORDER BY token");
    expect(query).not.toContain("LIMIT ");
    expect(query).not.toContain("token > @previousToken");
    expect(params).toMatchObject({ cond_0: 5, notificationSpecId: "spec-2" });
  });

  it("should treat sent, legacy, and send_failed notification rows as already-processed (no re-fetch)", async () => {
    querySpy.mockResolvedValueOnce([[{ cnt: 1 }]]);

    await bigqueryModule.countParticipantsForNotificationsBQ({
      notificationSpecId: "spec-state-machine",
      conditions: [["821247024", "equals", 1]],
    });

    const { query, params } = querySpy.mock.calls[0][0];
    expect(query).toContain("LEFT JOIN (");
    expect(query).toContain("SELECT DISTINCT token, TRUE AS isSent");
    expect(query).toContain("notificationSpecificationsID = @notificationSpecId");
    expect(query).toContain("IFNULL(isSent, TRUE) = TRUE");
    expect(query).toContain("processingState = 'send_failed'");
    expect(query).toContain("AND isSent IS NULL");
    expect(params.notificationSpecId).toBe("spec-state-machine");
  });

  it("should reject unsafe BigQuery identifiers in condition keys and time fields", async () => {
    await expect(
      bigqueryModule.getParticipantsForNotificationsBQ({
        notificationSpecId: "spec-x",
        conditions: [["state.uid; DROP TABLE participants;--", "equals", "x"]],
      })
    ).rejects.toThrow(/Unsafe BigQuery identifier/);
  });

  it("should return -1 when the shared count query fails", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    querySpy.mockRejectedValueOnce(new Error("BQ unavailable"));

    const count = await bigqueryModule.countParticipantsForNotificationsBQ({
      notificationSpecId: "spec-3",
      conditions: [["821247024", "equals", 1]],
    });

    expect(count).toBe(-1);
    expect(consoleSpy).toHaveBeenCalled();
  });
});
