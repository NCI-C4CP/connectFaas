/**
 * Backfill email suppressions from SendGrid's suppression APIs.
 *
 * Usage:
 *   SENDGRID_API_KEY=<key> node scripts/backfillEmailSuppressions.js --type=legacy_global_unsubscribes [--dry-run]
 *
 * Supported --type values are exact SendGrid-native suppression labels:
 * bounces, spam_reports, invalid_emails, global_unsubscribes,
 * legacy_global_unsubscribes, and group_unsubscribes with --group-id.
 */

const {
  buildEmailSuppressionDoc,
  getEmailSuppressionPolicyForImportType,
  normalizeEmailAddress,
} = require("../utils/emailSuppressionPolicy");

const SENDGRID_API_BASE_URL = "https://api.sendgrid.com";
const SENDGRID_API_PAGE_LIMIT = 500;

const SENDGRID_PAGINATED_SUPPRESSION_CONFIG = Object.freeze({
  legacy_global_unsubscribes: Object.freeze({
    endpoint: "/v3/suppression/unsubscribes",
    supportsTimeRange: false,
  }),
  global_unsubscribes: Object.freeze({
    endpoint: "/v3/suppression/unsubscribes",
    supportsTimeRange: false,
  }),
  bounces: Object.freeze({
    endpoint: "/v3/suppression/bounces",
    supportsTimeRange: true,
  }),
  invalid_emails: Object.freeze({
    endpoint: "/v3/suppression/invalid_emails",
    supportsTimeRange: true,
  }),
  spam_reports: Object.freeze({
    endpoint: "/v3/suppression/spam_reports",
    supportsTimeRange: true,
  }),
});

const SUPPORTED_SENDGRID_SUPPRESSION_TYPES = Object.freeze([
  "legacy_global_unsubscribes",
  "global_unsubscribes",
  "group_unsubscribes",
  "bounces",
  "spam_reports",
  "invalid_emails",
]);

const normalizeSendGridSuppressionType = (type = "") => (
  typeof type === "string" ? type.trim().toLowerCase() : ""
);

const resolveSendGridSuppressionType = (type = "") => {
  const normalizedType = normalizeSendGridSuppressionType(type);
  return SUPPORTED_SENDGRID_SUPPRESSION_TYPES.includes(normalizedType) ? normalizedType : "";
};

const getSendGridApiBaseUrl = ({ apiBaseUrl = "" } = {}) => {
  if (apiBaseUrl) return apiBaseUrl;
  return SENDGRID_API_BASE_URL;
};

const getSendGridSuppressionRequestConfig = ({ type = "", groupId = "" } = {}) => {
  const resolvedType = resolveSendGridSuppressionType(type);
  if (!resolvedType) return null;

  if (resolvedType === "group_unsubscribes") {
    const normalizedGroupId = `${groupId || ""}`.trim();
    if (!normalizedGroupId) return null;
    return {
      type: resolvedType,
      endpoint: `/v3/asm/groups/${encodeURIComponent(normalizedGroupId)}/suppressions`,
      paginated: false,
      supportsTimeRange: false,
    };
  }

  const config = SENDGRID_PAGINATED_SUPPRESSION_CONFIG[resolvedType];
  return config ? {
    type: resolvedType,
    endpoint: config.endpoint,
    paginated: true,
    supportsTimeRange: config.supportsTimeRange,
  } : null;
};

const normalizePageLimit = (limit = SENDGRID_API_PAGE_LIMIT) => {
  const parsedLimit = Number.parseInt(`${limit}`, 10);
  if (!Number.isFinite(parsedLimit)) return SENDGRID_API_PAGE_LIMIT;
  return Math.max(1, Math.min(SENDGRID_API_PAGE_LIMIT, parsedLimit));
};

const buildSendGridRequestUrl = ({ apiBaseUrl = SENDGRID_API_BASE_URL, endpoint = "", query = {} } = {}) => {
  const baseUrl = apiBaseUrl.endsWith("/") ? apiBaseUrl : `${apiBaseUrl}/`;
  const url = new URL(endpoint, baseUrl);
  Object.entries(query).forEach(([key, value]) => {
    if (value !== null && value !== undefined && value !== "") {
      url.searchParams.set(key, `${value}`);
    }
  });
  return url.toString();
};

const buildSendGridRequestHeaders = ({ apiKey = "", onBehalfOf = "" } = {}) => {
  const headers = {
    Accept: "application/json",
    Authorization: `Bearer ${apiKey}`,
  };
  if (onBehalfOf) {
    headers["on-behalf-of"] = onBehalfOf;
  }
  return headers;
};

const readSendGridResponseBody = async (response) => {
  if (typeof response.text === "function") {
    const text = await response.text();
    if (!text) return { text: "", json: null };
    try {
      return { text, json: JSON.parse(text) };
    } catch (err) {
      return { text, json: null, parseError: err };
    }
  }
  if (typeof response.json === "function") {
    const json = await response.json();
    return { text: JSON.stringify(json), json };
  }
  return { text: "", json: null };
};

const SENDGRID_RETRYABLE_STATUSES = new Set([408, 429, 500, 502, 503, 504]);
const SENDGRID_MAX_RETRIES = 5;
const sleepMs = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const requestSendGridJson = async ({
  apiKey = "",
  apiBaseUrl = SENDGRID_API_BASE_URL,
  endpoint = "",
  query = {},
  onBehalfOf = "",
  fetchImpl = globalThis.fetch,
  maxRetries = SENDGRID_MAX_RETRIES,
  delayImpl = sleepMs,
} = {}) => {
  if (!apiKey) {
    throw new Error("SendGrid API key is required. Set SENDGRID_API_KEY or pass --api-key-env=<env var>.");
  }
  if (typeof fetchImpl !== "function") {
    throw new Error("No fetch implementation is available for SendGrid API requests.");
  }

  const url = buildSendGridRequestUrl({ apiBaseUrl, endpoint, query });
  let lastError = null;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    let response;
    try {
      response = await fetchImpl(url, {
        method: "GET",
        headers: buildSendGridRequestHeaders({ apiKey, onBehalfOf }),
      });
    } catch (networkError) {
      lastError = networkError;
      if (attempt === maxRetries) throw networkError;
      const backoffMs = Math.min(30000, 500 * Math.pow(2, attempt)) + Math.floor(Math.random() * 250);
      console.warn(`SendGrid request network error (attempt ${attempt + 1}/${maxRetries + 1}): ${networkError.message}. Retrying in ${backoffMs}ms.`);
      await delayImpl(backoffMs);
      continue;
    }

    const body = await readSendGridResponseBody(response);

    if (!response.ok) {
      const detail = body.text ? `: ${body.text.slice(0, 500)}` : "";
      const error = new Error(`SendGrid suppression API request failed (${response.status}) for ${endpoint}${detail}`);
      error.status = response.status;
      error.endpoint = endpoint;
      error.body = body.text;
      lastError = error;
      if (SENDGRID_RETRYABLE_STATUSES.has(response.status) && attempt < maxRetries) {
        const backoffMs = Math.min(30000, 500 * Math.pow(2, attempt)) + Math.floor(Math.random() * 250);
        console.warn(`SendGrid ${response.status} for ${endpoint} (attempt ${attempt + 1}/${maxRetries + 1}). Retrying in ${backoffMs}ms.`);
        await delayImpl(backoffMs);
        continue;
      }
      throw error;
    }

    if (body.parseError) {
      throw new Error(`SendGrid suppression API returned non-JSON response for ${endpoint}.`);
    }

    return body.json;
  }

  throw lastError || new Error(`SendGrid request to ${endpoint} failed after retries.`);
};

const normalizeSendGridSuppressionPage = (body, endpoint = "") => {
  if (Array.isArray(body)) return body;
  throw new Error(`Unexpected SendGrid suppression API response shape for ${endpoint}.`);
};

const fetchSendGridSuppressionRows = async ({
  type = "",
  groupId = "",
  apiKey = "",
  apiBaseUrl = SENDGRID_API_BASE_URL,
  pageLimit = SENDGRID_API_PAGE_LIMIT,
  startTime = null,
  endTime = null,
  onBehalfOf = "",
  fetchImpl = globalThis.fetch,
  onPage = null,
  startOffset = 0,
  maxRows = null,
} = {}) => {
  const resolvedType = resolveSendGridSuppressionType(type);
  if (resolvedType === "group_unsubscribes" && !`${groupId || ""}`.trim()) {
    throw new Error("group_unsubscribes requires groupId/--group-id to avoid importing every ASM group.");
  }
  const config = getSendGridSuppressionRequestConfig({ type, groupId });
  if (!config) {
    throw new Error(`Unknown type: ${type}. Must be exactly one of: ${SUPPORTED_SENDGRID_SUPPRESSION_TYPES.join(", ")}`);
  }

  const requestPage = async (query = {}) => {
    const body = await requestSendGridJson({
      apiKey,
      apiBaseUrl,
      endpoint: config.endpoint,
      query,
      onBehalfOf,
      fetchImpl,
    });
    return normalizeSendGridSuppressionPage(body, config.endpoint);
  };

  if (!config.paginated) {
    const rows = await requestPage();
    if (typeof onPage === "function") {
      onPage({ endpoint: config.endpoint, offset: null, rows: rows.length, totalRows: rows.length });
    }
    return rows;
  }

  const limit = normalizePageLimit(pageLimit);
  const cap = Number.isFinite(Number(maxRows)) && Number(maxRows) > 0 ? Number(maxRows) : null;
  const rows = [];
  let offset = Math.max(0, Number(startOffset) || 0);
  let capReached = false;
  while (true) {
    const query = { limit, offset };
    if (config.supportsTimeRange) {
      if (startTime !== null && startTime !== undefined) query.start_time = startTime;
      if (endTime !== null && endTime !== undefined) query.end_time = endTime;
    }

    const page = await requestPage(query);
    rows.push(...page);
    if (typeof onPage === "function") {
      onPage({ endpoint: config.endpoint, offset, rows: page.length, totalRows: rows.length });
    }

    if (cap !== null && rows.length >= cap) {
      capReached = true;
      rows.length = cap;
      break;
    }
    if (page.length < limit) break;
    offset += limit;
  }

  if (capReached) {
    console.warn(`Reached --max-rows cap (${cap}); additional rows beyond offset ${offset + limit} were not fetched. Re-run with --start-offset=${offset + limit} to continue.`);
  }

  return rows;
};

const extractSendGridSuppressionEmail = (row) => {
  if (typeof row === "string") return normalizeEmailAddress(row);
  if (row && typeof row === "object") return normalizeEmailAddress(row.email);
  return "";
};

const classifySuppressionRow = ({ email, type }) => {
  const resolvedType = resolveSendGridSuppressionType(type);
  const policy = resolvedType ? getEmailSuppressionPolicyForImportType(resolvedType) : null;
  if (!policy) return null;
  return {
    email: normalizeEmailAddress(email),
    reason: policy.reason,
    suppressBulk: policy.suppressBulk,
    suppressOperational: policy.suppressOperational,
  };
};

const classifySendGridSuppressionRows = (rows = [], type = "") => {
  const resolvedType = resolveSendGridSuppressionType(type);
  const classified = [];
  let skipped = 0;

  for (const row of rows) {
    const email = extractSendGridSuppressionEmail(row);
    if (!email || !email.includes("@")) {
      skipped++;
      continue;
    }

    const result = classifySuppressionRow({ email, type: resolvedType });
    if (result) {
      classified.push(result);
    } else {
      skipped++;
    }
  }

  return { classified, skipped };
};

const lookupParticipantTokensByEmail = async (db, emailArray = []) => {
  const tokenByEmail = new Map();
  const matchedEmails = [];
  const unmatchedEmails = [];
  const ambiguousEmails = [];
  const uniqueEmails = [...new Set(emailArray.map((email) => normalizeEmailAddress(email)).filter(Boolean))];

  // Bounded parallelism: Firestore array-contains lookups can run concurrently
  // safely. 25 in flight keeps a 1,000-email backfill responsive without
  // exhausting Firestore connection pools or read quotas.
  const LOOKUP_CONCURRENCY = 25;
  const lookupOne = async (email) => {
    const snapshot = await db
      .collection("participants")
      .where("query.allEmails", "array-contains", email)
      .select("token")
      .limit(2)
      .get();
    return { email, snapshot };
  };

  for (let i = 0; i < uniqueEmails.length; i += LOOKUP_CONCURRENCY) {
    const chunk = uniqueEmails.slice(i, i + LOOKUP_CONCURRENCY);
    const results = await Promise.all(chunk.map(lookupOne));

    for (const { email, snapshot } of results) {
      if (snapshot.empty) {
        unmatchedEmails.push(email);
        continue;
      }
      if (snapshot.size > 1) {
        ambiguousEmails.push(email);
        console.warn(`Ambiguous participant match for ${email}; leaving suppression token unset.`);
        continue;
      }
      const token = snapshot.docs[0].data()?.token || "";
      if (token) {
        tokenByEmail.set(email, token);
        matchedEmails.push(email);
      } else {
        unmatchedEmails.push(email);
      }
    }
  }

  return { tokenByEmail, matchedEmails, unmatchedEmails, ambiguousEmails };
};

const buildSuppressionDocs = (classifiedRows, tokenByEmail = new Map()) => {
  const byEmail = new Map();
  for (const row of classifiedRows) {
    byEmail.set(row.email, row);
  }
  const now = new Date().toISOString();
  return Array.from(byEmail.values()).map((row) => {
    // Keep backfill writes monotonic like runtime suppression writes. Imports
    // should not clear a prior hard-bounce, spam-report, or unsubscribe flag.
    return buildEmailSuppressionDoc({
      normalizedEmail: row.email,
      reason: row.reason,
      token: tokenByEmail.get(row.email) || "",
      suppressBulk: row.suppressBulk,
      suppressOperational: row.suppressOperational,
      notificationId: "backfill",
      lastEventAt: now,
    });
  }).filter(Boolean);
};

// CLI runner; only runs when invoked directly.

const runCli = async () => {
  const args = process.argv.slice(2);
  const getArg = (name) => {
    const arg = args.find((a) => a.startsWith(`--${name}=`));
    return arg ? arg.split("=").slice(1).join("=") : null;
  };
  const dryRun = args.includes("--dry-run");
  const type = getArg("type");
  const normalizedType = resolveSendGridSuppressionType(type);
  const groupId = getArg("group-id") || "";
  const apiKeyEnv = getArg("api-key-env") || "SENDGRID_API_KEY";
  const apiKey = process.env[apiKeyEnv];
  const apiBaseUrl = getSendGridApiBaseUrl({
    apiBaseUrl: getArg("api-base-url") || "",
  });
  const pageLimit = normalizePageLimit(getArg("limit") || SENDGRID_API_PAGE_LIMIT);
  const startTimeRaw = getArg("start-time");
  const endTimeRaw = getArg("end-time");
  const startOffsetRaw = getArg("start-offset");
  const maxRowsRaw = getArg("max-rows");
  const onBehalfOf = getArg("on-behalf-of") || "";
  const config = getSendGridSuppressionRequestConfig({ type, groupId });

  const validateUnixSeconds = (value, label) => {
    if (value === null || value === undefined) return null;
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || !Number.isInteger(parsed) || parsed < 0) {
      console.error(`--${label} must be a non-negative integer (Unix epoch seconds). Got: ${JSON.stringify(value)}`);
      process.exit(1);
    }
    return parsed;
  };
  const startTime = validateUnixSeconds(startTimeRaw, "start-time");
  const endTime = validateUnixSeconds(endTimeRaw, "end-time");

  const validateNonNegativeInteger = (value, label) => {
    if (value === null || value === undefined) return null;
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || !Number.isInteger(parsed) || parsed < 0) {
      console.error(`--${label} must be a non-negative integer. Got: ${JSON.stringify(value)}`);
      process.exit(1);
    }
    return parsed;
  };
  const startOffset = validateNonNegativeInteger(startOffsetRaw, "start-offset") || 0;
  const maxRows = validateNonNegativeInteger(maxRowsRaw, "max-rows");

  if (!type) {
    console.error(
      "Usage: SENDGRID_API_KEY=<key> node scripts/backfillEmailSuppressions.js --type=<bounces|spam_reports|invalid_emails|global_unsubscribes|legacy_global_unsubscribes|group_unsubscribes> [--group-id=<id>] [--dry-run]",
    );
    process.exit(1);
  }

  if (normalizedType === "group_unsubscribes" && !groupId.trim()) {
    console.error("--type=group_unsubscribes requires --group-id=<id> to avoid importing every ASM group.");
    process.exit(1);
  }

  if (!normalizedType || !getEmailSuppressionPolicyForImportType(normalizedType) || !config) {
    console.error(`Unknown type: ${type}. Must be exactly one of: ${SUPPORTED_SENDGRID_SUPPRESSION_TYPES.join(", ")}`);
    process.exit(1);
  }

  if (!apiKey) {
    console.error(`Missing SendGrid API key. Set ${apiKeyEnv} or pass --api-key-env=<env var>.`);
    process.exit(1);
  }

  const apiRows = await fetchSendGridSuppressionRows({
    type: normalizedType,
    groupId,
    apiKey,
    apiBaseUrl,
    pageLimit,
    startTime,
    endTime,
    onBehalfOf,
    startOffset,
    maxRows,
    onPage: ({ offset, rows, totalRows }) => {
      const pageLabel = offset === null ? "unpaginated" : `offset ${offset}`;
      console.log(`Fetched ${rows} rows from SendGrid (${pageLabel}); total ${totalRows}.`);
    },
  });
  const { classified, skipped } = classifySendGridSuppressionRows(apiRows, normalizedType);

  let db = null;
  let firestoreProjectId = "(uninitialized)";
  let tokenLookup = {
    tokenByEmail: new Map(),
    matchedEmails: [],
    unmatchedEmails: [],
    ambiguousEmails: [],
  };
  if (classified.length > 0) {
    const admin = require("firebase-admin");
    admin.initializeApp();
    db = admin.firestore();
    db.settings({ ignoreUndefinedProperties: true });
    firestoreProjectId = db?.app?.options?.projectId
      || process.env.GCLOUD_PROJECT
      || process.env.GOOGLE_CLOUD_PROJECT
      || "(unknown)";
    console.log(`\nFirestore project: ${firestoreProjectId}`);
    tokenLookup = await lookupParticipantTokensByEmail(db, classified.map((row) => row.email));
  }

  const docs = buildSuppressionDocs(classified, tokenLookup.tokenByEmail);

  console.log("\nBackfill Summary");
  console.log("Source: SendGrid API");
  console.log(`Endpoint: ${config.endpoint}`);
  console.log(`API base URL: ${apiBaseUrl}`);
  console.log(`Firestore project: ${firestoreProjectId}`);
  console.log(`Type: ${normalizedType}`);
  if (groupId) console.log(`Group ID: ${groupId}`);
  if (config.paginated) console.log(`Page limit: ${pageLimit}`);
  if (startOffset) console.log(`Start offset: ${startOffset}`);
  if (maxRows) console.log(`Max rows: ${maxRows}`);
  if (startTime !== null) console.log(`Start time (epoch s): ${startTime}`);
  if (endTime !== null) console.log(`End time (epoch s): ${endTime}`);
  console.log(`Total API rows: ${apiRows.length}`);
  console.log(`Classified: ${classified.length}`);
  console.log(`After dedup: ${docs.length}`);
  console.log(`Skipped: ${skipped}`);
  console.log(`Participant tokens linked: ${tokenLookup.matchedEmails.length}`);
  console.log(`Participant emails not found: ${tokenLookup.unmatchedEmails.length}`);
  console.log(`Participant emails ambiguous: ${tokenLookup.ambiguousEmails.length}`);
  console.log(`Dry run: ${dryRun}\n`);

  if (dryRun) {
    console.log("Dry run - no writes performed.");
    if (docs.length > 0) {
      console.log("Sample doc:", JSON.stringify(docs[0], null, 2));
    }
    return;
  }

  const BATCH_SIZE = 500;
  let written = 0;

  for (let i = 0; i < docs.length; i += BATCH_SIZE) {
    const chunk = docs.slice(i, i + BATCH_SIZE);
    const batch = db.batch();
    for (const doc of chunk) {
      const ref = db.collection("emailAddressStatus").doc(doc.normalizedEmail);
      batch.set(ref, doc, { merge: true });
    }
    await batch.commit();
    written += chunk.length;
    console.log(`Written ${written}/${docs.length}`);
  }

  console.log(`\nBackfill complete. ${written} documents written.`);
};

// Run CLI only when invoked directly
if (require.main === module) {
  runCli().catch((err) => {
    console.error("Backfill failed:", err);
    process.exit(1);
  });
}

module.exports = {
  SENDGRID_API_BASE_URL,
  SENDGRID_API_PAGE_LIMIT,
  SUPPORTED_SENDGRID_SUPPRESSION_TYPES,
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
};
