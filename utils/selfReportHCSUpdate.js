const { getResponseJSON, setHeaders, logIPAddress, safeJSONParse } = require('./shared');
const { validateIso8601Timestamp } = require('./validation');
const fieldMapping = require('./fieldToConceptIdMapping');

/**
 * Self-Report Health Care System Update (Connect PWA "Share New Health Information",
 * episphere/connect#1658). One Firestore doc per submitted update in `selfReportHCSUpdates`:
 *   - Submit-only: Submitted docs are append-only and never editable afterward. The PWA displays only the most recent update.
 *   - Format: flat Quest-like D_<cid> string values.
 *   - The submitted timestamp and docLastUpdatedTimestamp are stamped server-side in ISO8601 form (`YYYY-MM-DDTHH:mm:ss.SSSZ`).
 */

const SELF_REPORT_HCS_UPDATE_COLLECTION = 'selfReportHCSUpdates';

const hcsCIDs = fieldMapping.selfReportHCSUpdate;
const DOC_LAST_UPDATED = fieldMapping.docLastUpdatedTimestamp;
const SUBMITTED_TIMESTAMP_KEY = `D_${hcsCIDs.submittedTimestamp}`;
const YES = String(fieldMapping.yes);
const NO = String(fieldMapping.no);
const YES_NO = new Set([YES, NO]);

const MONTH_CIDS = new Set(hcsCIDs.monthResponses.map(String));
const LANGUAGE_CIDS = new Set([fieldMapping.english, fieldMapping.spanish]);

// Every storable question is a flat top-level scalar.
const SCALAR_CIDS = new Set([
    ...Object.values(hcsCIDs.facility),
    hcsCIDs.changeMonth, hcsCIDs.changeYear, hcsCIDs.additionalInfo,
].map(String));
const YES_NO_CIDS = new Set([hcsCIDs.facility.intlFlag, hcsCIDs.facility.googleValidated].map(String));
// International-only fields (Line 4, Country) may not accompany a domestic address.
const INTL_ONLY_CIDS = [hcsCIDs.facility.line4, hcsCIDs.facility.country];

const TOP_LEVEL_D_KEY_RE = /^D_(\d{9})$/;
// Strip server-owned metadata if sent by a client (lockedAttributes precedent — spoofs never reject).
const SERVER_OWNED_KEYS = new Set([SUBMITTED_TIMESTAMP_KEY, 'uid', 'token', 'Connect_ID']);

const MAX_D_VALUE_LENGTH = 800;   // spec write-in cap (additional-information textbox)
const D_VALUE_MAX_LENGTH_BY_CID = new Map([
    [hcsCIDs.facility.line1, 70],
    [hcsCIDs.facility.line2, 70],
    [hcsCIDs.facility.line3, 70],
    [hcsCIDs.facility.line4, 70],
    [hcsCIDs.facility.city, 45],
    [hcsCIDs.facility.state, 48],
    [hcsCIDs.facility.zip, 45],
    [hcsCIDs.changeYear, 4],
    [hcsCIDs.additionalInfo, 800],
].map(([cid, maxLength]) => [String(cid), maxLength]));
const MAX_KEYS = 50;
const MAX_BODY_LENGTH = 20000;

// Data dictionary range check for the change year: prevent > 1 year in the future.
const CHANGE_YEAR_FUTURE_ALLOWANCE = 1;
const YEAR_RE = /^(19|20)\d{2}$/;
const isValidYear = (v, maxYear) => typeof v === 'string' && YEAR_RE.test(v) && Number(v) <= maxYear;
const dKey = (cid) => `D_${cid}`;
const valueOf = (body, cid) => body?.[dKey(cid)];

// Object.fromEntries uses define-own-property semantics, so a crafted "__proto__" body key
// becomes an ordinary own key (rejected by the whitelist) instead of silently mutating the
// prototype and letting validation read values that would never be persisted.
const stripServerOwnedKeys = (body) =>
    Object.fromEntries(Object.entries(body).filter(([k]) => !SERVER_OWNED_KEYS.has(k)));

/**
 * Validate the submitted snapshot's shape. Strict flat key whitelist, string-typed D_ values, size caps.
 * @returns {string[]} errors (empty = valid)
 */
const validateSnapshotShape = (body) => {
    const errors = [];
    const keys = Object.keys(body);
    if (keys.length > MAX_KEYS) errors.push(`too many keys (${keys.length} > ${MAX_KEYS})`);
    if (JSON.stringify(body).length > MAX_BODY_LENGTH) errors.push('body too large');

    const badKeys = [];
    for (const key of keys) {
        if (key === String(hcsCIDs.surveyLanguage)) {
            if (!LANGUAGE_CIDS.has(body[key])) errors.push(`${hcsCIDs.surveyLanguage} must be a valid survey language cid`);
            continue;
        }
        if (key === String(DOC_LAST_UPDATED)) {
            const validation = validateIso8601Timestamp(body[key]);
            if (validation.error) errors.push(`${DOC_LAST_UPDATED} must be a canonical ISO timestamp (${validation.message})`);
            continue;
        }
        const match = TOP_LEVEL_D_KEY_RE.exec(key);
        if (!match || !SCALAR_CIDS.has(match[1])) { badKeys.push(key); continue; }
        const value = body[key];
        const maxLength = D_VALUE_MAX_LENGTH_BY_CID.get(match[1]) ?? MAX_D_VALUE_LENGTH;
        if (typeof value !== 'string') errors.push(`${key} must be a string`);
        else if (value.length > maxLength) errors.push(`${key} exceeds ${maxLength} chars`);
    }
    if (badKeys.length) errors.push(`invalid keys: ${badKeys.join(', ')}`);
    return errors;
};

/**
 * The submit rule matrix. Returns { error, message }.
 * Repeat submissions are accepted; each is a new append-only row (the PWA shows the latest).
 */
const validateSubmission = (body) => {
    const fail = (message) => ({ error: true, message });
    const currentYear = new Date().getFullYear();

    // Rule: facility name and change year are required. All other address fields areoptional.
    const facilityName = valueOf(body, hcsCIDs.facility.line1);
    if (typeof facilityName !== 'string' || !facilityName.trim()) {
        return fail(`Invalid or missing primary care facility name (${hcsCIDs.facility.line1}).`);
    }

    // Range check per the data dictionary.
    const changeYear = valueOf(body, hcsCIDs.changeYear);
    if (!isValidYear(changeYear, currentYear + CHANGE_YEAR_FUTURE_ALLOWANCE)) {
        return fail(`Invalid or missing primary care facility change year (${hcsCIDs.changeYear}).`);
    }

    // Rule: change month optional.
    const changeMonth = valueOf(body, hcsCIDs.changeMonth);
    if (changeMonth !== undefined && !MONTH_CIDS.has(changeMonth)) {
        return fail(`Invalid primary care facility change month (${hcsCIDs.changeMonth}).`);
    }

    // Rule: Yes/No flags, and an international facility cannot be Google-address validated.
    for (const cid of YES_NO_CIDS) {
        const value = valueOf(body, cid);
        if (value !== undefined && !YES_NO.has(value)) return fail(`Invalid Yes/No value (${cid}).`);
    }
    const intl = valueOf(body, hcsCIDs.facility.intlFlag) === YES;
    if (intl && valueOf(body, hcsCIDs.facility.googleValidated) === YES) {
        return fail('International primary care facility cannot be Google-address validated.');
    }

    // Rule: Line 4 and Country are international-only, and a domestic zip is 5 digits (UI parity).
    if (!intl) {
        for (const cid of INTL_ONLY_CIDS) {
            if (valueOf(body, cid) !== undefined) {
                return fail(`Field ${cid} is only allowed for international facilities.`);
            }
        }
        const zip = valueOf(body, hcsCIDs.facility.zip);
        if (zip && !/^\d{5}$/.test(zip)) {
            return fail(`Invalid domestic zip code (${hcsCIDs.facility.zip}).`);
        }
    }

    return { error: false, message: 'Success!' };
};

const parseBody = (req) => {
    const body = typeof req.body === 'string' ? safeJSONParse(req.body) : req.body;
    return body && typeof body === 'object' && !Array.isArray(body) && Object.keys(body).length ? body : null;
};

const loadParticipant = async (uid) => {
    const { retrieveUserProfile } = require('./firestore');
    const profile = await retrieveUserProfile(uid);
    if (!profile || profile instanceof Error || !profile.token || !profile.Connect_ID) return null;
    return { token: profile.token, connectId: profile.Connect_ID, profile };
};

/**
 * Submit one append-only health care system update (PHI — same write eligibility as the
 * self-report cancer dx module: verified and not withdrawn).
 */
const storeSelfReportHCSUpdate = async (req, res, uid) => {
    logIPAddress(req);
    setHeaders(res);
    if (req.method !== 'POST') {
        return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
    }

    const raw = parseBody(req);
    if (!raw) {
        return res.status(400).json(getResponseJSON('Bad request: empty submission.', 400));
    }

    const body = stripServerOwnedKeys(raw);
    const shapeErrors = validateSnapshotShape(body);
    if (shapeErrors.length) {
        return res.status(400).json(getResponseJSON(`Bad request: ${shapeErrors.join('; ')}`, 400));
    }

    const participant = await loadParticipant(uid);
    if (!participant) {
        return res.status(404).json(getResponseJSON('Token not found!', 404));
    }

    const { ineligibilityReason } = require('./selfReportCancerDx');
    const ineligible = ineligibilityReason(participant.profile);
    if (ineligible) {
        return res.status(403).json(getResponseJSON(`Not eligible to report a health care system update: ${ineligible}.`, 403));
    }

    const validation = validateSubmission(body);
    if (validation.error) return res.status(400).json(getResponseJSON(validation.message, 400));

    const { addSelfReportHCSUpdateDoc } = require('./firestore');
    const now = new Date().toISOString(); // one timestamp for submittedTimestamp + docLastUpdatedTimestamp
    await addSelfReportHCSUpdateDoc({
        ...body,
        [SUBMITTED_TIMESTAMP_KEY]: now,
        [DOC_LAST_UPDATED]: now,
        uid,
        token: participant.token,
        Connect_ID: participant.connectId,
    });
    return res.status(200).json(getResponseJSON('Health care system update submitted successfully!', 200));
};

/**
 * Read previously-submitted updates, ascending by submitted timestamp (the PWA shows the last row).
 */
const getSelfReportHCSUpdate = async (req, res, uid) => {
    logIPAddress(req);
    setHeaders(res);
    if (req.method !== 'GET') return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));

    const { getSelfReportHCSUpdateDocs } = require('./firestore');
    const submitted = await getSelfReportHCSUpdateDocs(uid);
    const sorted = [...submitted].sort((a, b) => String(a[SUBMITTED_TIMESTAMP_KEY] || '').localeCompare(String(b[SUBMITTED_TIMESTAMP_KEY] || '')));
    return res.status(200).json({ data: { submitted: sorted }, code: 200 });
};

module.exports = {
    SELF_REPORT_HCS_UPDATE_COLLECTION,
    storeSelfReportHCSUpdate,
    getSelfReportHCSUpdate,
    stripServerOwnedKeys,
    validateSnapshotShape,
    validateSubmission,
};
