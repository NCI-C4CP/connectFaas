const { getResponseJSON, setHeaders, logIPAddress, safeJSONParse } = require('./shared');
const { validateIso8601Timestamp } = require('./validation');
const fieldMapping = require('./fieldToConceptIdMapping');

/**
 * Self-Report Cancer Diagnosis survey (Connect PWA "Share New Health Information",
 * episphere/connect#1295). One Firestore doc per diagnosis in `selfReportCancerDx`:
 *   - Per-screen progress: the PWA POSTs its full current snapshot on every Next/Back. The
 *     participant's single in-progress doc is replaced (set, not merge). Back-deletion is
 *     free because the new snapshot lacks the cleared fields.
 *   - Submit validates the complete snapshot (rule matrix below), computes DxNumber (participant-wide
 *     diagnosis sequence), stamps the site's DxDt, strips the resume-only stateJSON/positionJSON strings,
 *     and finalizes the doc in place. Submitted diagnosis docs are append-only and never editable afterward.
 *   - Format: Quest-flat D_<cid> scalar keys plus dictionary-nested detail keys:
 *     `D_<parentCid>_D_<childCid>` and `D_<parentCid>_D_<childCid>_<counter>` for
 *     repeated treatment physicians/facilities.
 *   - ALL timestamps use Connect's canonical ISO8601 form (`YYYY-MM-DDTHH:mm:ss.SSSZ`);
 *     docLastUpdatedTimestamp is refreshed server-side on every write.
 */

const SELF_REPORT_CANCER_DX_COLLECTION = 'selfReportCancerDx';

const selfReportCancerCIDs = fieldMapping.selfReportCancerDx;
const DOC_LAST_UPDATED = fieldMapping.docLastUpdatedTimestamp;
const DX_NUMBER_KEY = `D_${selfReportCancerCIDs.dxNumber}`;
const STARTED_AT_KEY = 'startedAt';
const YES = String(fieldMapping.yes);
const NO = String(fieldMapping.no);
const YES_NO = new Set([YES, NO]);

const MONTH_CIDS = new Set(selfReportCancerCIDs.monthResponses.map(String));
const SITE_CIDS = new Set(selfReportCancerCIDs.siteResponses.map(String));
const ELIGIBLE_SITE_CIDS = new Set(selfReportCancerCIDs.screeningEligibleSiteCids.map(String));
const OTHER_SITE_CID = String(fieldMapping.cancerSites.other);

const isRealCid = (cid) => typeof cid === 'number'; // NPI cids are TODO strings until assigned
const TX_TYPE_CIDS = [selfReportCancerCIDs.treatment.chemo, selfReportCancerCIDs.treatment.surgery, selfReportCancerCIDs.treatment.radiation, selfReportCancerCIDs.treatment.other];
const TX_SINGLE_CHILD_CIDS = [
    selfReportCancerCIDs.treatment.startMonth, selfReportCancerCIDs.treatment.startYear, selfReportCancerCIDs.treatment.endMonth, selfReportCancerCIDs.treatment.endYear,
    selfReportCancerCIDs.treatment.ongoing,
].filter(isRealCid);
const TX_REPEATED_CHILD_CIDS = [
    selfReportCancerCIDs.treatment.physFirstName, selfReportCancerCIDs.treatment.physLastName, selfReportCancerCIDs.treatment.physNpi,
    ...Object.values(selfReportCancerCIDs.treatment.facility),
].filter(isRealCid);
const TX_NESTED_CHILD_CIDS = [...TX_SINGLE_CHILD_CIDS, ...TX_REPEATED_CHILD_CIDS];
const SCRN_OPTION_CIDS = Object.values(selfReportCancerCIDs.screening.optionValues);
const SCRN_NESTED_CHILD_CIDS = [
    selfReportCancerCIDs.screening.month, selfReportCancerCIDs.screening.year, selfReportCancerCIDs.screening.phyFirstName, selfReportCancerCIDs.screening.phyLastName,
    selfReportCancerCIDs.screening.phyNpi, ...Object.values(selfReportCancerCIDs.screening.facility),
].filter(isRealCid);

// Every cid a client may legitimately send as a D_<cid> question key. The shape check is strict against this set.
// The TODO NPI cids self-include once assigned. isRealCid drops them while they remain strings.
const KNOWN_QUESTION_CIDS = new Set([
    selfReportCancerCIDs.primarySite, selfReportCancerCIDs.primarySiteOther, selfReportCancerCIDs.dxMonth, selfReportCancerCIDs.dxYear, selfReportCancerCIDs.txReceived,
    ...TX_TYPE_CIDS, selfReportCancerCIDs.treatment.otherDescribe, ...TX_NESTED_CHILD_CIDS,
    selfReportCancerCIDs.screening.detected, ...SCRN_OPTION_CIDS, ...SCRN_NESTED_CHILD_CIDS,
].filter(isRealCid).map(String));

// Structural classes within the set. A well-formed key with a known cid can still be malformed
// (a scalar cid wearing a nested child, a child cid used without its parent, or an unsupported
// parent/child pair) and would still write a junk BigQuery column. So the shape check enforces
// scalar vs dictionary-nested keys.
const SCALAR_CIDS = new Set([
    selfReportCancerCIDs.primarySite, selfReportCancerCIDs.primarySiteOther, selfReportCancerCIDs.dxMonth, selfReportCancerCIDs.dxYear, selfReportCancerCIDs.txReceived,
    ...TX_TYPE_CIDS, selfReportCancerCIDs.treatment.otherDescribe,
    selfReportCancerCIDs.screening.detected, ...SCRN_OPTION_CIDS,
].map(String));
const TX_PARENT_CIDS = new Set(TX_TYPE_CIDS.map(String));
const SCRN_PARENT_CIDS = new Set(SCRN_OPTION_CIDS.map(String));
const TX_SINGLE_CHILD_CID_SET = new Set(TX_SINGLE_CHILD_CIDS.map(String));
const TX_REPEATED_CHILD_CID_SET = new Set(TX_REPEATED_CHILD_CIDS.map(String));
const SCRN_CHILD_CID_SET = new Set(SCRN_NESTED_CHILD_CIDS.map(String));
const MAX_LOOP_POSITION = 10;  // <=10 physicians per the spec. <= 10 facilities per the spec.

const KEY_RE = /^D_(\d{9})(?:_D_(\d{9})(?:_([1-9]\d?))?)?$/;
const OP_KEYS = new Set(['stateJSON', 'positionJSON', String(selfReportCancerCIDs.surveyLanguage), String(DOC_LAST_UPDATED)]);
// Strip server-owned metadata if sent by a client.
const SERVER_OWNED_KEYS = new Set([
    DX_NUMBER_KEY,
    ...Object.values(selfReportCancerCIDs.siteToDxDtCid).map((cid) => `D_${cid}`),
    STARTED_AT_KEY, 'uid', 'token', 'Connect_ID',
]);

const MAX_D_VALUE_LENGTH = 800;   // spec write-in cap
const MAX_STATE_JSON = 400000;
const MAX_POSITION_JSON = 50000;
const MAX_KEYS = 1500;
const MAX_BODY_LENGTH = 700000;    // headroom under Firestore's 1 MiB doc limit

const YEAR_RE = /^(19|20)\d{2}$/;
const isValidYear = (v, maxYear) => typeof v === 'string' && YEAR_RE.test(v) && Number(v) <= maxYear;

/** Remove server-owned keys silently (lockedAttributes precedent) — spoofs never reject. */
const stripServerOwnedKeys = (body) => {
    const out = {};
    for (const [k, v] of Object.entries(body)) {
        if (!SERVER_OWNED_KEYS.has(k)) out[k] = v;
    }
    return out;
};

/**
 * Validate save and submit snapshots. Strict key whitelist, string-typed D_ values, parseable operational strings, and size caps.
 * @returns {string[]} errors (empty = valid)
 */
const validateSnapshotShape = (body) => {
    const errors = [];
    const keys = Object.keys(body);
    if (keys.length > MAX_KEYS) errors.push(`too many keys (${keys.length} > ${MAX_KEYS})`);
    if (JSON.stringify(body).length > MAX_BODY_LENGTH) errors.push('body too large');

    const badKeys = [];
    for (const key of keys) {
        if (OP_KEYS.has(key)) continue;
        const match = KEY_RE.exec(key);
        // Reject anything that isn't D_<knownScalarCid>, D_<txTypeCid>_D_<txChildCid>,
        // D_<txTypeCid>_D_<repeatedTxChildCid>_<counter>, or D_<screeningOptionCid>_D_<screeningChildCid>.
        if (!match) { badKeys.push(key); continue; }
        const [, parentOrScalarCid, childCid, positionStr] = match;
        if (childCid === undefined) {
            if (!SCALAR_CIDS.has(parentOrScalarCid) || !KNOWN_QUESTION_CIDS.has(parentOrScalarCid)) {
                badKeys.push(key); continue;
            }
        } else if (TX_PARENT_CIDS.has(parentOrScalarCid)) {
            if (TX_SINGLE_CHILD_CID_SET.has(childCid)) {
                if (positionStr !== undefined) { badKeys.push(key); continue; }
            } else if (TX_REPEATED_CHILD_CID_SET.has(childCid)) {
                if (positionStr === undefined || Number(positionStr) > MAX_LOOP_POSITION) {
                    badKeys.push(key); continue;
                }
            } else {
                badKeys.push(key); continue;
            }
        } else if (SCRN_PARENT_CIDS.has(parentOrScalarCid)) {
            if (!SCRN_CHILD_CID_SET.has(childCid) || positionStr !== undefined) {
                badKeys.push(key); continue;
            }
        } else {
            badKeys.push(key); continue;
        }
        const value = body[key];
        if (typeof value !== 'string') errors.push(`${key} must be a string`);
        else if (value.length > MAX_D_VALUE_LENGTH) errors.push(`${key} exceeds ${MAX_D_VALUE_LENGTH} chars`);
    }
    if (badKeys.length) errors.push(`invalid keys: ${badKeys.join(', ')}`);

    for (const [opKey, cap] of [['stateJSON', MAX_STATE_JSON], ['positionJSON', MAX_POSITION_JSON]]) {
        const value = body[opKey];
        if (value === undefined) continue;
        if (typeof value !== 'string' || value.length > cap || safeJSONParse(value) === null) {
            errors.push(`${opKey} must be a parseable JSON string under ${cap} chars`);
        }
    }
    if (body[String(DOC_LAST_UPDATED)] !== undefined) {
        const validation = validateIso8601Timestamp(body[String(DOC_LAST_UPDATED)]);
        if (validation.error) errors.push(`${DOC_LAST_UPDATED} must be a canonical ISO timestamp (${validation.message})`);
    }
    return errors;
};

const nestedKey = (parentCid, childCid, position) =>
    ['D_' + parentCid, 'D_' + childCid, position].filter((part) => part !== undefined).join('_');

/** Extract dictionary-nested keys from a snapshot. */
const nestedKeysOf = (body, parentCids, childCids) => {
    const parentSet = new Set(parentCids.map(String));
    const childSet = new Set(childCids.map(String));
    const out = [];
    for (const key of Object.keys(body)) {
        const match = KEY_RE.exec(key);
        if (match && match[2] !== undefined && parentSet.has(match[1]) && childSet.has(match[2])) {
            out.push({
                key,
                parentCid: match[1],
                cid: match[2],
                position: match[3] === undefined ? undefined : Number(match[3]),
            });
        }
    }
    return out;
};
const hasAnyKey = (body, cids) =>
    Object.keys(body).some((key) => {
        const match = KEY_RE.exec(key);
        return match && match[2] === undefined && cids.map(String).includes(match[1]);
    });

/**
 * The submit rule matrix (rules 1-13). Returns { error, message }.
 * Same-site/same-year resubmits are accepted and DxNumber increments.
 * If duplicate blocking is ever needed, add one isolated rule here:
 *   - Reject (409) when an existing submitted diagnosis matches on D_181737942 (site) AND D_908235757 (year).
 */
const validateSubmission = (body) => {
    const fail = (message) => ({ error: true, message });
    const currentYear = new Date().getFullYear();

    // Rule 1: site
    const site = body[`D_${selfReportCancerCIDs.primarySite}`];
    if (!site || !SITE_CIDS.has(site)) return fail(`Invalid or missing primary site (${selfReportCancerCIDs.primarySite}).`);

    // Rule 2: optional other-describe. If supplied, it must only accompany primary site = Other.
    const describe = body[`D_${selfReportCancerCIDs.primarySiteOther}`];
    const describeValid = describe === undefined || (typeof describe === 'string' && describe.trim().length <= 800);
    if (site === OTHER_SITE_CID ? !describeValid : describe !== undefined) {
        return fail(`Other-describe (${selfReportCancerCIDs.primarySiteOther}) is allowed only when primary site is Other.`);
    }

    // Rule 3: diagnosis year (never in the future)
    if (!isValidYear(body[`D_${selfReportCancerCIDs.dxYear}`], currentYear)) return fail(`Invalid or missing diagnosis year (${selfReportCancerCIDs.dxYear}).`);

    // Rule 4: diagnosis month
    const dxMonth = body[`D_${selfReportCancerCIDs.dxMonth}`];
    if (dxMonth !== undefined && !MONTH_CIDS.has(dxMonth)) return fail(`Invalid diagnosis month (${selfReportCancerCIDs.dxMonth}).`);

    // Rule 5: treatment received is optional. If omitted, the treatment section was not answered.
    const txReceived = body[`D_${selfReportCancerCIDs.txReceived}`];

    const txNestedKeys = nestedKeysOf(body, TX_TYPE_CIDS, TX_NESTED_CHILD_CIDS);
    if (txReceived === undefined) {
        if (hasAnyKey(body, [...TX_TYPE_CIDS, selfReportCancerCIDs.treatment.otherDescribe]) || txNestedKeys.length) {
            return fail('Treatment fields are not allowed when treatment received is unanswered.');
        }
    } else if (!YES_NO.has(txReceived)) {
        return fail(`Invalid treatment-received flag (${selfReportCancerCIDs.txReceived}).`);
    } else if (txReceived === NO) {
        // Rule 9: section never displayed -> zero treatment keys
        if (hasAnyKey(body, [...TX_TYPE_CIDS, selfReportCancerCIDs.treatment.otherDescribe]) || txNestedKeys.length) {
            return fail('Treatment fields are not allowed when treatment received is No.');
        }
    } else {
        // Rule 6: the full treatment-type group must be present as explicit Yes/No. The frontend emits
        // every displayed select-all option. A partial group breaks the Quest-flat analytics contract.
        // Zero selected treatment types is allowed; it represents Q3 = Yes with no type selected.
        const flags = TX_TYPE_CIDS.map((cid) => body[`D_${cid}`]);
        if (flags.some((v) => !YES_NO.has(v))) return fail('Each treatment type flag (chemo/surgery/radiation/other) must be present as Yes/No when treatment received is Yes.');
        const selectedTxParents = TX_TYPE_CIDS.map(String).filter((cid) => body[`D_${cid}`] === YES);
        for (const { parentCid } of txNestedKeys) {
            if (!selectedTxParents.includes(parentCid)) return fail(`Treatment detail fields are not allowed for unselected treatment type ${parentCid}.`);
        }
        for (const parentCid of selectedTxParents) {
            if (!isValidYear(body[nestedKey(parentCid, selfReportCancerCIDs.treatment.startYear)], currentYear + 5)) {
                return fail(`Invalid or missing treatment start year for treatment type ${parentCid}.`);
            }
            const ongoing = body[nestedKey(parentCid, selfReportCancerCIDs.treatment.ongoing)];
            if (!YES_NO.has(ongoing)) return fail(`Invalid or missing ongoing flag for treatment type ${parentCid}.`);
            const endYear = body[nestedKey(parentCid, selfReportCancerCIDs.treatment.endYear)];
            const endMonth = body[nestedKey(parentCid, selfReportCancerCIDs.treatment.endMonth)];
            if (ongoing === YES && (endYear !== undefined || endMonth !== undefined)) {
                return fail(`Ongoing treatments must not carry an end date (treatment type ${parentCid}).`);
            }
            if (endYear !== undefined && !isValidYear(endYear, currentYear + 5)) {
                return fail(`Invalid treatment end year for treatment type ${parentCid}.`);
            }
        }
        // Rule 8: optional flat other-describe. If supplied, it must only accompany the Other type flag.
        const otherSelected = body[`D_${selfReportCancerCIDs.treatment.other}`] === YES;
        const txDescribe = body[`D_${selfReportCancerCIDs.treatment.otherDescribe}`];
        const txDescribeValid = txDescribe === undefined || (typeof txDescribe === 'string' && txDescribe.trim().length <= 800);
        if (otherSelected ? !txDescribeValid : txDescribe !== undefined) {
            return fail(`Treatment other-describe (${selfReportCancerCIDs.treatment.otherDescribe}) is allowed only when the Other treatment type is selected.`);
        }
    }

    // Rule 10-12: screening
    const detected = body[`D_${selfReportCancerCIDs.screening.detected}`];
    const scrnNestedKeys = nestedKeysOf(body, SCRN_OPTION_CIDS, SCRN_NESTED_CHILD_CIDS);
    const optionFlags = SCRN_OPTION_CIDS.filter((cid) => body[`D_${cid}`] !== undefined);
    if (!ELIGIBLE_SITE_CIDS.has(site)) {
        if (detected !== undefined || optionFlags.length || scrnNestedKeys.length) {
            return fail('Screening fields are only allowed for breast, colon/rectal, and lung diagnoses.');
        }
    } else {
        if (!YES_NO.has(detected)) return fail(`Invalid or missing screening-detected flag (${selfReportCancerCIDs.screening.detected}).`);
        if (detected === NO) {
            if (optionFlags.length || scrnNestedKeys.length) {
                return fail('Screening details are not allowed when screening-detected is No.');
            }
        } else {
            const siteOptions = (selfReportCancerCIDs.screening.optionCidsBySiteCid[Number(site)] || []).map(String);
            // No option from a different site
            const wrongSite = optionFlags.filter((cid) => !siteOptions.includes(String(cid)));
            if (wrongSite.length) return fail(`Screening option(s) not valid for this site: ${wrongSite.join(', ')}.`);
            // The full option group this site displays must be present as explicit Yes/No.
            // A partial group breaks the contract. Matches the frontend's select-all.
            for (const cid of siteOptions) {
                if (!YES_NO.has(body[`D_${cid}`])) return fail(`Each screening option for this site must be present as Yes/No when screening-detected is Yes (missing or invalid ${cid}).`);
            }
            const selectedScreeningParents = siteOptions.filter((cid) => body[`D_${cid}`] === YES);
            if (selectedScreeningParents.length === 0) return fail('At least one screening type is required when screening-detected is Yes.');
            for (const { parentCid } of scrnNestedKeys) {
                if (!selectedScreeningParents.includes(parentCid)) return fail(`Screening detail fields are not allowed for unselected screening type ${parentCid}.`);
            }
            for (const parentCid of selectedScreeningParents) {
                if (!isValidYear(body[nestedKey(parentCid, selfReportCancerCIDs.screening.year)], currentYear + 5)) {
                    return fail(`Invalid or missing screening year for screening type ${parentCid}.`);
                }
            }
        }
    }

    // Rule 13: every month-valued nested key. NPI cids once their cids are assigned.
    const monthCids = [selfReportCancerCIDs.treatment.startMonth, selfReportCancerCIDs.treatment.endMonth, selfReportCancerCIDs.screening.month].filter(isRealCid);
    for (const { key, cid } of [...txNestedKeys, ...scrnNestedKeys]) {
        if (monthCids.map(String).includes(cid) && !MONTH_CIDS.has(body[key])) {
            return fail(`Invalid month value at ${key}.`);
        }
        const npiCids = [selfReportCancerCIDs.treatment.physNpi, selfReportCancerCIDs.screening.phyNpi].filter(isRealCid).map(String);
        if (npiCids.includes(cid) && !/^\d{10}$/.test(body[key])) {
            return fail(`Invalid NPI at ${key}.`);
        }
    }

    return { error: false, message: 'Success!' };
};

/**
 * Participant-wide diagnosis sequence: count of already-submitted diagnoses, + 1.
 */
const computeDxNumber = (submittedDiagnoses) => String(submittedDiagnoses.length + 1);

const hasOwn = (object, key) => Object.prototype.hasOwnProperty.call(object, key);
const isSubmittedDiagnosis = (diagnosis = {}) => diagnosis !== null && typeof diagnosis === 'object' && hasOwn(diagnosis, DX_NUMBER_KEY);

const submittedTimestampOf = (diagnosis = {}) => {
    const siteCid = diagnosis[`D_${selfReportCancerCIDs.primarySite}`];
    const dxDtCid = selfReportCancerCIDs.siteToDxDtCid[Number(siteCid)];
    return dxDtCid ? diagnosis[`D_${dxDtCid}`] || '' : '';
};

const startedAtOf = (diagnosis = {}) => diagnosis[STARTED_AT_KEY] || '';

/** Partition Firestore docs for the query. Newest startedAt wins if in-progress docs multiply. */
const partitionDiagnosisDocs = (diagnosisDocs) => {
    const inProgressDocs = diagnosisDocs.filter((doc) => !isSubmittedDiagnosis(doc.data));
    const submittedDocs = diagnosisDocs.filter((doc) => isSubmittedDiagnosis(doc.data));
    let inProgressDoc = null;
    if (inProgressDocs.length) {
        inProgressDoc = inProgressDocs.reduce((a, b) => (String(startedAtOf(a.data)) >= String(startedAtOf(b.data)) ? a : b));
        if (inProgressDocs.length > 1) {
            console.error(`selfReportCancerDx: ${inProgressDocs.length} in-progress docs for one participant; using the newest.`);
        }
    }
    return { inProgressDoc, submittedDocs };
};

/** Parse the request body. */

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
 * Server-side write eligibility for a new self-reported diagnosis (PHI).
 * Verification status must equal `verified`, and withdrawal may not be Yes.
 * Returns a short reason when ineligible, else null.
 */
const ineligibilityReason = (profile) => {
    if (profile[fieldMapping.verificationStatus] !== fieldMapping.verified) return 'participant is not verified';
    if (profile[fieldMapping.withdrawConsent] === fieldMapping.yes) return 'participant has withdrawn consent';
    return null;
};

/**
 * Common POST setup for self-report diagnosis writes.
 */
const prepareWriteRequest = async (req, res, uid) => {
    logIPAddress(req);
    setHeaders(res);
    if (req.method !== 'POST') {
        res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        return null;
    }

    const raw = parseBody(req);
    if (!raw) {
        res.status(400).json(getResponseJSON('Bad request: empty submission.', 400));
        return null;
    }

    const body = stripServerOwnedKeys(raw);
    const shapeErrors = validateSnapshotShape(body);
    if (shapeErrors.length) {
        res.status(400).json(getResponseJSON(`Bad request: ${shapeErrors.join('; ')}`, 400));
        return null;
    }

    const participant = await loadParticipant(uid);
    if (!participant) {
        res.status(404).json(getResponseJSON('Token not found!', 404));
        return null;
    }

    const ineligible = ineligibilityReason(participant.profile);
    if (ineligible) {
        res.status(403).json(getResponseJSON(`Not eligible to report a new diagnosis: ${ineligible}.`, 403));
        return null;
    }

    return { body, participant };
};

const saveProgressSnapshot = async (res, uid, body, participant) => {
    const { getSelfReportCancerDxDocs, writeSelfReportCancerDxDoc } = require('./firestore');
    const { inProgressDoc } = await getSelfReportCancerDxDocs(uid);
    const now = new Date().toISOString();
    const diagnosisDoc = {
        ...body,
        [DOC_LAST_UPDATED]: now, // server-authoritative, whatever the client sent
        uid,
        token: participant.token,
        Connect_ID: participant.connectId,
        [STARTED_AT_KEY]: startedAtOf(inProgressDoc?.data) || now,
    };
    
    await writeSelfReportCancerDxDoc(inProgressDoc?.docId ?? null, diagnosisDoc, { guardSubmitted: true });
    return res.status(200).json(getResponseJSON('Progress saved successfully!', 200));
};

const submitDiagnosisSnapshot = async (res, uid, body, participant) => {
    const validation = validateSubmission(body);
    if (validation.error) return res.status(400).json(getResponseJSON(validation.message, 400));

    const { submitSelfReportCancerDxTransaction } = require('./firestore');
    const siteCid = body[`D_${selfReportCancerCIDs.primarySite}`];
    const now = new Date().toISOString(); // One timestamp for the site's DxDt + lastUpdated
    const { stateJSON, positionJSON, ...surveyFields } = body;
    // DxNumber is participant-wide. Computing it from submitted diagnoses and writing the finalized doc in
    // one transaction makes concurrent submits retry and receive distinct numbers.
    await submitSelfReportCancerDxTransaction(uid, ({ inProgressDoc, submittedDiagnoses }) => ({
        ...surveyFields,
        [DX_NUMBER_KEY]: computeDxNumber(submittedDiagnoses),
        [`D_${selfReportCancerCIDs.siteToDxDtCid[Number(siteCid)]}`]: now,
        [DOC_LAST_UPDATED]: now,
        uid,
        token: participant.token,
        Connect_ID: participant.connectId,
    }));
    return res.status(200).json(getResponseJSON('Diagnosis submitted successfully!', 200));
};

/**
 * Combined write endpoint for the self-report cancer diagnosis module.
 * Use query action=save for in-progress snapshots and action=submit for finalization.
 */
const storeSelfReportCancerDx = async (req, res, uid) => {
    const action = String(req.query?.action || '').trim().toLowerCase();
    if (!['save', 'submit'].includes(action)) {
        logIPAddress(req);
        setHeaders(res);
        if (req.method !== 'POST') return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        return res.status(400).json(getResponseJSON("Bad request: action must be 'save' or 'submit'.", 400));
    }

    const context = await prepareWriteRequest(req, res, uid);
    if (!context) return undefined;

    return action === 'submit'
        ? submitDiagnosisSnapshot(res, uid, context.body, context.participant)
        : saveProgressSnapshot(res, uid, context.body, context.participant);
};

/**
 * Save the in-progress snapshot: upsert-replace the participant's single in-progress doc.
 * */
const saveSelfReportCancerDxProgress = async (req, res, uid) => {
    const context = await prepareWriteRequest(req, res, uid);
    if (!context) return undefined;
    return saveProgressSnapshot(res, uid, context.body, context.participant);
};

/**
 * Validate and finalize one append-only diagnosis doc. Reuses the in-progress doc when present.
 * */
const submitSelfReportCancerDx = async (req, res, uid) => {
    const context = await prepareWriteRequest(req, res, uid);
    if (!context) return undefined;
    return submitDiagnosisSnapshot(res, uid, context.body, context.participant);
};

/**
 * Resume and read previously-reported diagnoses.
 * Returns { inProgress (stateJSON intact) | null, submitted[] ascending by site DxDt }.
 * */
const getSelfReportCancerDx = async (req, res, uid) => {
    logIPAddress(req);
    setHeaders(res);
    if (req.method !== 'GET') return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));

    const { getSelfReportCancerDxDocs } = require('./firestore');
    const { inProgressDoc, submittedDiagnoses } = await getSelfReportCancerDxDocs(uid);
    const sortedSubmitted = [...submittedDiagnoses].sort((a, b) => String(submittedTimestampOf(a)).localeCompare(String(submittedTimestampOf(b))));
    return res.status(200).json({ data: { inProgress: inProgressDoc?.data ?? null, submitted: sortedSubmitted }, code: 200 });
};

module.exports = {
    SELF_REPORT_CANCER_DX_COLLECTION,
    storeSelfReportCancerDx,
    saveSelfReportCancerDxProgress,
    submitSelfReportCancerDx,
    getSelfReportCancerDx,
    stripServerOwnedKeys,
    validateSnapshotShape,
    validateSubmission,
    ineligibilityReason,
    computeDxNumber,
    isSubmittedDiagnosis,
    partitionDiagnosisDocs,
    submittedTimestampOf,
};
