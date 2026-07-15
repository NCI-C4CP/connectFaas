/**
 * Self-Report Cancer Diagnosis endpoint tests (save / submit / get).
 *
 * Harness: setupTestSuite installs the firebase-admin require-cache mocks so utils/firestore loads safely (apiEndpoints/sites pattern).
 * Handlers require firestore INLINE, so vi.spyOn(firestore, ...) intercepts through the CommonJS cache.
 * Requests via node-mocks-http with the x-forwarded-for + connection pair logIPAddress dereferences.
 */

const httpMocks = require('node-mocks-http');
const { setupTestSuite } = require('../shared/testHelpers');

let firestore;
let mod;
let fieldMapping;
let selfReportCancerCIDs;
let cancerSiteCIDs;
let monthCIDs;
let YES;
let NO;

beforeAll(() => {
    setupTestSuite({ setupConsole: false, setupModuleMocks: true });
    firestore = require('../../utils/firestore');
    mod = require('../../utils/selfReportCancerDx');
    fieldMapping = require('../../utils/fieldToConceptIdMapping');
    selfReportCancerCIDs = fieldMapping.selfReportCancerDx;
    cancerSiteCIDs = fieldMapping.cancerSites;
    monthCIDs = selfReportCancerCIDs.monthResponses.map(String);
    YES = String(fieldMapping.yes);
    NO = String(fieldMapping.no);
});

const UID = 'uid-test-1';
const DX_NUMBER_KEY = 'D_480939157';
const BREAST_DXDT_KEY = 'D_104045590';
const PROSTATE_DXDT_KEY = 'D_199928758';
// Keep payload keys literal to mirror Quest-like D_<cid> snapshots. Map response cids where names exist.
const responseCid = (cid) => String(cid);
const dKey = (cid, ...positions) => ['D_' + cid, ...positions].filter((part) => part !== undefined).join('_');
const treatmentRepeatKey = (childCid, position) => dKey(childCid, position, position);
const primarySitePayload = (siteCid, otherText) => {
    const group = { [dKey(selfReportCancerCIDs.primarySite)]: responseCid(siteCid) };
    if (otherText !== undefined) group[dKey(selfReportCancerCIDs.primarySiteOther)] = otherText;
    return { [dKey(selfReportCancerCIDs.sourceQuestions.primarySite)]: group };
};

const invoke = async (handler, method, body, query = {}) => {
    const req = httpMocks.createRequest({
        method,
        headers: { 'x-forwarded-for': 'dummy' },
        connection: {},
        body,
        query,
    });
    const res = httpMocks.createResponse();
    await handler(req, res, UID);
    return res;
};

const OP = { stateJSON: '{"v":3,"state":{}}', positionJSON: '{"screenId":"diagnosisDate"}' };

// Minimal valid submit: prostate (not screening-eligible), no treatment.
const minimalSubmit = () => ({
    ...primarySitePayload(cancerSiteCIDs.prostate),
    D_908235757: '2024',
    D_874288004: NO,
    ...OP,
});

// Full valid submit: breast + chemo, ongoing + screening breast2D.
const breastSubmit = () => ({
    ...primarySitePayload(cancerSiteCIDs.breast),
    D_299768751: monthCIDs[10],
    D_908235757: '2024',
    D_874288004: YES,
    D_388069854: {
        D_244216107: YES, D_293873603: NO, D_555019890: NO, D_459406752: NO,
    },
    D_244216107: {
        D_281136649: '2024',
        D_566057154: { D_735592270: YES },
    },
    D_944065539: YES,
    D_130601750: {
        D_425815239: YES, D_759642936: NO, D_528508094: NO,
        D_502929020: NO, D_412252588: NO,
    },
    D_425815239: { D_858052564: '2017' },
    ...OP,
});

let writeSpy;

beforeEach(() => {
    vi.spyOn(firestore, 'retrieveUserProfile').mockResolvedValue({
        token: 'tok-1', Connect_ID: 1234567890,
        [fieldMapping.verificationStatus]: fieldMapping.verified, // verified + active => eligible.
    });
    vi.spyOn(firestore, 'getSelfReportCancerDxDocs').mockResolvedValue({ inProgressDoc: null, submittedDiagnoses: [] });
    writeSpy = vi.spyOn(firestore, 'writeSelfReportCancerDxDoc').mockResolvedValue();
    // submit finalizes in a transaction. Bridge it to the same getSelfReportCancerDxDocs mock and writeSpy.
    vi.spyOn(firestore, 'submitSelfReportCancerDxTransaction').mockImplementation(async (uid, buildFinalDoc) => {
        const { inProgressDoc, submittedDiagnoses } = await firestore.getSelfReportCancerDxDocs(uid);
        const doc = buildFinalDoc({ inProgressDoc, submittedDiagnoses });
        writeSpy(inProgressDoc?.docId ?? null, doc);
        return { docId: inProgressDoc?.docId ?? 'generated-id' };
    });
});

afterEach(() => {
    vi.restoreAllMocks();
});

const writtenDoc = () => writeSpy.mock.calls.at(-1)[1];
const writtenDocId = () => writeSpy.mock.calls.at(-1)[0];
const ISO_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

describe('storeSelfReportCancerDx — combined write endpoint', () => {
    it('requires action=save or action=submit', async () => {
        const res = await invoke(mod.storeSelfReportCancerDx, 'POST', minimalSubmit());
        expect(res.statusCode).toBe(400);
        expect(res._getJSONData().message).toContain("action must be 'save' or 'submit'");
    });

    it('action=save stores progress without finalizing the diagnosis', async () => {
        const res = await invoke(mod.storeSelfReportCancerDx, 'POST', minimalSubmit(), { action: 'save' });
        expect(res.statusCode).toBe(200);
        expect(writtenDocId()).toBeNull();
        expect(writtenDoc()[DX_NUMBER_KEY]).toBeUndefined();
    });

    it('action=submit validates and finalizes the diagnosis', async () => {
        const res = await invoke(mod.storeSelfReportCancerDx, 'POST', minimalSubmit(), { action: 'submit' });
        expect(res.statusCode).toBe(200);
        expect(writtenDoc()[DX_NUMBER_KEY]).toBe('1');
        expect(writtenDoc()[PROSTATE_DXDT_KEY]).toMatch(ISO_RE);
    });
});

describe('saveSelfReportCancerDxProgress — guards & shape', () => {
    it('rejects non-POST', async () => {
        const res = await invoke(mod.saveSelfReportCancerDxProgress, 'GET', {});
        expect(res.statusCode).toBe(405);
    });

    it('rejects an empty body', async () => {
        const res = await invoke(mod.saveSelfReportCancerDxProgress, 'POST', {});
        expect(res.statusCode).toBe(400);
    });

    it('rejects every malformed key in ONE 400 listing all offenders', async () => {
        const res = await invoke(mod.saveSelfReportCancerDxProgress, 'POST', {
            ...minimalSubmit(),
            foo: 'x',
            D_12: 'x',
            D_123456789_1: 'x',     // single index
            D_123456789_0_1: 'x',   // zero index
        });
        expect(res.statusCode).toBe(400);
        const msg = res._getJSONData().message;
        for (const k of ['foo', 'D_12', 'D_123456789_1', 'D_123456789_0_1']) expect(msg).toContain(k);
    });

    it('rejects a well-formed but unknown cid (strict whitelist, not just format)', async () => {
        const save = await invoke(mod.saveSelfReportCancerDxProgress, 'POST', { ...minimalSubmit(), D_999999999: 'x' });
        expect(save.statusCode).toBe(400);
        expect(save._getJSONData().message).toContain('D_999999999');
        const submit = await invoke(mod.submitSelfReportCancerDx, 'POST', { ...minimalSubmit(), D_999999999: 'x' });
        expect(submit.statusCode).toBe(400);
    });

        it('rejects unspeced completion metadata', async () => {
        const save = await invoke(mod.saveSelfReportCancerDxProgress, 'POST', { ...minimalSubmit(), COMPLETED: true });
        expect(save.statusCode).toBe(400);
        expect(save._getJSONData().message).toContain('COMPLETED');
        const submit = await invoke(mod.submitSelfReportCancerDx, 'POST', { ...minimalSubmit(), COMPLETED_TS: '2026-01-01T00:00:00.000Z' });
        expect(submit.statusCode).toBe(400);
        expect(submit._getJSONData().message).toContain('COMPLETED_TS');
    });

    it('accepts source-question maps, detail maps, and the operational keys', async () => {
        const res = await invoke(mod.saveSelfReportCancerDxProgress, 'POST', {
            ...minimalSubmit(),
            [dKey(selfReportCancerCIDs.sourceQuestions.treatmentType)]: {
                [dKey(selfReportCancerCIDs.treatment.chemo)]: YES,
                [dKey(selfReportCancerCIDs.treatment.surgery)]: NO,
                [dKey(selfReportCancerCIDs.treatment.radiation)]: NO,
                [dKey(selfReportCancerCIDs.treatment.other)]: NO,
            },
            [dKey(selfReportCancerCIDs.treatment.chemo)]: {
                [dKey(selfReportCancerCIDs.treatment.startYear)]: '2024',
                [treatmentRepeatKey(selfReportCancerCIDs.treatment.physFirstName, 10)]: 'Maya',
            },
            [selfReportCancerCIDs.surveyLanguage]: fieldMapping.english, // surveyLanguage: numeric value allowed
            [fieldMapping.docLastUpdatedTimestamp]: '2026-06-12T00:00:00.000Z', // lastUpdated: ISO string
        });
        expect(res.statusCode).toBe(200);
    });

    it('allows only known survey-language cids when survey language is supplied', async () => {
        const english = await invoke(mod.saveSelfReportCancerDxProgress, 'POST', {
            ...minimalSubmit(),
            [selfReportCancerCIDs.surveyLanguage]: fieldMapping.english,
        });
        expect(english.statusCode).toBe(200);

        const bad = await invoke(mod.saveSelfReportCancerDxProgress, 'POST', {
            ...minimalSubmit(),
            [selfReportCancerCIDs.surveyLanguage]: 999999999,
        });
        expect(bad.statusCode).toBe(400);
        expect(bad._getJSONData().message).toContain(String(selfReportCancerCIDs.surveyLanguage));
    });

    it('rejects non-string D_ values and oversized values', async () => {
        const numeric = await invoke(mod.saveSelfReportCancerDxProgress, 'POST', { ...minimalSubmit(), D_908235757: 2024 });
        expect(numeric.statusCode).toBe(400);
        const oversized = await invoke(mod.saveSelfReportCancerDxProgress, 'POST', {
            ...minimalSubmit(),
            ...primarySitePayload(cancerSiteCIDs.other, 'x'.repeat(1001)),
        });
        expect(oversized.statusCode).toBe(400);
    });

    it('rejects unparseable operational strings', async () => {
        const res = await invoke(mod.saveSelfReportCancerDxProgress, 'POST', { ...minimalSubmit(), stateJSON: 'not json' });
        expect(res.statusCode).toBe(400);
    });

    it('rejects non-canonical ISO timestamp shapes for operational timestamp fields', async () => {
        const res = await invoke(mod.saveSelfReportCancerDxProgress, 'POST', {
            ...minimalSubmit(),
            [fieldMapping.docLastUpdatedTimestamp]: 'not-a-canonical-timestamp',
        });
        expect(res.statusCode).toBe(400);
        expect(res._getJSONData().message).toContain(String(fieldMapping.docLastUpdatedTimestamp));
    });

    it('silently STRIPS server-owned keys instead of rejecting', async () => {
        const res = await invoke(mod.saveSelfReportCancerDxProgress, 'POST', {
            ...minimalSubmit(),
            [DX_NUMBER_KEY]: '7',                    // DxNumber
            [BREAST_DXDT_KEY]: '2026-01-01T00:00:00.000Z', // breast DxDt
            startedAt: '1999-01-01T00:00:00.000Z',
            uid: 'spoofed',
        });
        expect(res.statusCode).toBe(200);
        const doc = writtenDoc();
        expect(DX_NUMBER_KEY in doc).toBe(false);
        expect(BREAST_DXDT_KEY in doc).toBe(false);
        expect(doc.startedAt).not.toBe('1999-01-01T00:00:00.000Z');
        expect(doc.startedAt).toMatch(ISO_RE);
        expect(doc.uid).toBe(UID);                  // server's own identity
    });

    it('refreshes docLastUpdatedTimestamp authoritatively, ignoring the client value', async () => {
        const res = await invoke(mod.saveSelfReportCancerDxProgress, 'POST', {
            ...minimalSubmit(), [fieldMapping.docLastUpdatedTimestamp]: '1999-01-01T00:00:00.000Z',
        });
        expect(res.statusCode).toBe(200);
        expect(writtenDoc()[fieldMapping.docLastUpdatedTimestamp]).not.toBe('1999-01-01T00:00:00.000Z');
        expect(writtenDoc()[fieldMapping.docLastUpdatedTimestamp]).toMatch(ISO_RE);
    });

    it('404s when the participant profile lacks a token', async () => {
        firestore.retrieveUserProfile.mockResolvedValue({});
        const res = await invoke(mod.saveSelfReportCancerDxProgress, 'POST', minimalSubmit());
        expect(res.statusCode).toBe(404);
    });
});

describe('saveSelfReportCancerDxProgress — upsert replace semantics', () => {
    it('first save creates (docId null) with no DxNumber, ISO startedAt, and identity', async () => {
        const res = await invoke(mod.saveSelfReportCancerDxProgress, 'POST', minimalSubmit());
        expect(res.statusCode).toBe(200);
        expect(writtenDocId()).toBeNull();
        const doc = writtenDoc();
        expect(DX_NUMBER_KEY in doc).toBe(false);
        expect(doc.startedAt).toMatch(ISO_RE);
        expect(doc.uid).toBe(UID);
        expect(doc.token).toBe('tok-1');
        expect(doc.Connect_ID).toBe(1234567890);
    });

    it('later saves replace the in-progress doc: dropped fields disappear, startedAt survives', async () => {
        firestore.getSelfReportCancerDxDocs.mockResolvedValue({
            inProgressDoc: {
                docId: 'doc-123',
                data: { D_874288004: NO, startedAt: '2026-06-10T10:00:00.000Z' },
            },
            submittedDiagnoses: [],
        });
        // Back from Q3: the new snapshot no longer carries D_874288004.
        const res = await invoke(mod.saveSelfReportCancerDxProgress, 'POST', {
            ...primarySitePayload(cancerSiteCIDs.prostate), D_908235757: '2024', ...OP,
        });
        expect(res.statusCode).toBe(200);
        expect(writtenDocId()).toBe('doc-123');
        const doc = writtenDoc();
        expect('D_874288004' in doc).toBe(false);                  // Back-deletion via replace
        expect(doc.startedAt).toBe('2026-06-10T10:00:00.000Z');    // carried forward
    });
});

describe('write eligibility: server-side gate on save & submit', () => {
    const eligibleProfile = (overrides) => ({
        token: 'tok-1', Connect_ID: 1234567890,
        [fieldMapping.verificationStatus]: fieldMapping.verified,
        ...overrides,
    });
    const handlers = [
        ['save', () => mod.saveSelfReportCancerDxProgress],
        ['submit', () => mod.submitSelfReportCancerDx],
    ];

    for (const [name, getHandler] of handlers) {
        it(`${name}: 403 when not verified`, async () => {
            firestore.retrieveUserProfile.mockResolvedValue({ token: 'tok-1', Connect_ID: 1234567890 }); // no verificationStatus
            const res = await invoke(getHandler(), 'POST', minimalSubmit());
            expect(res.statusCode).toBe(403);
        });
        it(`${name}: 403 when withdrawn`, async () => {
            firestore.retrieveUserProfile.mockResolvedValue(eligibleProfile({ [fieldMapping.withdrawConsent]: fieldMapping.yes }));
            expect((await invoke(getHandler(), 'POST', minimalSubmit())).statusCode).toBe(403);
        });
        it(`${name}: 200 when data-destruction requested`, async () => {
            firestore.retrieveUserProfile.mockResolvedValue(eligibleProfile({ [fieldMapping.participantMap.destroyData]: fieldMapping.yes }));
            expect((await invoke(getHandler(), 'POST', minimalSubmit())).statusCode).toBe(200);
        });
        it(`${name}: 200 when deceased (EMR or NORC)`, async () => {
            firestore.retrieveUserProfile.mockResolvedValue(eligibleProfile({ [fieldMapping.participantDeceased]: fieldMapping.yes }));
            expect((await invoke(getHandler(), 'POST', minimalSubmit())).statusCode).toBe(200);
            firestore.retrieveUserProfile.mockResolvedValue(eligibleProfile({ [fieldMapping.participantDeceasedNORC]: fieldMapping.yes }));
            expect((await invoke(getHandler(), 'POST', minimalSubmit())).statusCode).toBe(200);
        });
        it(`${name}: 200 for a verified, active participant`, async () => {
            expect((await invoke(getHandler(), 'POST', minimalSubmit())).statusCode).toBe(200);
        });
    }

    it('ineligibilityReason is null only for a verified, active profile', () => {
        expect(mod.ineligibilityReason(eligibleProfile())).toBeNull();
        expect(mod.ineligibilityReason(eligibleProfile({ [fieldMapping.participantMap.destroyData]: fieldMapping.yes }))).toBeNull();
        expect(mod.ineligibilityReason(eligibleProfile({ [fieldMapping.participantDeceased]: fieldMapping.yes }))).toBeNull();
        expect(mod.ineligibilityReason(eligibleProfile({ [fieldMapping.participantDeceasedNORC]: fieldMapping.yes }))).toBeNull();
        expect(mod.ineligibilityReason({ token: 't', Connect_ID: 1 })).toContain('not verified');
    });
});

describe('submitSelfReportCancerDx — validation matrix', () => {
    const submit = (body) => invoke(mod.submitSelfReportCancerDx, 'POST', body);
    const expect400 = async (body, fragment) => {
        const res = await submit(body);
        expect(res.statusCode, fragment).toBe(400);
        if (fragment) expect(res._getJSONData().message.toLowerCase()).toContain(fragment.toLowerCase());
    };

    it('Primary site required and must be a valid survey site cid', async () => {
        const noSite = minimalSubmit(); delete noSite.D_176158861;
        await expect400(noSite, 'primary site');
        await expect400({ ...minimalSubmit(), ...primarySitePayload(999999999) }, 'primary site');
        await expect400({ ...minimalSubmit(), ...primarySitePayload(cancerSiteCIDs.unavailableUnknown) }, 'primary site'); // unavailableUnknown: chart-review only
    });

    it('Primary site Other write-in is optional when site = other and forbidden otherwise', async () => {
        const forbidden = minimalSubmit();
        forbidden.D_176158861.D_546976551 = 'Gallbladder';
        await expect400(forbidden, String(selfReportCancerCIDs.primarySiteOther));
        const missing = await submit({ ...minimalSubmit(), ...primarySitePayload(cancerSiteCIDs.other) });
        expect(missing.statusCode).toBe(200);
        expect(writtenDoc().D_176158861.D_546976551).toBeUndefined();
        const missingOtherAndTx = { ...minimalSubmit(), ...primarySitePayload(cancerSiteCIDs.other) };
        delete missingOtherAndTx.D_874288004;
        const unansweredTx = await submit(missingOtherAndTx);
        expect(unansweredTx.statusCode).toBe(200);
        expect(writtenDoc().D_176158861.D_546976551).toBeUndefined();
        expect(writtenDoc().D_874288004).toBeUndefined();
        const blank = await submit({ ...minimalSubmit(), ...primarySitePayload(cancerSiteCIDs.other, '') });
        expect(blank.statusCode).toBe(200);
        expect(writtenDoc().D_176158861.D_546976551).toBe('');
        const ok = await submit({ ...minimalSubmit(), ...primarySitePayload(cancerSiteCIDs.other, 'Gallbladder') });
        expect(ok.statusCode).toBe(200);
    });

    it('Diagnosis year required, format-checked, never in the future', async () => {
        const noYear = minimalSubmit(); delete noYear.D_908235757;
        await expect400(noYear, 'year');
        await expect400({ ...minimalSubmit(), D_908235757: '1899' }, 'year');
        await expect400({ ...minimalSubmit(), D_908235757: String(new Date().getFullYear() + 1) }, 'year');
    });

    it('dxMonth must be a month response cid when present', async () => {
        await expect400({ ...minimalSubmit(), D_299768751: '12' }, 'month');
        const ok = await submit({ ...minimalSubmit(), D_299768751: monthCIDs[0] });
        expect(ok.statusCode).toBe(200);
    });

    it('txReceived may be omitted but must be yes/no-coded when present', async () => {
        const missing = minimalSubmit(); delete missing.D_874288004;
        const res = await submit(missing);
        expect(res.statusCode).toBe(200);
        expect(writtenDoc().D_874288004).toBeUndefined();
        await expect400({ ...minimalSubmit(), D_874288004: '1' }, 'treatment');
    });

    it('txReceived=yes allows no type selected but requires valid details for selected types', async () => {
        const b = breastSubmit();
        const noTypes = breastSubmit();
        noTypes.D_388069854.D_244216107 = NO;
        delete noTypes.D_244216107;
        const noTypesRes = await submit(noTypes);
        expect(noTypesRes.statusCode).toBe(200);
        const twoTypes = breastSubmit();
        twoTypes.D_388069854.D_293873603 = YES;
        await expect400(twoTypes, 'start year');                                          // missing surgery start year
        const stray = breastSubmit();
        stray.D_293873603 = { D_281136649: '2024' };
        await expect400(stray, 'unselected treatment type');                              // surgery detail while surgery flag is No
        const tooFar = breastSubmit();
        tooFar.D_244216107.D_281136649 = String(new Date().getFullYear() + 6);
        await expect400(tooFar, 'start year');
        const scheduled = breastSubmit();
        scheduled.D_244216107.D_281136649 = String(new Date().getFullYear() + 5);
        expect((await submit(scheduled)).statusCode).toBe(200);                           // +5 allowed
        const beforeDx = breastSubmit();
        beforeDx.D_244216107.D_281136649 = '2023';
        await expect400(beforeDx, 'before diagnosis');
        const noOngoing = breastSubmit();
        delete noOngoing.D_244216107.D_566057154.D_735592270;
        await expect400(noOngoing, 'ongoing');
    });

    it('The full treatment-type group must be present as explicit Yes/No (no omitted No flags)', async () => {
        const partial = breastSubmit();
        delete partial.D_388069854.D_293873603; // only chemo flag present
        delete partial.D_388069854.D_555019890;
        delete partial.D_388069854.D_459406752;
        await expect400(partial, 'treatment type');
    });

    it('Ongoing XOR end date; end fields format-checked', async () => {
        const withEnd = breastSubmit();
        withEnd.D_244216107.D_566057154.D_729162012 = '2025';
        await expect400(withEnd, 'ongoing'); // ongoing=yes + end year
        const notOngoing = breastSubmit();
        notOngoing.D_244216107.D_566057154 = {
            D_735592270: NO,
            D_729162012: '2025',
            D_625530863: monthCIDs[4],
        };
        expect((await submit(notOngoing)).statusCode).toBe(200);
        const badMonth = breastSubmit();
        badMonth.D_244216107.D_566057154 = { ...notOngoing.D_244216107.D_566057154, D_625530863: '13' };
        await expect400(badMonth, 'month');
        const endBeforeStart = breastSubmit();
        endBeforeStart.D_244216107.D_566057154 = {
            D_735592270: NO,
            D_729162012: '2023',
        };
        await expect400(endBeforeStart, 'before start');
    });

    it('Treatment other-describe is optional when Other is selected and forbidden otherwise', async () => {
        const b = breastSubmit();
        b.D_388069854.D_459406752 = YES;
        b.D_459406752 = {
            D_281136649: '2024',
            D_566057154: { D_735592270: YES },
        };
        const missing = await submit(b);
        expect(missing.statusCode).toBe(200);
        expect(writtenDoc().D_388069854.D_420392069).toBeUndefined();
        const blankPayload = breastSubmit();
        blankPayload.D_388069854.D_459406752 = YES;
        blankPayload.D_388069854.D_420392069 = '';
        blankPayload.D_459406752 = b.D_459406752;
        const blank = await submit(blankPayload);
        expect(blank.statusCode).toBe(200);
        expect(writtenDoc().D_388069854.D_420392069).toBe('');
        const okPayload = breastSubmit();
        okPayload.D_388069854.D_459406752 = YES;
        okPayload.D_388069854.D_420392069 = 'Immunotherapy';
        okPayload.D_459406752 = b.D_459406752;
        const ok = await submit(okPayload);
        expect(ok.statusCode).toBe(200);
        const forbiddenDescribe = breastSubmit();
        forbiddenDescribe.D_388069854.D_420392069 = 'Immunotherapy';
        await expect400(forbiddenDescribe, String(selfReportCancerCIDs.treatment.otherDescribe)); // describe w/o other flag
    });

    it('txReceived=no forbids every treatment-section key', async () => {
        await expect400({ ...minimalSubmit(), D_388069854: { D_244216107: NO } }, 'treatment');
        await expect400({ ...minimalSubmit(), D_244216107: { D_281136649: '2024' } }, 'treatment');
        const unanswered = minimalSubmit();
        delete unanswered.D_874288004;
        await expect400({ ...unanswered, D_388069854: { D_244216107: NO } }, 'treatment');
        await expect400({ ...unanswered, D_244216107: { D_281136649: '2024' } }, 'treatment');
    });

    it('Screening gate required for eligible sites, forbidden otherwise', async () => {
        await expect400({ ...minimalSubmit(), D_944065539: NO }, 'screening');            // prostate
        const breastNoGate = breastSubmit(); delete breastNoGate.D_944065539;
        await expect400(breastNoGate, 'screening');
    });

    it('detected=yes needs site-valid chosen options and nested details under selected options', async () => {
        const zeroChosen = breastSubmit();
        zeroChosen.D_130601750.D_425815239 = NO;
        await expect400(zeroChosen, 'screening');                                         // zero chosen
        const wrongSite = breastSubmit();
        wrongSite.D_130601750.D_633630015 = YES;
        wrongSite.D_633630015 = { D_858052564: '2018' };
        await expect400(wrongSite, 'not valid');                                          // lungCT on breast
        const noYear = breastSubmit(); delete noYear.D_425815239.D_858052564;
        await expect400(noYear, 'screening year');
        await expect400({ ...breastSubmit(), D_528508094: { D_858052564: '2018' } }, 'unselected screening type');
        const afterDx = breastSubmit();
        afterDx.D_425815239.D_858052564 = '2025';
        await expect400(afterDx, 'after diagnosis');
        const sameYear = breastSubmit();
        sameYear.D_425815239.D_858052564 = '2024';
        expect((await submit(sameYear)).statusCode).toBe(200);
    });

    it('The full site screening-option group must be present as explicit Yes/No', async () => {
        const partial = breastSubmit();
        // drop the four explicit-No breast options, leaving only breast2D=Yes
        delete partial.D_130601750.D_759642936; delete partial.D_130601750.D_528508094; delete partial.D_130601750.D_502929020; delete partial.D_130601750.D_412252588;
        await expect400(partial, 'screening option');
    });

    it('detected=no forbids option flags and screening loops', async () => {
        const b = { ...breastSubmit(), D_944065539: NO };
        await expect400(b, 'screening');                                                  // option flags still present
        const clean = { ...minimalSubmit(), ...primarySitePayload(cancerSiteCIDs.breast), D_944065539: NO };
        expect((await submit(clean)).statusCode).toBe(200);
    });

    it('accepts mapped NPIs and Google address flags, and rejects invalid values', async () => {
        const valid = breastSubmit();
        valid.D_244216107.D_964819753_1_1 = 'Maya';
        valid.D_244216107.D_740626474_1_1 = 'Santos';
        valid.D_244216107.D_609996916_1_1 = '1234567890';
        valid.D_244216107.D_539812906_1_1 = NO;
        valid.D_244216107.D_568499390_1_1 = YES;
        valid.D_425815239.D_239126548 = 'Grace';
        valid.D_425815239.D_130343311 = 'Hopper';
        valid.D_425815239.D_879021105 = '1098765432';
        valid.D_425815239.D_501859375 = NO;
        valid.D_425815239.D_803865514 = YES;
        expect((await submit(valid)).statusCode).toBe(200);

        const badTxNpi = breastSubmit();
        badTxNpi.D_244216107.D_609996916_1_1 = '123';
        await expect400(badTxNpi, 'Invalid NPI');

        const badScreeningNpi = breastSubmit();
        badScreeningNpi.D_425815239.D_879021105 = 'not-npi';
        await expect400(badScreeningNpi, 'Invalid NPI');

        const badFlag = breastSubmit();
        badFlag.D_244216107.D_568499390_1_1 = '1';
        await expect400(badFlag, 'Yes/No');

        const txIntlGoogle = breastSubmit();
        txIntlGoogle.D_244216107.D_539812906_1_1 = YES;
        txIntlGoogle.D_244216107.D_568499390_1_1 = YES;
        await expect400(txIntlGoogle, 'International treatment facility');

        const screeningIntlGoogle = breastSubmit();
        screeningIntlGoogle.D_425815239.D_501859375 = YES;
        screeningIntlGoogle.D_425815239.D_803865514 = YES;
        await expect400(screeningIntlGoogle, 'International screening facility');
    });

    it('lung screening uses the screening source map and lung CT detail map', async () => {
        const lungNo = { ...minimalSubmit(), ...primarySitePayload(cancerSiteCIDs.lung), D_944065539: NO };
        expect((await submit(lungNo)).statusCode).toBe(200);
        const lungYes = {
            ...minimalSubmit(),
            ...primarySitePayload(cancerSiteCIDs.lung),
            D_944065539: YES,
            D_130601750: { D_633630015: YES },
            D_633630015: { D_858052564: '2020' },
        };
        expect((await submit(lungYes)).statusCode).toBe(200);
        await expect400({ ...lungYes, D_130601750: { D_425815239: YES } }, 'not valid');
        await expect400({ ...minimalSubmit(), ...primarySitePayload(cancerSiteCIDs.lung), D_944065539: NO, D_130601750: { D_633630015: NO } }, 'screening details');
        await expect400({ ...minimalSubmit(), ...primarySitePayload(cancerSiteCIDs.lung), D_944065539: YES, D_130601750: { D_633630015: YES } }, 'screening year');
    });

    it('Every month value must be a month response cid', async () => {
        const bad = breastSubmit();
        bad.D_244216107.D_742710886 = '0';
        await expect400(bad, 'month');
        const withMonth = breastSubmit();
        withMonth.D_244216107.D_742710886 = monthCIDs[0];
        const ok = await submit(withMonth);
        expect(ok.statusCode).toBe(200);
    });

    it('A same-site same-year resubmit succeeds with the next dxNumber', async () => {
        firestore.getSelfReportCancerDxDocs.mockResolvedValue({
            inProgressDoc: null,
            submittedDiagnoses: [{ ...primarySitePayload(cancerSiteCIDs.prostate), D_908235757: '2024', [DX_NUMBER_KEY]: '1', [PROSTATE_DXDT_KEY]: '2026-06-01T00:00:00.000Z' }],
        });
        const res = await invoke(mod.submitSelfReportCancerDx, 'POST', minimalSubmit());
        expect(res.statusCode).toBe(200);
        expect(writtenDoc()[DX_NUMBER_KEY]).toBe('2');
    });
});

describe('submitSelfReportCancerDx — finalization', () => {
    it('computes DxNumber across all submitted diagnoses for the participant', async () => {
        firestore.getSelfReportCancerDxDocs.mockResolvedValue({
            inProgressDoc: null,
            submittedDiagnoses: [
                { ...primarySitePayload(cancerSiteCIDs.prostate), [DX_NUMBER_KEY]: '1', [PROSTATE_DXDT_KEY]: '2026-06-01T00:00:00.000Z' },
                { ...primarySitePayload(cancerSiteCIDs.prostate), [DX_NUMBER_KEY]: '2', [PROSTATE_DXDT_KEY]: '2026-06-02T00:00:00.000Z' },
                { ...primarySitePayload(cancerSiteCIDs.breast), [DX_NUMBER_KEY]: '3', [BREAST_DXDT_KEY]: '2026-06-03T00:00:00.000Z' },
            ],
        });
        const res = await invoke(mod.submitSelfReportCancerDx, 'POST', minimalSubmit());
        expect(res.statusCode).toBe(200);
        expect(writtenDoc()[DX_NUMBER_KEY]).toBe('4'); // All prior submitted diagnoses count, regardless of site.
    });

    it('stamps exactly one site dxDt, strips op strings, and reuses the doc', async () => {
        firestore.getSelfReportCancerDxDocs.mockResolvedValue({
            inProgressDoc: { docId: 'doc-9', data: { startedAt: '2026-06-10T10:00:00.000Z' } },
            submittedDiagnoses: [],
        });
        const res = await invoke(mod.submitSelfReportCancerDx, 'POST', breastSubmit());
        expect(res.statusCode).toBe(200);
        expect(writtenDocId()).toBe('doc-9');
        const doc = writtenDoc();
        expect(doc.D_104045590).toMatch(ISO_RE);                 // breast DxDt
        const dxDtKeys = Object.values(fieldMapping.selfReportCancerDx.siteToDxDtCid)
            .map((cid) => `D_${cid}`).filter((k) => k in doc);
        expect(dxDtKeys).toEqual([BREAST_DXDT_KEY]);             // exactly one
        expect(doc[fieldMapping.docLastUpdatedTimestamp]).toBe(doc[BREAST_DXDT_KEY]);
        expect('startedAt' in doc).toBe(false);                   // in-progress metadata only
        expect('stateJSON' in doc).toBe(false);
        expect('positionJSON' in doc).toBe(false);
        expect(doc.uid).toBe(UID);
        expect(doc.token).toBe('tok-1');
        expect(doc.Connect_ID).toBe(1234567890);
        expect(doc[DX_NUMBER_KEY]).toBe('1');
    });

    it('stamps the right DxDt for a non-screening site and creates when no in-progress doc exists', async () => {
        const res = await invoke(mod.submitSelfReportCancerDx, 'POST', minimalSubmit());
        expect(res.statusCode).toBe(200);
        expect(writtenDocId()).toBeNull();                        // add (no in-progress doc)
        expect(writtenDoc()[PROSTATE_DXDT_KEY]).toMatch(ISO_RE);  // prostate DxDt
    });
});

describe('getSelfReportCancerDx', () => {
    it('rejects non-GET', async () => {
        const res = await invoke(mod.getSelfReportCancerDx, 'POST', {});
        expect(res.statusCode).toBe(405);
    });

    it('returns { data: { inProgress, submitted }, code: 200 } with stateJSON intact', async () => {
        firestore.getSelfReportCancerDxDocs.mockResolvedValue({
            inProgressDoc: { docId: 'd1', data: { ...primarySitePayload(cancerSiteCIDs.breast), stateJSON: '{"v":3,"state":{}}' } },
            submittedDiagnoses: [
                { ...primarySitePayload(cancerSiteCIDs.prostate), [DX_NUMBER_KEY]: '2', [PROSTATE_DXDT_KEY]: '2026-06-02T00:00:00.000Z' },
                { ...primarySitePayload(cancerSiteCIDs.breast), [DX_NUMBER_KEY]: '1', [BREAST_DXDT_KEY]: '2026-06-01T00:00:00.000Z' },
            ],
        });
        const res = await invoke(mod.getSelfReportCancerDx, 'GET');
        expect(res.statusCode).toBe(200);
        const { data } = res._getJSONData();
        expect(data.inProgress.stateJSON).toBe('{"v":3,"state":{}}');
        expect(data.submitted.map((diagnosis) => mod.submittedTimestampOf(diagnosis))).toEqual([
            '2026-06-01T00:00:00.000Z', '2026-06-02T00:00:00.000Z', // sorted ascending
        ]);
    });

    it('returns inProgress null when none exists', async () => {
        const res = await invoke(mod.getSelfReportCancerDx, 'GET');
        expect(res._getJSONData().data).toEqual({ inProgress: null, submitted: [] });
    });
});

describe('partitionDiagnosisDocs (pure helper for the firestore query)', () => {
    it('partitions by DxNumber and keeps the newest in-progress doc on corruption', () => {
        const diagnosisDocs = [
            { docId: 'a', data: { [DX_NUMBER_KEY]: '1', [BREAST_DXDT_KEY]: '2026-06-01T00:00:00.000Z' } },
            { docId: 'b', data: { startedAt: '2026-06-01T00:00:00.000Z' } },
            { docId: 'c', data: { startedAt: '2026-06-11T00:00:00.000Z' } }, // newest wins
        ];
        const out = mod.partitionDiagnosisDocs(diagnosisDocs);
        expect(out.inProgressDoc.docId).toBe('c');
        expect(out.submittedDocs.map((doc) => doc.docId)).toEqual(['a']);
    });
});

describe('validateSnapshotShape: structural whitelist', () => {
    const rejects = (body, frag) => {
        const errs = mod.validateSnapshotShape(body);
        expect(errs.length).toBeGreaterThan(0);
        if (frag) expect(errs.join(' ')).toContain(frag);
    };

    it('accepts a valid minimal snapshot (scalars + op keys)', () => {
        expect(mod.validateSnapshotShape(minimalSubmit())).toEqual([]);
    });
    it('rejects legacy flat source children and old composite keys', () => {
        rejects({ D_181737942: responseCid(cancerSiteCIDs.prostate) }, 'D_181737942');
        rejects({ D_244216107: YES }, 'D_244216107');
        rejects({ D_425815239: YES }, 'D_425815239');
        rejects({ D_633630015: YES }, 'D_633630015');
        rejects({ D_181737942_1_1: responseCid(cancerSiteCIDs.prostate) }, 'D_181737942_1_1');
        rejects({ D_181737942_D_281136649: '2024' }, 'D_181737942_D_281136649');
        rejects({ D_244216107_D_281136649: '2024' }, 'D_244216107_D_281136649');
    });
    it('rejects a nested child cid used without its parent', () => {
        rejects({ D_281136649: '2024' }, 'D_281136649');
    });
    it('rejects old ordinal loop keys', () => {
        rejects({ D_281136649_1_1: '2024' }, 'D_281136649_1_1');
        rejects({ D_964819753_2_3: 'Maya' }, 'D_964819753_2_3');
    });
    it('accepts source-question maps and treatment/screening detail maps', () => {
        expect(mod.validateSnapshotShape({
            D_176158861: { D_181737942: responseCid(cancerSiteCIDs.breast) },
            D_388069854: { D_244216107: YES, D_293873603: NO, D_555019890: NO, D_459406752: NO },
            D_244216107: {
                D_281136649: '2024',
                [treatmentRepeatKey(selfReportCancerCIDs.treatment.physFirstName, 3)]: 'Maya',
                [treatmentRepeatKey(selfReportCancerCIDs.treatment.physNpi, 3)]: '1234567890',
                [treatmentRepeatKey(selfReportCancerCIDs.treatment.facility.intlFlag, 3)]: NO,
                [treatmentRepeatKey(selfReportCancerCIDs.treatment.facility.googleValidated, 3)]: YES,
                D_566057154: { D_735592270: YES },
            },
            D_130601750: { D_425815239: YES, D_759642936: NO, D_528508094: NO, D_502929020: NO, D_412252588: NO },
            D_425815239: {
                D_858052564: '2017',
                D_879021105: '1098765432',
                D_501859375: NO,
                D_803865514: YES,
            },
        })).toEqual([]);
        expect(mod.validateSnapshotShape({
            D_130601750: { D_633630015: YES },
            D_633630015: { D_858052564: '2017' },
        })).toEqual([]);
    });
    it('rejects invalid parent-child combinations and repeated counters beyond the cap', () => {
        rejects({ D_244216107: { D_858052564: '2017' } });
        rejects({ D_425815239: { D_281136649: '2024' } });
        rejects({ D_633630015: { D_633630015: YES } }, 'D_633630015.D_633630015');
        rejects({ D_244216107: { D_281136649_1_1: '2024' } });
        rejects({ D_244216107: { D_964819753_1: 'Maya' } });
        rejects({ D_244216107: { D_964819753_1_2: 'Maya' } });
        rejects({ D_244216107: { D_964819753_11_11: 'Maya' } });
        rejects({ D_425815239: { D_239126548_1_1: 'Maya' } });
    });
});
