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
// Keep payload keys literal to mirror Quest-flat D_<cid> snapshots. Map response cids where names exist.
const responseCid = (cid) => String(cid);

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
    D_181737942: responseCid(cancerSiteCIDs.prostate),
    D_908235757: '2024',
    D_874288004: NO,
    ...OP,
});

// Full valid submit: breast + chemo (T1, ongoing) + screening breast2D (S1).
const breastSubmit = () => ({
    D_181737942: responseCid(cancerSiteCIDs.breast),
    D_299768751: monthCIDs[10],
    D_908235757: '2024',
    D_874288004: YES,
    D_244216107: YES, D_293873603: NO, D_555019890: NO, D_459406752: NO,
    D_281136649_1_1: '2024',
    D_735592270_1_1: YES,
    D_944065539: YES,
    D_425815239: YES, D_759642936: NO, D_528508094: NO,
    D_502929020: NO, D_412252588: NO,
    D_858052564_1_1: '2017',
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

    it('accepts loop keys, two-index keys, and the operational keys', async () => {
        const res = await invoke(mod.saveSelfReportCancerDxProgress, 'POST', {
            ...minimalSubmit(),
            D_281136649_2_2: '2024',
            D_964819753_1_10: 'Maya',
            [selfReportCancerCIDs.surveyLanguage]: fieldMapping.english, // surveyLanguage: numeric value allowed
            [fieldMapping.docLastUpdatedTimestamp]: '2026-06-12T00:00:00.000Z', // lastUpdated: ISO string
        });
        expect(res.statusCode).toBe(200);
    });

    it('rejects non-string D_ values and oversized values', async () => {
        const numeric = await invoke(mod.saveSelfReportCancerDxProgress, 'POST', { ...minimalSubmit(), D_908235757: 2024 });
        expect(numeric.statusCode).toBe(400);
        const oversized = await invoke(mod.saveSelfReportCancerDxProgress, 'POST', { ...minimalSubmit(), D_546976551: 'x'.repeat(1001) });
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
            D_181737942: responseCid(cancerSiteCIDs.prostate), D_908235757: '2024', ...OP,
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
        it(`${name}: 403 when data-destruction requested`, async () => {
            firestore.retrieveUserProfile.mockResolvedValue(eligibleProfile({ [fieldMapping.participantMap.destroyData]: fieldMapping.yes }));
            expect((await invoke(getHandler(), 'POST', minimalSubmit())).statusCode).toBe(403);
        });
        it(`${name}: 403 when deceased (EMR or NORC)`, async () => {
            firestore.retrieveUserProfile.mockResolvedValue(eligibleProfile({ [fieldMapping.participantDeceased]: fieldMapping.yes }));
            expect((await invoke(getHandler(), 'POST', minimalSubmit())).statusCode).toBe(403);
            firestore.retrieveUserProfile.mockResolvedValue(eligibleProfile({ [fieldMapping.participantDeceasedNORC]: fieldMapping.yes }));
            expect((await invoke(getHandler(), 'POST', minimalSubmit())).statusCode).toBe(403);
        });
        it(`${name}: 200 for a verified, active participant`, async () => {
            expect((await invoke(getHandler(), 'POST', minimalSubmit())).statusCode).toBe(200);
        });
    }

    it('ineligibilityReason is null only for a verified, active profile', () => {
        expect(mod.ineligibilityReason(eligibleProfile())).toBeNull();
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
        const noSite = minimalSubmit(); delete noSite.D_181737942;
        await expect400(noSite, 'primary site');
        await expect400({ ...minimalSubmit(), D_181737942: '999999999' }, 'primary site');
        await expect400({ ...minimalSubmit(), D_181737942: responseCid(cancerSiteCIDs.unavailableUnknown) }, 'primary site'); // unavailableUnknown: chart-review only
    });

    it('Other-describe XOR (required iff site = other)', async () => {
        await expect400({ ...minimalSubmit(), D_181737942: responseCid(cancerSiteCIDs.other) }, String(selfReportCancerCIDs.primarySiteOther));
        await expect400({ ...minimalSubmit(), D_546976551: 'Gallbladder' }, String(selfReportCancerCIDs.primarySiteOther));
        const ok = await submit({ ...minimalSubmit(), D_181737942: responseCid(cancerSiteCIDs.other), D_546976551: 'Gallbladder' });
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

    it('txReceived required and yes/no-coded', async () => {
        const missing = minimalSubmit(); delete missing.D_874288004;
        await expect400(missing, 'treatment');
        await expect400({ ...minimalSubmit(), D_874288004: '1' }, 'treatment');
    });

    it('txReceived=yes needs >=1 type flag and contiguous valid iterations', async () => {
        const b = breastSubmit();
        await expect400({ ...b, D_244216107: NO }, 'treatment type');                     // zero selected
        const twoTypes = { ...b, D_293873603: YES };                                      // chemo+surgery, K=2
        await expect400(twoTypes, 'start year');                                          // missing _2_2 start year
        const stray = { ...b, D_281136649_3_3: '2024', D_735592270_3_3: NO };
        await expect400(stray, 'loop');                                                   // index 3 with K=1
        await expect400({ ...b, D_281136649_1_1: String(new Date().getFullYear() + 6) }, 'start year');
        const scheduled = { ...b, D_281136649_1_1: String(new Date().getFullYear() + 5) };
        expect((await submit(scheduled)).statusCode).toBe(200);                           // +5 allowed
        const noOngoing = { ...b }; delete noOngoing.D_735592270_1_1;
        await expect400(noOngoing, 'ongoing');
    });

    it('The full treatment-type group must be present as explicit Yes/No (no omitted No flags)', async () => {
        const partial = breastSubmit();
        delete partial.D_293873603; delete partial.D_555019890; delete partial.D_459406752; // only chemo flag present
        await expect400(partial, 'treatment type');
    });

    it('Ongoing XOR end date; end fields format-checked', async () => {
        const b = breastSubmit();
        await expect400({ ...b, D_729162012_1_1: '2025' }, 'ongoing');                    // ongoing=yes + end year
        const notOngoing = { ...b, D_735592270_1_1: NO, D_729162012_1_1: '2025', D_625530863_1_1: monthCIDs[4] };
        expect((await submit(notOngoing)).statusCode).toBe(200);
        await expect400({ ...notOngoing, D_625530863_1_1: '13' }, 'month');
    });

    it('Flat treatment other-describe XOR the Other type flag', async () => {
        const b = { ...breastSubmit(), D_459406752: YES, D_281136649_2_2: '2024', D_735592270_2_2: YES };
        await expect400(b, String(selfReportCancerCIDs.treatment.otherDescribe));          // other selected, no describe
        const ok = await submit({ ...b, D_420392069: 'Immunotherapy' });
        expect(ok.statusCode).toBe(200);
        await expect400({ ...breastSubmit(), D_420392069: 'Immunotherapy' }, String(selfReportCancerCIDs.treatment.otherDescribe)); // describe w/o other flag
    });

    it('txReceived=no forbids every treatment-section key', async () => {
        await expect400({ ...minimalSubmit(), D_244216107: NO }, 'treatment');
        await expect400({ ...minimalSubmit(), D_281136649_1_1: '2024' }, 'treatment');
    });

    it('Screening gate required for eligible sites, forbidden otherwise', async () => {
        await expect400({ ...minimalSubmit(), D_944065539: NO }, 'screening');            // prostate
        const breastNoGate = breastSubmit(); delete breastNoGate.D_944065539;
        await expect400(breastNoGate, 'screening');
    });

    it('detected=yes needs site-valid chosen options and contiguous iterations', async () => {
        const b = breastSubmit();
        await expect400({ ...b, D_425815239: NO }, 'screening');                          // zero chosen
        await expect400({ ...b, D_633630015: YES }, 'screening');                         // lungCT on breast
        const noYear = { ...b }; delete noYear.D_858052564_1_1;
        await expect400(noYear, 'screening year');
        await expect400({ ...b, D_858052564_2_2: '2018' }, 'loop');                       // index 2 with M=1
    });

    it('The full site screening-option group must be present as explicit Yes/No', async () => {
        const partial = breastSubmit();
        // drop the four explicit-No breast options, leaving only breast2D=Yes
        delete partial.D_759642936; delete partial.D_528508094; delete partial.D_502929020; delete partial.D_412252588;
        await expect400(partial, 'screening option');
    });

    it('detected=no forbids option flags and screening loops', async () => {
        const b = { ...breastSubmit(), D_944065539: NO };
        await expect400(b, 'screening');                                                  // option flags still present
        const clean = { ...minimalSubmit(), D_181737942: responseCid(cancerSiteCIDs.breast), D_944065539: NO };
        expect((await submit(clean)).statusCode).toBe(200);
    });

    it('Every month value must be a month response cid', async () => {
        await expect400({ ...breastSubmit(), D_742710886_1_1: '0' }, 'month');
        const ok = await submit({ ...breastSubmit(), D_742710886_1_1: monthCIDs[0] });
        expect(ok.statusCode).toBe(200);
    });

    it('A same-site same-year resubmit succeeds with the next dxNumber', async () => {
        firestore.getSelfReportCancerDxDocs.mockResolvedValue({
            inProgressDoc: null,
            submittedDiagnoses: [{ D_181737942: responseCid(cancerSiteCIDs.prostate), D_908235757: '2024', [DX_NUMBER_KEY]: '1', [PROSTATE_DXDT_KEY]: '2026-06-01T00:00:00.000Z' }],
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
                { D_181737942: responseCid(cancerSiteCIDs.prostate), [DX_NUMBER_KEY]: '1', [PROSTATE_DXDT_KEY]: '2026-06-01T00:00:00.000Z' },
                { D_181737942: responseCid(cancerSiteCIDs.prostate), [DX_NUMBER_KEY]: '2', [PROSTATE_DXDT_KEY]: '2026-06-02T00:00:00.000Z' },
                { D_181737942: responseCid(cancerSiteCIDs.breast), [DX_NUMBER_KEY]: '3', [BREAST_DXDT_KEY]: '2026-06-03T00:00:00.000Z' },
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
            inProgressDoc: { docId: 'd1', data: { D_181737942: responseCid(cancerSiteCIDs.breast), stateJSON: '{"v":3,"state":{}}' } },
            submittedDiagnoses: [
                { D_181737942: responseCid(cancerSiteCIDs.prostate), [DX_NUMBER_KEY]: '2', [PROSTATE_DXDT_KEY]: '2026-06-02T00:00:00.000Z' },
                { D_181737942: responseCid(cancerSiteCIDs.breast), [DX_NUMBER_KEY]: '1', [BREAST_DXDT_KEY]: '2026-06-01T00:00:00.000Z' },
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
    it('rejects a scalar cid carrying a loop suffix', () => {
        rejects({ D_181737942_1_1: responseCid(cancerSiteCIDs.prostate) }, 'D_181737942_1_1'); // primarySite is scalar
    });
    it('rejects a loop cid used WITHOUT a loop suffix', () => {
        rejects({ D_281136649: '2024' }, 'D_281136649'); // startYear is a loop cid
    });
    it('rejects an iteration-scalar with mismatched indices (_T_T required)', () => {
        rejects({ D_281136649_1_2: '2024' }, 'D_281136649_1_2');
    });
    it('accepts a positional cid with second != first (_T_P physician position)', () => {
        expect(mod.validateSnapshotShape({ D_964819753_2_3: 'Maya' })).toEqual([]);
    });
    it('rejects loop indices beyond the cap (iteration > 10 or position > 10)', () => {
        rejects({ D_281136649_11_11: '2024' });  // iteration 11
        rejects({ D_964819753_1_11: 'Maya' });   // physician position 11
    });
});
