/**
 * Self-Report Health Care System Update endpoint tests (submit / get), issue #1658.
 *
 * Harness: setupTestSuite installs the firebase-admin require-cache mocks so utils/firestore loads
 * safely (selfReportCancerDx pattern). Handlers require firestore INLINE, so vi.spyOn(firestore, ...)
 * intercepts through the CommonJS cache.
 */

const httpMocks = require('node-mocks-http');
const { setupTestSuite } = require('../shared/testHelpers');

let firestore;
let mod;
let fieldMapping;
let hcsCIDs;
let monthCIDs;
let YES;
let NO;

beforeAll(() => {
    setupTestSuite({ setupConsole: false, setupModuleMocks: true });
    firestore = require('../../utils/firestore');
    mod = require('../../utils/selfReportHCSUpdate');
    fieldMapping = require('../../utils/fieldToConceptIdMapping');
    hcsCIDs = fieldMapping.selfReportHCSUpdate;
    monthCIDs = fieldMapping.selfReportMonthValues.map(String);
    YES = String(fieldMapping.yes);
    NO = String(fieldMapping.no);
});

const UID = 'uid-test-1';
const SUBMITTED_TS_KEY = 'D_223569179';
const dKey = (cid) => `D_${cid}`;

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

// Minimal valid submit: facility name and change year are the participant-entered required fields.
const minimalSubmit = () => ({
    [dKey(hcsCIDs.facility.line1)]: 'Sibley Memorial Hospital',
    [dKey(hcsCIDs.changeYear)]: '2025',
});

// Full valid domestic submit.
const domesticSubmit = () => ({
    ...minimalSubmit(),
    [dKey(hcsCIDs.facility.line2)]: '5255 Loughboro Rd NW',
    [dKey(hcsCIDs.facility.line3)]: 'Suite 100',
    [dKey(hcsCIDs.facility.city)]: 'Washington',
    [dKey(hcsCIDs.facility.state)]: 'District of Columbia',
    [dKey(hcsCIDs.facility.zip)]: '20016',
    [dKey(hcsCIDs.facility.intlFlag)]: NO,
    [dKey(hcsCIDs.facility.googleValidated)]: YES,
    [dKey(hcsCIDs.changeMonth)]: monthCIDs[10],
    [dKey(hcsCIDs.additionalInfo)]: 'I moved across town.',
    [fieldMapping.surveyLanguage]: fieldMapping.english,
});

// Full valid international submit.
const internationalSubmit = () => ({
    [dKey(hcsCIDs.facility.line1)]: 'Royal Marsden',
    [dKey(hcsCIDs.facility.line2)]: '203 Fulham Rd',
    [dKey(hcsCIDs.facility.line4)]: 'Building B, Chelsea',
    [dKey(hcsCIDs.facility.city)]: 'London',
    [dKey(hcsCIDs.facility.state)]: 'Greater London',
    [dKey(hcsCIDs.facility.zip)]: 'SW3 6JJ',
    [dKey(hcsCIDs.facility.intlFlag)]: YES,
    [dKey(hcsCIDs.facility.googleValidated)]: NO,
    [dKey(hcsCIDs.facility.country)]: '156628245',
    [dKey(hcsCIDs.changeYear)]: '2024',
});

let addSpy;

beforeEach(() => {
    vi.spyOn(firestore, 'retrieveUserProfile').mockResolvedValue({
        token: 'tok-1', Connect_ID: 1234567890,
        [fieldMapping.verificationStatus]: fieldMapping.verified, // verified + active => eligible.
    });
    addSpy = vi.spyOn(firestore, 'addSelfReportHCSUpdateDoc').mockResolvedValue();
    vi.spyOn(firestore, 'getSelfReportHCSUpdateDocs').mockResolvedValue([]);
});

afterEach(() => {
    vi.restoreAllMocks();
});

const writtenDoc = () => addSpy.mock.calls.at(-1)[0];
const ISO_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

describe('storeSelfReportHCSUpdate — request plumbing', () => {
    it('rejects non-POST requests', async () => {
        const res = await invoke(mod.storeSelfReportHCSUpdate, 'GET', undefined);
        expect(res.statusCode).toBe(405);
    });

    it('rejects an empty body', async () => {
        const res = await invoke(mod.storeSelfReportHCSUpdate, 'POST', {});
        expect(res.statusCode).toBe(400);
    });

    it('rejects ineligible participants: not verified', async () => {
        firestore.retrieveUserProfile.mockResolvedValue({ token: 'tok-1', Connect_ID: 1234567890 }); // no verificationStatus
        const res = await invoke(mod.storeSelfReportHCSUpdate, 'POST', minimalSubmit());
        expect(res.statusCode).toBe(403);
        expect(addSpy).not.toHaveBeenCalled();
    });

    it('rejects ineligible participants: withdrawn consent', async () => {
        firestore.retrieveUserProfile.mockResolvedValue({
            token: 'tok-1', Connect_ID: 1234567890,
            [fieldMapping.verificationStatus]: fieldMapping.verified,
            [fieldMapping.withdrawConsent]: fieldMapping.yes,
        });
        const res = await invoke(mod.storeSelfReportHCSUpdate, 'POST', minimalSubmit());
        expect(res.statusCode).toBe(403);
        expect(addSpy).not.toHaveBeenCalled();
    });
});

describe('storeSelfReportHCSUpdate (validation)', () => {
    it('rejects unknown D_ keys', async () => {
        const res = await invoke(mod.storeSelfReportHCSUpdate, 'POST', { ...minimalSubmit(), D_999999999: 'x' });
        expect(res.statusCode).toBe(400);
    });

    it('rejects nested (non-string) values', async () => {
        const res = await invoke(mod.storeSelfReportHCSUpdate, 'POST', {
            ...minimalSubmit(),
            [dKey(hcsCIDs.facility.city)]: { D_973363047: 'Washington' },
        });
        expect(res.statusCode).toBe(400);
    });

    it('rejects an over-length additional-information value', async () => {
        const res = await invoke(mod.storeSelfReportHCSUpdate, 'POST', {
            ...minimalSubmit(),
            [dKey(hcsCIDs.additionalInfo)]: 'x'.repeat(801),
        });
        expect(res.statusCode).toBe(400);
    });

    it('rejects text fields beyond their data-dictionary lengths', async () => {
        const cases = [
            [hcsCIDs.facility.line1, 71],
            [hcsCIDs.facility.line2, 71],
            [hcsCIDs.facility.line3, 71],
            [hcsCIDs.facility.line4, 71],
            [hcsCIDs.facility.city, 46],
            [hcsCIDs.facility.state, 49],
            [hcsCIDs.facility.zip, 46],
        ];
        for (const [cid, length] of cases) {
            const res = await invoke(mod.storeSelfReportHCSUpdate, 'POST', {
                ...minimalSubmit(),
                [dKey(cid)]: 'x'.repeat(length),
            });
            expect(res.statusCode).toBe(400);
            expect(res._getJSONData().message).toContain(`D_${cid}`);
        }
    });

    it('rejects an invalid survey language cid', async () => {
        const res = await invoke(mod.storeSelfReportHCSUpdate, 'POST', { ...minimalSubmit(), [fieldMapping.surveyLanguage]: 123 });
        expect(res.statusCode).toBe(400);
    });

    it('rejects a crafted __proto__ key instead of letting inherited values pass validation', async () => {
        // JSON.parse creates "__proto__" as an ordinary own key.
        const body = JSON.parse(`{"__proto__": {"${dKey(hcsCIDs.facility.line1)}": "Injected"}, "${dKey(hcsCIDs.facility.line2)}": "1 Care Way", "${dKey(hcsCIDs.changeYear)}": "2025"}`);
        const res = await invoke(mod.storeSelfReportHCSUpdate, 'POST', body);
        expect(res.statusCode).toBe(400);
        expect(addSpy).not.toHaveBeenCalled();
    });

    it('silently strips spoofed server-owned keys instead of rejecting', async () => {
        const res = await invoke(mod.storeSelfReportHCSUpdate, 'POST', {
            ...minimalSubmit(),
            [SUBMITTED_TS_KEY]: '1999-01-01T00:00:00.000Z',
            [String(fieldMapping.docLastUpdatedTimestamp)]: 'not-a-canonical-timestamp',
            uid: 'spoofed-uid',
            Connect_ID: 42,
        });
        expect(res.statusCode).toBe(200);
        expect(writtenDoc()[SUBMITTED_TS_KEY]).toMatch(ISO_RE);
        expect(writtenDoc()[SUBMITTED_TS_KEY]).not.toBe('1999-01-01T00:00:00.000Z');
        expect(writtenDoc()[String(fieldMapping.docLastUpdatedTimestamp)]).toBe(writtenDoc()[SUBMITTED_TS_KEY]);
        expect(writtenDoc().uid).toBe(UID);
        expect(writtenDoc().Connect_ID).toBe(1234567890);
    });
});

describe('storeSelfReportHCSUpdate — submission rules', () => {
    it('requires a nonblank facility name', async () => {
        const missing = await invoke(mod.storeSelfReportHCSUpdate, 'POST', {
            [dKey(hcsCIDs.changeYear)]: '2025',
        });
        const whitespace = await invoke(mod.storeSelfReportHCSUpdate, 'POST', {
            [dKey(hcsCIDs.facility.line1)]: '   ',
            [dKey(hcsCIDs.changeYear)]: '2025',
        });
        expect([missing.statusCode, whitespace.statusCode]).toEqual([400, 400]);
        expect(missing._getJSONData().message).toContain(String(hcsCIDs.facility.line1));
        expect(whitespace._getJSONData().message).toContain(String(hcsCIDs.facility.line1));
        expect(addSpy).not.toHaveBeenCalled();
    });

    it('accepts facility name plus year while every other address field remains optional', async () => {
        const minimal = await invoke(mod.storeSelfReportHCSUpdate, 'POST', minimalSubmit());
        const withOptionalStreet = await invoke(mod.storeSelfReportHCSUpdate, 'POST', {
            ...minimalSubmit(), [dKey(hcsCIDs.facility.line2)]: '5255 Loughboro Rd NW',
        });
        expect([minimal.statusCode, withOptionalStreet.statusCode]).toEqual([200, 200]);
    });

    it('requires the change year', async () => {
        const body = minimalSubmit();
        delete body[dKey(hcsCIDs.changeYear)];
        const res = await invoke(mod.storeSelfReportHCSUpdate, 'POST', body);
        expect(res.statusCode).toBe(400);
    });

    it('range-checks the change year: rejects more than 1 year in the future, accepts up to +1', async () => {
        const currentYear = new Date().getFullYear();
        const tooFar = await invoke(mod.storeSelfReportHCSUpdate, 'POST', {
            ...minimalSubmit(), [dKey(hcsCIDs.changeYear)]: String(currentYear + 2),
        });
        expect(tooFar.statusCode).toBe(400);
        const nextYear = await invoke(mod.storeSelfReportHCSUpdate, 'POST', {
            ...minimalSubmit(), [dKey(hcsCIDs.changeYear)]: String(currentYear + 1),
        });
        expect(nextYear.statusCode).toBe(200);
    });

    it('rejects an invalid change month cid', async () => {
        const res = await invoke(mod.storeSelfReportHCSUpdate, 'POST', {
            ...minimalSubmit(), [dKey(hcsCIDs.changeMonth)]: '123456789',
        });
        expect(res.statusCode).toBe(400);
    });

    it('rejects an international facility marked Google-address validated', async () => {
        const res = await invoke(mod.storeSelfReportHCSUpdate, 'POST', {
            ...internationalSubmit(), [dKey(hcsCIDs.facility.googleValidated)]: YES,
        });
        expect(res.statusCode).toBe(400);
    });

    it('rejects a malformed domestic zip and accepts a free-form international postal code', async () => {
        const badZip = await invoke(mod.storeSelfReportHCSUpdate, 'POST', {
            ...minimalSubmit(), [dKey(hcsCIDs.facility.zip)]: 'ABC123!',
        });
        expect(badZip.statusCode).toBe(400);
        // internationalSubmit carries postal 'SW3 6JJ' under the merged zip cid.
        const intl = await invoke(mod.storeSelfReportHCSUpdate, 'POST', internationalSubmit());
        expect(intl.statusCode).toBe(200);
    });

    it('rejects international-only fields (Line 4, Country) on a domestic address', async () => {
        for (const extra of [
            { [dKey(hcsCIDs.facility.line4)]: 'Building B' },
            { [dKey(hcsCIDs.facility.country)]: '156628245' },
        ]) {
            const res = await invoke(mod.storeSelfReportHCSUpdate, 'POST', { ...domesticSubmit(), ...extra });
            expect(res.statusCode).toBe(400);
        }
    });

    it('accepts a full domestic submit and stamps server-owned fields', async () => {
        const res = await invoke(mod.storeSelfReportHCSUpdate, 'POST', domesticSubmit());
        expect(res.statusCode).toBe(200);
        const doc = writtenDoc();
        expect(doc[dKey(hcsCIDs.facility.line1)]).toBe('Sibley Memorial Hospital');
        expect(doc[SUBMITTED_TS_KEY]).toMatch(ISO_RE);
        expect(doc[String(fieldMapping.docLastUpdatedTimestamp)]).toBe(doc[SUBMITTED_TS_KEY]);
        expect(doc.uid).toBe(UID);
        expect(doc.token).toBe('tok-1');
        expect(doc.Connect_ID).toBe(1234567890);
    });

    it('accepts a full international submit', async () => {
        const res = await invoke(mod.storeSelfReportHCSUpdate, 'POST', internationalSubmit());
        expect(res.statusCode).toBe(200);
    });

    it('accepts repeat submissions as new append-only rows', async () => {
        const first = await invoke(mod.storeSelfReportHCSUpdate, 'POST', minimalSubmit());
        const second = await invoke(mod.storeSelfReportHCSUpdate, 'POST', minimalSubmit());
        expect(first.statusCode).toBe(200);
        expect(second.statusCode).toBe(200);
        expect(addSpy).toHaveBeenCalledTimes(2);
    });
});

describe('data lifecycle wiring', () => {
    it('selfReportHCSUpdates is covered by the production data-destruction collection list', () => {
        const { listOfCollectionsRelatedToDataDestruction } = require('../../utils/shared');
        expect(listOfCollectionsRelatedToDataDestruction).toContain('selfReportHCSUpdates');
        expect(listOfCollectionsRelatedToDataDestruction).toContain('selfReportCancerDx');
    });
});

describe('getSelfReportHCSUpdate', () => {
    it('rejects non-GET requests', async () => {
        const res = await invoke(mod.getSelfReportHCSUpdate, 'POST', undefined);
        expect(res.statusCode).toBe(405);
    });

    it('returns submitted rows ascending by submitted timestamp', async () => {
        firestore.getSelfReportHCSUpdateDocs.mockResolvedValue([
            { [SUBMITTED_TS_KEY]: '2026-07-06T00:00:00.000Z', [dKey(hcsCIDs.facility.line1)]: 'Newest' },
            { [SUBMITTED_TS_KEY]: '2025-11-20T00:00:00.000Z', [dKey(hcsCIDs.facility.line1)]: 'Oldest' },
        ]);
        const res = await invoke(mod.getSelfReportHCSUpdate, 'GET', undefined);
        expect(res.statusCode).toBe(200);
        const { data } = res._getJSONData();
        expect(data.submitted.map((r) => r[dKey(hcsCIDs.facility.line1)])).toEqual(['Oldest', 'Newest']);
    });

    it('returns an empty list for participants with no updates', async () => {
        const res = await invoke(mod.getSelfReportHCSUpdate, 'GET', undefined);
        expect(res._getJSONData().data.submitted).toEqual([]);
    });
});
