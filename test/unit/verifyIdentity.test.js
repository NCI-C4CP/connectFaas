const { setupTestSuite } = require('../shared/testHelpers');

let mocks;
let firestoreModule;
let fieldMapping;

const makeDoc = (data, updateFn) => ({
    data: () => data,
    ref: { update: updateFn },
});

const makeSnapshot = (docs = []) => ({
    size: docs.length,
    empty: docs.length === 0,
    docs,
});

/**
 * Build a participant doc populated with DOB fields keyed by their literal CIDs
 * (564964481/795827569/544150384). Assert verifyIdentity reads DOB by CID — independent of which namespace
 * fieldToConceptIdMapping exposes those CIDs through.
 */
const buildParticipantWithDob = ({ month, day, year, verificationStatus, extra = {} }) => ({
    token: 'token-1',
    827220437: 'site-1', // healthCareProvider CID
    [fieldMapping.verificationStatus.toString()]: verificationStatus ?? fieldMapping.notVerified,
    564964481: month, // birthMonth
    795827569: day,   // birthDay
    544150384: year,  // birthYear
    ...extra,
});

const setupParticipantsQuery = (docs) => {
    const snapshot = makeSnapshot(docs);
    mocks.firestore.collection.mockImplementation((collectionName) => ({
        where: vi.fn().mockReturnThis(),
        get: vi.fn().mockResolvedValue(snapshot),
    }));
    return snapshot;
};

describe('verifyIdentity (deterministic)', () => {
    beforeAll(() => {
        const mockSystem = setupTestSuite({
            setupConsole: false,
            setupModuleMocks: true,
        });
        mocks = mockSystem.mocks;

        if (!vi.isMockFunction(console.error)) vi.spyOn(console, 'error').mockImplementation(() => {});

        fieldMapping = require('../../utils/fieldToConceptIdMapping');
        firestoreModule = require('../../utils/firestore');
    });

    it('pins the DOB CIDs verifyIdentity reads from (fieldMapping.birthMonth, fieldMapping.birthDay, fieldMapping.birthYear)', () => {
        expect(fieldMapping.birthMonth.toString()).toBe('564964481');
        expect(fieldMapping.birthDay.toString()).toBe('795827569');
        expect(fieldMapping.birthYear.toString()).toBe('544150384');
    });

    it('returns Invalid token! Error when no participant matches token + site', async () => {
        setupParticipantsQuery([]);

        const result = await firestoreModule.verifyIdentity('verified', 'missing-token', 'site-1');

        expect(result).toBeInstanceOf(Error);
        expect(result.message).toBe('Invalid token!');
    });

    it('rejects transition from an already-verified status', async () => {
        const update = vi.fn().mockResolvedValue();
        setupParticipantsQuery([
            makeDoc(buildParticipantWithDob({
                month: '01', day: '15', year: '1990',
                verificationStatus: fieldMapping.verified,
            }), update),
        ]);

        const result = await firestoreModule.verifyIdentity('cannotbeverified', 'token-1', 'site-1');

        expect(result).toBeInstanceOf(Error);
        expect(result.message).toMatch(/Verification status cannot be changed/);
        expect(update).not.toHaveBeenCalled();
    });

    it('returns "DOB missing or incomplete" with errorCode 206 when any DOB CID is absent', async () => {
        const update = vi.fn().mockResolvedValue();
        setupParticipantsQuery([
            makeDoc(buildParticipantWithDob({
                month: '01', day: '', year: '1990', // birthDay missing
            }), update),
        ]);

        const result = await firestoreModule.verifyIdentity('verified', 'token-1', 'site-1');

        expect(result).toBeInstanceOf(Error);
        expect(result.message).toBe('Participant DOB missing or incomplete');
        expect(result.errorCode).toBe(206);
        expect(update).not.toHaveBeenCalled();
    });

    it('returns "DOB out of range" with errorCode 206 when participant is under 18', async () => {
        const tenYearsAgo = new Date();
        tenYearsAgo.setFullYear(tenYearsAgo.getFullYear() - 10);

        const update = vi.fn().mockResolvedValue();
        setupParticipantsQuery([
            makeDoc(buildParticipantWithDob({
                month: String(tenYearsAgo.getMonth() + 1).padStart(2, '0'),
                day: String(tenYearsAgo.getDate()).padStart(2, '0'),
                year: String(tenYearsAgo.getFullYear()),
            }), update),
        ]);

        const result = await firestoreModule.verifyIdentity('verified', 'token-1', 'site-1');

        expect(result).toBeInstanceOf(Error);
        expect(result.message).toMatch(/Participant DOB \(.+\) is out of range/);
        expect(result.errorCode).toBe(206);
        expect(update).not.toHaveBeenCalled();
    });

    it('returns "DOB out of range" with errorCode 206 when participant is over 90', async () => {
        const update = vi.fn().mockResolvedValue();
        setupParticipantsQuery([
            makeDoc(buildParticipantWithDob({
                month: '01', day: '15', year: '1900',
            }), update),
        ]);

        const result = await firestoreModule.verifyIdentity('verified', 'token-1', 'site-1');

        expect(result).toBeInstanceOf(Error);
        expect(result.message).toMatch(/Participant DOB \(.+\) is out of range/);
        expect(result.errorCode).toBe(206);
        expect(update).not.toHaveBeenCalled();
    });

    it('verifies and updates the participant when DOB is in range', async () => {
        const thirtyYearsAgo = new Date();
        thirtyYearsAgo.setFullYear(thirtyYearsAgo.getFullYear() - 30);

        const update = vi.fn().mockResolvedValue();
        setupParticipantsQuery([
            makeDoc(buildParticipantWithDob({
                month: String(thirtyYearsAgo.getMonth() + 1).padStart(2, '0'),
                day: String(thirtyYearsAgo.getDate()).padStart(2, '0'),
                year: String(thirtyYearsAgo.getFullYear()),
            }), update),
        ]);

        const result = await firestoreModule.verifyIdentity('verified', 'token-1', 'site-1');

        expect(result).toBe(true);
        expect(update).toHaveBeenCalledOnce();
        const updateArg = update.mock.calls[0][0];
        expect(updateArg[fieldMapping.verificationStatus.toString()]).toBe(fieldMapping.verified);
        expect(typeof updateArg[fieldMapping.autogeneratedVerificationStatusUpdatedTime]).toBe('string');
    });

    it('does not require DOB when transitioning to a non-verified status', async () => {
        const update = vi.fn().mockResolvedValue();
        // No DOB CIDs populated; transitioning to "cannotbeverified" so DOB
        // validation should be skipped entirely.
        setupParticipantsQuery([
            makeDoc({
                token: 'token-1',
                827220437: 'site-1',
                [fieldMapping.verificationStatus.toString()]: fieldMapping.notVerified,
            }, update),
        ]);

        const result = await firestoreModule.verifyIdentity('cannotbeverified', 'token-1', 'site-1');

        expect(result).toBe(true);
        expect(update).toHaveBeenCalledOnce();
    });
});
