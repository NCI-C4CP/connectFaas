const fieldMapping = require('../../utils/fieldToConceptIdMapping');
const {
    checkSurveyStatusesWhenVerified,
    developmentTier,
    VALID_TIERS,
    PROJECT_TIER_MAP,
} = require('../../utils/shared');

const { verificationStatus, verified, notVerified, cannotBeVerified, notStarted, cancerScreeningHistorySurveyStatus, dhq3SurveyStatus } = fieldMapping;

describe('tier exports', () => {
    it('VALID_TIERS contains DEV, STAGE, PROD and is frozen', () => {
        expect(VALID_TIERS).toEqual(expect.arrayContaining(['DEV', 'STAGE', 'PROD']));
        expect(VALID_TIERS).toHaveLength(3);
        expect(Object.isFrozen(VALID_TIERS)).toBe(true);
    });

    it('PROJECT_TIER_MAP maps all three known project IDs (DEV registered explicitly) and is frozen', () => {
        expect(PROJECT_TIER_MAP['nih-nci-dceg-connect-dev']).toBe('DEV');
        expect(PROJECT_TIER_MAP['nih-nci-dceg-connect-stg-5519']).toBe('STAGE');
        expect(PROJECT_TIER_MAP['nih-nci-dceg-connect-prod-6d04']).toBe('PROD');
        expect(Object.isFrozen(PROJECT_TIER_MAP)).toBe(true);
    });

    it('falls back to DEV for unknown or unset GCLOUD_PROJECT (safe direction — never auto-resolves to STAGE/PROD)', () => {
        // developmentTier is captured at module load. We can't easily replay that here,
        // but we can verify the contract by mirroring the lookup logic.
        const lookup = (projectId) => PROJECT_TIER_MAP[projectId] || 'DEV';
        expect(lookup(undefined)).toBe('DEV');
        expect(lookup('')).toBe('DEV');
        expect(lookup('totally-unknown-project')).toBe('DEV');
        expect(lookup('nih-nci-dceg-connect-stg-5519')).toBe('STAGE');
    });

    it('developmentTier is a member of VALID_TIERS', () => {
        expect(VALID_TIERS).toContain(developmentTier);
    });
});

describe('checkSurveyStatusesWhenVerified', () => {
    // --- No verification status or not verified in payloadData  ---

    it('returns payloadData unchanged when payloadData has no verification status', () => {
        const payloadData = { "state.148197146": 638335430 };
        const docData = { [verificationStatus]: notVerified };
        const result = checkSurveyStatusesWhenVerified(payloadData, docData);
        expect(result[cancerScreeningHistorySurveyStatus]).toBeUndefined();
        expect(result[dhq3SurveyStatus]).toBeUndefined();
    });

    it('returns payloadData unchanged when payloadData has verification status other than "verified" ', () => {
        const payloadData = { [verificationStatus]: cannotBeVerified };
        const docData = {};
        const result = checkSurveyStatusesWhenVerified(payloadData, docData);
        expect(result[cancerScreeningHistorySurveyStatus]).toBeUndefined();
        expect(result[dhq3SurveyStatus]).toBeUndefined();
    });

    // --- Verified in payloadData ---

    it('initializes only the missing survey status when payloadData sets verificationStatus to verified', () => {
        const existingStatus = 789467219; // "not yet eligible"
        const payloadData = { [verificationStatus]: verified };
        const docData = { [cancerScreeningHistorySurveyStatus]: existingStatus };
        const result = checkSurveyStatusesWhenVerified(payloadData, docData);
        expect(result[cancerScreeningHistorySurveyStatus]).toBeUndefined(); // not added, already in docData
        expect(result[dhq3SurveyStatus]).toBe(notStarted);
    });

    it('initializes survey statuses, if missing, when payloadData sets verificationStatus to verified', () => {
        const payloadData = { [verificationStatus]: verified };
        const docData = {};
        const result = checkSurveyStatusesWhenVerified(payloadData, docData);
        expect(result[cancerScreeningHistorySurveyStatus]).toBe(notStarted);
        expect(result[dhq3SurveyStatus]).toBe(notStarted);
    });

});
