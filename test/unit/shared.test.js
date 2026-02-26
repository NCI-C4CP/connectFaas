const { expect } = require('chai');
const fieldMapping = require('../../utils/fieldToConceptIdMapping');
const { checkSurveyStatusAfterVerification } = require('../../utils/shared');

const { verificationStatus, verified, notStarted, cancerScreeningHistorySurveyStatus, dhq3SurveyStatus } = fieldMapping;


describe('checkSurveyStatusAfterVerification', () => {
    it('returns data unchanged when participant is not verified', () => {
        const data = { [verificationStatus]: fieldMapping.notVerified };
        const result = checkSurveyStatusAfterVerification(data);
        expect(result).to.equal(data);
        expect(result[cancerScreeningHistorySurveyStatus]).to.be.undefined;
        expect(result[dhq3SurveyStatus]).to.be.undefined;
    });

    it('initializes cancerScreeningHistorySurveyStatus to notStarted when absent', () => {
        const data = { [verificationStatus]: verified, [dhq3SurveyStatus]: notStarted };
        checkSurveyStatusAfterVerification(data);
        expect(data[cancerScreeningHistorySurveyStatus]).to.equal(notStarted);
    });

    it('initializes dhq3SurveyStatus to notStarted when absent', () => {
        const data = { [verificationStatus]: verified, [cancerScreeningHistorySurveyStatus]: notStarted };
        checkSurveyStatusAfterVerification(data);
        expect(data[dhq3SurveyStatus]).to.equal(notStarted);
    });

    it('initializes both survey statuses to notStarted when both are absent', () => {
        const data = { [verificationStatus]: verified };
        checkSurveyStatusAfterVerification(data);
        expect(data[cancerScreeningHistorySurveyStatus]).to.equal(notStarted);
        expect(data[dhq3SurveyStatus]).to.equal(notStarted);
    });

    it('does not overwrite cancerScreeningHistorySurveyStatus when already set', () => {
        const existingStatus = 615768760; // "started"
        const data = {
            [verificationStatus]: verified,
            [cancerScreeningHistorySurveyStatus]: existingStatus,
        };
        checkSurveyStatusAfterVerification(data);
        expect(data[cancerScreeningHistorySurveyStatus]).to.equal(existingStatus);
    });

    it('does not overwrite dhq3SurveyStatus when already set', () => {
        const existingStatus = 615768760; // "started"
        const data = {
            [verificationStatus]: verified,
            [dhq3SurveyStatus]: existingStatus,
        };
        checkSurveyStatusAfterVerification(data);
        expect(data[dhq3SurveyStatus]).to.equal(existingStatus);
    });

});

