const { expect } = require('chai');
const fieldMapping = require('../../utils/fieldToConceptIdMapping');
const { checkSurveyStatusesWhenVerified } = require('../../utils/shared');

const { verificationStatus, verified, notVerified, cannotBeVerified, notStarted, cancerScreeningHistorySurveyStatus, dhq3SurveyStatus } = fieldMapping;

describe('checkSurveyStatusesWhenVerified', () => {
    // --- No verification status or not verified in payloadData  ---

    it('returns payloadData unchanged when payloadData has no verification status', () => {
        const payloadData = { "state.148197146": 638335430 };
        const docData = { [verificationStatus]: notVerified };
        const result = checkSurveyStatusesWhenVerified(payloadData, docData);
        expect(result[cancerScreeningHistorySurveyStatus]).to.be.undefined;
        expect(result[dhq3SurveyStatus]).to.be.undefined;
    });

    it('returns payloadData unchanged when payloadData has verification status other than "verified" ', () => {
        const payloadData = { [verificationStatus]: cannotBeVerified };
        const docData = {};
        const result = checkSurveyStatusesWhenVerified(payloadData, docData);
        expect(result[cancerScreeningHistorySurveyStatus]).to.be.undefined;
        expect(result[dhq3SurveyStatus]).to.be.undefined;
    });

    // --- Verified in payloadData ---

    it('initializes only the missing survey status when payloadData sets verificationStatus to verified', () => {
        const existingStatus = 789467219; // "not yet eligible"
        const payloadData = { [verificationStatus]: verified };
        const docData = { [cancerScreeningHistorySurveyStatus]: existingStatus };
        const result = checkSurveyStatusesWhenVerified(payloadData, docData);
        expect(result[cancerScreeningHistorySurveyStatus]).to.be.undefined; // not added, already in docData
        expect(result[dhq3SurveyStatus]).to.equal(notStarted);
    });

    it('initializes survey statuses, if missing, when payloadData sets verificationStatus to verified', () => {
        const payloadData = { [verificationStatus]: verified };
        const docData = {};
        const result = checkSurveyStatusesWhenVerified(payloadData, docData);
        expect(result[cancerScreeningHistorySurveyStatus]).to.equal(notStarted);
        expect(result[dhq3SurveyStatus]).to.equal(notStarted);
    });

});
