const fieldMapping = require('../../utils/fieldToConceptIdMapping');
const { checkSurveyStatusesWhenVerified, htmlToPlaintext } = require('../../utils/shared');

const { verificationStatus, verified, notVerified, cannotBeVerified, notStarted, cancerScreeningHistorySurveyStatus, dhq3SurveyStatus } = fieldMapping;

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

describe('htmlToPlaintext', () => {
    it('should strip HTML tags', () => {
        expect(htmlToPlaintext('<p>Hello <strong>world</strong></p>')).toBe('Hello world');
    });

    it('should convert <br> to newlines', () => {
        expect(htmlToPlaintext('line one<br>line two<br/>line three<BR />line four')).toBe(
            'line one\nline two\nline three\nline four'
        );
    });

    it('should convert </p> to double newlines', () => {
        expect(htmlToPlaintext('<p>Para one</p><p>Para two</p>')).toBe('Para one\n\nPara two');
    });

    it('should convert </li> to newlines', () => {
        expect(htmlToPlaintext('<ul><li>Item A</li><li>Item B</li></ul>')).toBe('Item A\nItem B');
    });

    it('should convert </h1> through </h6> to double newlines', () => {
        expect(htmlToPlaintext('<h1>Title</h1><p>Body</p>')).toBe('Title\n\nBody');
        expect(htmlToPlaintext('<h3>Heading</h3>text')).toBe('Heading\n\ntext');
    });

    it('should decode common HTML entities', () => {
        expect(htmlToPlaintext('&amp; &lt; &gt; &nbsp; &#39; &quot;')).toBe('& < >   \' "');
    });

    it('should collapse 3+ consecutive newlines to 2', () => {
        expect(htmlToPlaintext('<p>A</p><p></p><p>B</p>')).toBe('A\n\nB');
    });

    it('should return empty string for null/undefined/empty input', () => {
        expect(htmlToPlaintext(null)).toBe('');
        expect(htmlToPlaintext(undefined)).toBe('');
        expect(htmlToPlaintext('')).toBe('');
    });

    it('should return plain text unchanged', () => {
        expect(htmlToPlaintext('Just plain text')).toBe('Just plain text');
    });

    it('should trim leading and trailing whitespace', () => {
        expect(htmlToPlaintext('  <p>content</p>  ')).toBe('content');
    });
});
