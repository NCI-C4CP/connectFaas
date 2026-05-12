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

    it('should render list items with a "- " bullet', () => {
        expect(htmlToPlaintext('<ul><li>Item A</li><li>Item B</li></ul>')).toBe('- Item A\n- Item B');
        expect(htmlToPlaintext('<ol><li>First</li><li>Second</li></ol>')).toBe('- First\n- Second');
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

    // --- Anchor href extraction ---

    it('should render anchors as "text (href)"', () => {
        expect(htmlToPlaintext('<a href="https://example.com">Click here</a>')).toBe(
            'Click here (https://example.com)'
        );
    });

    it('should not duplicate when anchor text equals href', () => {
        expect(htmlToPlaintext('<a href="https://example.com">https://example.com</a>')).toBe(
            'https://example.com'
        );
    });

    it('should fall back to just the href when anchor text is empty', () => {
        expect(htmlToPlaintext('<a href="https://example.com"></a>')).toBe('https://example.com');
    });

    it('should fall back to just the text when href is empty', () => {
        expect(htmlToPlaintext('<a href="">Label</a>')).toBe('Label');
    });

    it('should leave anchors with no href attribute as text-only', () => {
        expect(htmlToPlaintext('<a name="anchor">Section</a>')).toBe('Section');
    });

    it('should accept single-quoted href and extra attributes in any order', () => {
        expect(htmlToPlaintext("<a target='_blank' href='https://example.com' rel='noopener'>link</a>")).toBe(
            'link (https://example.com)'
        );
    });

    it('should accept whitespace around href equals sign', () => {
        expect(htmlToPlaintext('<a href = "https://example.com">link</a>')).toBe('link (https://example.com)');
    });

    it('should decode entities inside href values', () => {
        expect(htmlToPlaintext('<a href="https://example.com/?a=1&amp;b=2">link</a>')).toBe(
            'link (https://example.com/?a=1&b=2)'
        );
    });

    it('should not match data-href as an href attribute (word boundary)', () => {
        expect(htmlToPlaintext('<a data-href="https://example.com">y</a>')).toBe('y');
    });

    it('should strip inline tags from anchor text but preserve their content', () => {
        expect(htmlToPlaintext('<a href="https://example.com"><strong>bold</strong> link</a>')).toBe(
            'bold link (https://example.com)'
        );
    });

    // --- Numeric entity decoding (decimal + hex) ---

    it('should decode decimal numeric entities', () => {
        expect(htmlToPlaintext('&#8211;')).toBe('–');
        expect(htmlToPlaintext('&#8212;')).toBe('—');
        expect(htmlToPlaintext('&#8482;')).toBe('™');
        expect(htmlToPlaintext('&#169;')).toBe('©');
    });

    it('should decode hex numeric entities (both lowercase and uppercase x)', () => {
        expect(htmlToPlaintext('&#x2013;')).toBe('–');
        expect(htmlToPlaintext('&#X2014;')).toBe('—');
        expect(htmlToPlaintext('&#x2122;')).toBe('™');
    });

    it('should leave out-of-range numeric entities intact', () => {
        expect(htmlToPlaintext('&#9999999;')).toBe('&#9999999;');
        expect(htmlToPlaintext('&#x110000;')).toBe('&#x110000;');
    });

    // --- Extended named entities ---

    it('should decode &apos;', () => {
        expect(htmlToPlaintext("don&apos;t")).toBe("don't");
    });

    it('should decode dashes and ellipsis', () => {
        expect(htmlToPlaintext('A &ndash; B &mdash; C&hellip;')).toBe('A – B — C…');
    });

    it('should decode smart quotes', () => {
        expect(htmlToPlaintext('&ldquo;hello&rdquo; &lsquo;world&rsquo;')).toBe('“hello” ‘world’');
    });

    it('should decode copyright, registered, trademark, and currency symbols', () => {
        expect(htmlToPlaintext('&copy; &reg; &trade; &euro;&pound;&yen;&cent;')).toBe('© ® ™ €£¥¢');
    });

    // --- &amp; decoded last ---

    it('should preserve entity-encoded literals by decoding &amp; last', () => {
        // Previously this leaked through: &amp; → & first, then &lt; → < -> corrupt output `<`.
        expect(htmlToPlaintext('&amp;lt;')).toBe('&lt;');
        expect(htmlToPlaintext('&amp;amp;')).toBe('&amp;');
        expect(htmlToPlaintext('&amp;copy;')).toBe('&copy;');
    });

    // --- Script, style, and comment removal ---

    it('should remove <script> blocks and their content', () => {
        expect(htmlToPlaintext('<script>alert("x")</script>Body')).toBe('Body');
        expect(htmlToPlaintext('<script src="x.js">code</script>Body')).toBe('Body');
        expect(htmlToPlaintext('<SCRIPT>uppercase</SCRIPT>Body')).toBe('Body');
    });

    it('should remove <style> blocks and their content', () => {
        expect(htmlToPlaintext('<style>p { color: red; }</style>Body')).toBe('Body');
        expect(htmlToPlaintext('<style type="text/css">.x{}</style>Body')).toBe('Body');
    });

    it('should remove HTML comments', () => {
        expect(htmlToPlaintext('<!-- hidden -->Visible')).toBe('Visible');
        expect(htmlToPlaintext('<!--\n  multi-line comment\n-->after')).toBe('after');
    });

    // --- Tables ---

    it('should flatten tables with tabs between cells and newlines between rows', () => {
        expect(
            htmlToPlaintext('<table><tr><td>A</td><td>B</td></tr><tr><td>C</td><td>D</td></tr></table>')
        ).toBe('A\tB\nC\tD');
    });

    it('should treat <th> the same as <td>', () => {
        expect(
            htmlToPlaintext('<table><tr><th>H1</th><th>H2</th></tr><tr><td>A</td><td>B</td></tr></table>')
        ).toBe('H1\tH2\nA\tB');
    });

    // --- Block opening tags separate from preceding text ---

    it('should separate a block element from preceding inline text', () => {
        expect(htmlToPlaintext('text<h3>Heading</h3>more')).toBe('text\n\nHeading\n\nmore');
        expect(htmlToPlaintext('intro<div>block</div>tail')).toBe('intro\nblock\ntail');
    });

    // --- Realistic email-shaped composite ---

    it('should handle a realistic multi-element fragment', () => {
        const html = [
            '<h2>Welcome to Connect</h2>',
            '<p>Thanks for joining! Please &mdash; if you have time &mdash; visit ',
            '<a href="https://myconnect.cancer.gov">your dashboard</a>.</p>',
            '<ul><li>Update your profile</li><li>Complete the &ldquo;Health&rdquo; survey</li></ul>',
            '<p>&copy; 2026 NCI</p>',
        ].join('');
        expect(htmlToPlaintext(html)).toBe(
            [
                'Welcome to Connect',
                '',
                'Thanks for joining! Please — if you have time — visit your dashboard (https://myconnect.cancer.gov).',
                '',
                '- Update your profile',
                '- Complete the “Health” survey',
                '',
                '© 2026 NCI',
            ].join('\n')
        );
    });
});
