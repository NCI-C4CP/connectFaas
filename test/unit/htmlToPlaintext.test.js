const { htmlToPlaintext } = require('../../utils/htmlToPlaintext');

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

    it('should remove <script> blocks with whitespace in the closing tag', () => {
        // Regression: previous regex required literal `</script>` and let the body leak through.
        expect(htmlToPlaintext('<script>alert("x")</script >Body')).toBe('Body');
        expect(htmlToPlaintext('<script>alert("x")</script\n>Body')).toBe('Body');
        expect(htmlToPlaintext('<script>alert("x")</script\t>Body')).toBe('Body');
    });

    it('should remove <script> blocks with attribute-like content in the closing tag', () => {
        // HTML5 forbids attributes on end tags but browsers tolerate them; CodeQL flagged
        // `</script\t\n bar>` specifically. The closing pattern now uses `[^>]*` after the
        // tag name so any pre-`>` garbage is consumed.
        expect(htmlToPlaintext('<script>alert("x")</script\t\n bar>Body')).toBe('Body');
        expect(htmlToPlaintext('<script>alert("x")</script foo="bar">Body')).toBe('Body');
        expect(htmlToPlaintext('<style>p{}</style data-x="y">Body')).toBe('Body');
    });

    it('should not let HTML entities in the closing tag bypass script stripping', () => {
        // The tokenizer treats `&` as a non-tag-boundary character, so `</script&nbsp;>` is not recognized as a close tag.
        // The scanner keeps consuming as script content to EOF. Body is dropped along with the script.
        expect(htmlToPlaintext('<script>alert("x")</script&nbsp;>Body')).toBe('');
    });

    it('should remove <style> blocks and their content', () => {
        expect(htmlToPlaintext('<style>p { color: red; }</style>Body')).toBe('Body');
        expect(htmlToPlaintext('<style type="text/css">.x{}</style>Body')).toBe('Body');
    });

    it('should remove <style> blocks with whitespace in the closing tag', () => {
        expect(htmlToPlaintext('<style>p{}</style >Body')).toBe('Body');
        expect(htmlToPlaintext('<style>p{}</style\n>Body')).toBe('Body');
    });

    it('should remove HTML comments', () => {
        expect(htmlToPlaintext('<!-- hidden -->Visible')).toBe('Visible');
        expect(htmlToPlaintext('<!--\n  multi-line comment\n-->after')).toBe('after');
    });

    it('should scrub entity-decoded script/style/comment markup (content dropped)', () => {
        // If a template writes `&lt;script&gt;...`, entity decoding produces literal
        // `<script>...` markup. The post-decode scrub treats it like real script/style
        // markup and drops the whole tag-pair including content (more aggressive than
        // the prior regex-only pass, which kept the content). For this study's email
        // templates there is no legitimate use case for `&lt;script&gt;`-shaped content.
        expect(htmlToPlaintext('Before &lt;script&gt;alert(1)&lt;/script&gt; after')).toBe('Before  after');
        expect(htmlToPlaintext('Before &lt;style&gt;.x{}&lt;/style&gt; after')).toBe('Before  after');
        expect(htmlToPlaintext('Note &lt;!-- comment --&gt; here')).toBe('Note  here');
    });

    it('should not leak <script or <style literal text from obfuscated input', () => {
        // For pathological obfuscation patterns (`<scr<script>...`), the tokenizer
        // treats the outer `<scr...>` as a malformed tag and parses what it can. The
        // exact text output varies, but the security-relevant property — no `<script`
        // / `<style` literal remains in the result — must hold.
        expect(htmlToPlaintext('<scr<script>X</script>ipt>after')).not.toMatch(/<\s*\/?\s*script\b/i);
        expect(htmlToPlaintext('<sty<style>.x{}</style>le>after')).not.toMatch(/<\s*\/?\s*style\b/i);
    });

    it('should preserve arithmetic-like text content that is not actually a tag', () => {
        // The tokenizer treats `<` not followed by a letter, `/`, or `!` as literal text,
        // so `< 5` and `> 3` survive in plain text instead of being consumed as a malformed tag.
        expect(htmlToPlaintext('if x < 5 then y > 3')).toBe('if x < 5 then y > 3');
    });

    // --- Tokenizer-specific regressions ---

    it('should not be derailed by `>` inside a quoted attribute value', () => {
        // The tokenizer tracks quote state inside tags, so a `>` inside `href="x>y"`
        // does not terminate the tag prematurely.
        expect(htmlToPlaintext('<a href="https://example.com/?a=1>b">link</a>')).toBe(
            'link (https://example.com/?a=1>b)'
        );
    });

    it('should handle mixed-case tag names', () => {
        expect(htmlToPlaintext('<P>Title</P><DIV>body</DIV>')).toBe('Title\n\nbody');
        expect(htmlToPlaintext('<UL><LI>x</LI><LI>y</LI></UL>')).toBe('- x\n- y');
    });

    it('should drop an unclosed <script> entirely (to end of input)', () => {
        // Without a closing `</script>`, the tokenizer skips to end of input.
        expect(htmlToPlaintext('Before <script>alert(unclosed')).toBe('Before');
        expect(htmlToPlaintext('Before <style>.x{ color: red')).toBe('Before');
    });

    it('should not match `<scriptural>` as a script tag', () => {
        // Tag-boundary check (`<script` must be followed by whitespace, `>`, `/`, or end)
        // prevents false matches on names that merely start with "script".
        expect(htmlToPlaintext('<scriptural>text</scriptural>')).toBe('text');
    });

    // --- SendGrid substitution placeholder preservation ---

    it('should preserve camelCase placeholder tags as literal text', () => {
        // Templates use `<firstName>` / `<loginDetails>` as SendGrid substitution placeholders. They have no attrs and a camelCase name (no real HTML tag is
        // camelCase). The plain-text alternative must keep these as-is so SendGrid can substitute them at delivery time.
        expect(htmlToPlaintext('Hi <firstName>, welcome!')).toBe('Hi <firstName>, welcome!');
        expect(htmlToPlaintext('Use <loginDetails> to sign in.')).toBe('Use <loginDetails> to sign in.');
        expect(htmlToPlaintext('<firstName>')).toBe('<firstName>');
        // Other plausible camelCase placeholders (lastName, connectId, participantName) are preserved by the same rule.
        expect(htmlToPlaintext('Hello <lastName>')).toBe('Hello <lastName>');
        expect(htmlToPlaintext('<connectId> assigned')).toBe('<connectId> assigned');
    });

    it('should preserve closing placeholder tags too', () => {
        // Unusual in real templates (placeholders are not paired) but the rule applies
        // symmetrically. Both `<firstName>` and `</firstName>` are preserved as text.
        expect(htmlToPlaintext('<firstName>Joe</firstName>')).toBe('<firstName>Joe</firstName>');
    });

    it('should still treat uppercase HTML tags as real HTML (case-insensitive)', () => {
        // `<P>` and `<DIV>` have uppercase letters, but their lowercased name is in the
        // known HTML tag list, so they are processed normally.
        expect(htmlToPlaintext('<P>title</P><DIV>body</DIV>')).toBe('title\n\nbody');
        expect(htmlToPlaintext('<H1>hi</H1>x')).toBe('hi\n\nx');
    });

    it('should still strip non-camelCase unknown tags', () => {
        // Names that have NO uppercase letter (`<scriptural>`, `<unknowntag>`) are not
        // placeholders by the rule, so they are stripped as ordinary unknown HTML.
        expect(htmlToPlaintext('<scriptural>text</scriptural>')).toBe('text');
        expect(htmlToPlaintext('<unknowntag>x</unknowntag>')).toBe('x');
    });

    it('should drop the opening token when a placeholder-shaped tag has attributes', () => {
        // Real templates never put attrs on placeholders.
        expect(htmlToPlaintext('<firstName id="x">y</firstName>')).toBe('y</firstName>');
    });

    it('should collapse indented <br/> runs down to a single blank line', () => {
        // Regression: production templates often have `<br/>` separated by indentation
        // whitespace (`\n        <br/>\n        <br/>\n`). The whitespace order must trim
        // trailing per-line whitespace before the `\n{3,}` collapse. Otherwise, the indented
        // empties stay as a sequence of newlines-with-spaces-between and the collapse misses
        // them, leaving 4+ blank lines in the output.
        const html = 'first<br/>\n        <br/>\n        <br/>\n        <br/>\n        second';
        expect(htmlToPlaintext(html)).toBe('first\n\nsecond');
    });

    it('should handle comments containing `>` characters', () => {
        // The comment scanner looks for `-->` specifically; a `>` inside the comment body
        // does not end the comment.
        expect(htmlToPlaintext('<!-- 1 > 2 and 2 < 3 -->visible')).toBe('visible');
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
