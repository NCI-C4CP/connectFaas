// ----- htmlToPlaintext: HTML → plaintext converter -----------
// A small character-level HTML tokenizer drives the converter. Motivations:
//   1. CodeQL queries `js/incomplete-multi-character-sanitization` and `js/bad-tag-filter`
//      treat regex-based HTML stripping as fundamentally incomplete. Their official
//      recommendation is to use a parser-based approach.
//   2. A state machine handles obfuscation, quoted attribute values, malformed input,
//      and case mismatches deterministically with no backtracking surprises.

const _CH_LT = 0x3C;     // '<'
const _CH_GT = 0x3E;     // '>'
const _CH_SLASH = 0x2F;  // '/'
const _CH_BANG = 0x21;   // '!'
const _CH_EQ = 0x3D;     // '='
const _CH_DQUOT = 0x22;  // '"'
const _CH_SQUOT = 0x27;  // "'"
const _CH_SP = 0x20;     // ' '
const _CH_TAB = 0x09;    // '\t'
const _CH_LF = 0x0A;     // '\n'
const _CH_CR = 0x0D;     // '\r'
const _CH_DASH = 0x2D;   // '-'

const _isAsciiAlpha = (code) =>
    (code >= 0x41 && code <= 0x5A) || (code >= 0x61 && code <= 0x7A);

const _isAsciiAlphaNumeric = (code) =>
    _isAsciiAlpha(code) || (code >= 0x30 && code <= 0x39);

const _isHtmlWhitespace = (code) =>
    code === _CH_SP || code === _CH_TAB || code === _CH_LF || code === _CH_CR;

const _isTagBoundary = (code) =>
    !code || code === _CH_GT || code === _CH_SLASH || _isHtmlWhitespace(code);

// Case-insensitive prefix match without allocating a substring.
const _matchesPrefix = (html, pos, prefix) => {
    const len = prefix.length;
    if (pos + len > html.length) return false;
    for (let k = 0; k < len; k++) {
        const a = html.charCodeAt(pos + k);
        const b = prefix.charCodeAt(k);
        if (a === b) continue;
        if (a >= 0x41 && a <= 0x5A && a + 32 === b) continue;
        if (a >= 0x61 && a <= 0x7A && a - 32 === b) continue;
        return false;
    }
    return true;
};

// Scan forward from the start of a `<script ...>` or `<style ...>` and return the
// position immediately past the matching close tag. Handles quoted attribute values
// in both the opening and closing tags so a `<` or `>` inside a quoted attribute
// does not derail the scan.
const _skipRawTextElement = (html, pos, tagName) => {
    const n = html.length;
    let i = pos + 1; // past '<'
    let inQuote = 0;
    while (i < n) {
        const c = html.charCodeAt(i);
        if (inQuote) {
            if (c === inQuote) inQuote = 0;
        } else if (c === _CH_DQUOT || c === _CH_SQUOT) {
            inQuote = c;
        } else if (c === _CH_GT) {
            i++;
            break;
        }
        i++;
    }
    const closePrefix = "</" + tagName;
    while (i < n) {
        const lt = html.indexOf("<", i);
        if (lt === -1) return n;
        if (_matchesPrefix(html, lt, closePrefix)) {
            const afterName = lt + closePrefix.length;
            const afterCh = afterName < n ? html.charCodeAt(afterName) : 0;
            if (_isTagBoundary(afterCh)) {
                let j = afterName;
                inQuote = 0;
                while (j < n) {
                    const c = html.charCodeAt(j);
                    if (inQuote) {
                        if (c === inQuote) inQuote = 0;
                    } else if (c === _CH_DQUOT || c === _CH_SQUOT) {
                        inQuote = c;
                    } else if (c === _CH_GT) {
                        return j + 1;
                    }
                    j++;
                }
                return n;
            }
        }
        i = lt + 1;
    }
    return n;
};

// Return the position immediately past the closing `>` of the tag whose `<` is at pos.
const _scanTagEnd = (html, pos) => {
    const n = html.length;
    let i = pos + 1; // past '<'
    let inQuote = 0;
    while (i < n) {
        const c = html.charCodeAt(i);
        if (inQuote) {
            if (c === inQuote) inQuote = 0;
        } else if (c === _CH_DQUOT || c === _CH_SQUOT) {
            inQuote = c;
        } else if (c === _CH_GT) {
            return i + 1;
        }
        i++;
    }
    return n;
};

// Parse a tag-shaped substring (e.g. `<a href='x'>` or `</p>`) into structured form.
// Returns null when the input is not a recognizable HTML tag (no valid name).
const _parseTag = (src) => {
    if (src.length < 2 || src.charCodeAt(0) !== _CH_LT) return null;
    let pos = 1;
    let isClose = false;
    if (src.charCodeAt(pos) === _CH_SLASH) {
        isClose = true;
        pos++;
    }
    if (pos >= src.length || !_isAsciiAlpha(src.charCodeAt(pos))) return null;

    const nameStart = pos;
    while (pos < src.length) {
        const c = src.charCodeAt(pos);
        if (!_isAsciiAlphaNumeric(c) && c !== _CH_DASH) break;
        pos++;
    }
    const name = src.slice(nameStart, pos).toLowerCase();

    const attrs = {};
    while (pos < src.length) {
        while (pos < src.length && _isHtmlWhitespace(src.charCodeAt(pos))) pos++;
        if (pos >= src.length) break;
        const c = src.charCodeAt(pos);
        if (c === _CH_GT || c === _CH_SLASH) break;

        const attrNameStart = pos;
        while (pos < src.length) {
            const cc = src.charCodeAt(pos);
            if (cc === _CH_EQ || cc === _CH_GT || cc === _CH_SLASH || _isHtmlWhitespace(cc)) break;
            pos++;
        }
        if (pos === attrNameStart) {
            pos++;
            continue;
        }
        const attrName = src.slice(attrNameStart, pos).toLowerCase();

        while (pos < src.length && _isHtmlWhitespace(src.charCodeAt(pos))) pos++;
        let attrValue = "";
        if (pos < src.length && src.charCodeAt(pos) === _CH_EQ) {
            pos++;
            while (pos < src.length && _isHtmlWhitespace(src.charCodeAt(pos))) pos++;
            if (pos < src.length) {
                const q = src.charCodeAt(pos);
                if (q === _CH_DQUOT || q === _CH_SQUOT) {
                    pos++;
                    const vStart = pos;
                    while (pos < src.length && src.charCodeAt(pos) !== q) pos++;
                    attrValue = src.slice(vStart, pos);
                    if (pos < src.length) pos++; // consume closing quote
                } else {
                    const vStart = pos;
                    while (pos < src.length) {
                        const cc = src.charCodeAt(pos);
                        if (cc === _CH_GT || _isHtmlWhitespace(cc)) break;
                        pos++;
                    }
                    attrValue = src.slice(vStart, pos);
                }
            }
        }
        attrs[attrName] = attrValue;
    }

    return { type: "tag", name, isClose, attrs };
};

// Comprehensive list of HTML5 element names used by the placeholder-preservation heuristic. Anything NOT in this set, with no attributes and at least one uppercase
// letter in its original-case name (i.e., camelCase or PascalCase) is treated as a substitution placeholder and preserved verbatim as text. See the note in _tokenizeHtml.
const _KNOWN_HTML_TAG_NAMES = new Set([
    "a", "abbr", "address", "area", "article", "aside", "audio", "b", "base", "bdi",
    "bdo", "blockquote", "body", "br", "button", "canvas", "caption", "cite", "code",
    "col", "colgroup", "data", "datalist", "dd", "del", "details", "dfn", "dialog",
    "div", "dl", "dt", "em", "embed", "fieldset", "figcaption", "figure", "footer",
    "form", "h1", "h2", "h3", "h4", "h5", "h6", "head", "header", "hgroup", "hr",
    "html", "i", "iframe", "img", "input", "ins", "kbd", "label", "legend", "li",
    "link", "main", "map", "mark", "meta", "meter", "nav", "noscript", "object", "ol",
    "optgroup", "option", "output", "p", "param", "picture", "pre", "progress", "q",
    "rp", "rt", "ruby", "s", "samp", "script", "section", "select", "slot", "small",
    "source", "span", "strong", "style", "sub", "summary", "sup", "svg", "table",
    "tbody", "td", "template", "textarea", "tfoot", "th", "thead", "time", "title",
    "tr", "track", "u", "ul", "var", "video", "wbr",
]);

// Walk the input character by character and produce a stream of `text` and `tag`
// tokens. `<script>` and `<style>` blocks (including their content) and HTML
// comments are dropped entirely. A `<` not followed by a letter, `/`, or `!` is
// treated as literal text — this preserves arithmetic-like content (`if x < 5`).
const _tokenizeHtml = (html) => {
    const tokens = [];
    const n = html.length;
    let i = 0;

    while (i < n) {
        if (html.charCodeAt(i) !== _CH_LT) {
            const start = i;
            while (i < n && html.charCodeAt(i) !== _CH_LT) i++;
            tokens.push({ type: "text", value: html.slice(start, i) });
            continue;
        }

        const nextCode = i + 1 < n ? html.charCodeAt(i + 1) : 0;
        const isTagStart = _isAsciiAlpha(nextCode)
            || nextCode === _CH_SLASH
            || nextCode === _CH_BANG;
        if (!isTagStart) {
            tokens.push({ type: "text", value: "<" });
            i++;
            continue;
        }

        if (_matchesPrefix(html, i, "<!--")) {
            const end = html.indexOf("-->", i + 4);
            i = end === -1 ? n : end + 3;
            continue;
        }

        if (_matchesPrefix(html, i, "<script")
                && _isTagBoundary(i + 7 < n ? html.charCodeAt(i + 7) : 0)) {
            i = _skipRawTextElement(html, i, "script");
            continue;
        }
        if (_matchesPrefix(html, i, "<style")
                && _isTagBoundary(i + 6 < n ? html.charCodeAt(i + 6) : 0)) {
            i = _skipRawTextElement(html, i, "style");
            continue;
        }

        const tagEnd = _scanTagEnd(html, i);
        const tagSrc = html.slice(i, tagEnd);
        const tok = _parseTag(tagSrc);
        if (tok) {
            // Placeholder preservation. A tag-shaped token with no attributes whose original-case name contains at least one uppercase letter and is not in
            // the known HTML tag list is treated as a SendGrid substitution placeholder (e.g., `<firstName>`, `<loginDetails>`). SendGrid's substitution
            // layer replaces it in the plain-text alternative at delivery time.
            const hasAttrs = Object.keys(tok.attrs).length > 0;
            const originalNameMatch = tagSrc.match(/^<\/?([a-zA-Z][a-zA-Z0-9_-]*)/);
            const originalName = originalNameMatch ? originalNameMatch[1] : "";
            const looksLikePlaceholder = /[A-Z]/.test(originalName)
                && !_KNOWN_HTML_TAG_NAMES.has(tok.name);
            if (!hasAttrs && looksLikePlaceholder) {
                tokens.push({ type: "text", value: tagSrc });
            } else {
                tokens.push(tok);
            }
        }
        i = tagEnd;
    }

    return tokens;
};

// Re-scan a text string (typically the entity-decoded plain text near the end of the
// converter) for any literal `<script ... >`, `<style ... >`, or `<!-- ... -->` markup
// that arrived via entity-encoded sources (e.g., a template wrote `&lt;script&gt;` and
// the decoder produced `<script>` as text). Character-by-character scanning — no
// regex-based sanitization — so the strip rules do not reintroduce CodeQL findings.
const _scrubDangerousMarkup = (text) => {
    const out = [];
    const n = text.length;
    let i = 0;
    while (i < n) {
        if (text.charCodeAt(i) !== _CH_LT) {
            const start = i;
            while (i < n && text.charCodeAt(i) !== _CH_LT) i++;
            out.push(text.slice(start, i));
            continue;
        }
        if (_matchesPrefix(text, i, "<!--")) {
            const end = text.indexOf("-->", i + 4);
            i = end === -1 ? n : end + 3;
            continue;
        }
        if (_matchesPrefix(text, i, "<script")
                && _isTagBoundary(i + 7 < n ? text.charCodeAt(i + 7) : 0)) {
            i = _skipRawTextElement(text, i, "script");
            continue;
        }
        if (_matchesPrefix(text, i, "<style")
                && _isTagBoundary(i + 6 < n ? text.charCodeAt(i + 6) : 0)) {
            i = _skipRawTextElement(text, i, "style");
            continue;
        }
        // Not a dangerous marker; preserve the `<` as a literal text character.
        out.push("<");
        i++;
    }
    return out.join("");
};

const _HEAVY_BLOCK_TAGS = new Set(["p", "h1", "h2", "h3", "h4", "h5", "h6", "blockquote"]);
const _LIGHT_BLOCK_TAGS = new Set([
    "div", "section", "article", "header", "footer", "nav", "aside",
    "table", "ul", "ol", "tr", "li",
]);

const _NAMED_ENTITIES = {
    "&nbsp;": " ", "&lt;": "<", "&gt;": ">", "&quot;": '"', "&apos;": "'",
    "&ndash;": "–", "&mdash;": "—", "&hellip;": "…",
    "&ldquo;": "“", "&rdquo;": "”", "&lsquo;": "‘", "&rsquo;": "’",
    "&copy;": "©", "&reg;": "®", "&trade;": "™", "&deg;": "°",
    "&middot;": "·", "&bull;": "•", "&times;": "×", "&divide;": "÷",
    "&plusmn;": "±", "&sect;": "§", "&para;": "¶",
    "&laquo;": "«", "&raquo;": "»",
    "&euro;": "€", "&pound;": "£", "&yen;": "¥", "&cent;": "¢",
};

/**
 * Converts HTML to plaintext by tokenizing the input with a small character-level
 * state machine, mapping each tag to its plain-text representation, and decoding
 * HTML entities. Used to populate the `text` part of outbound SendGrid emails;
 * SendGrid does not auto-generate text/plain from text/html on the v3 Mail Send API.
 *
 * Implementation note for static analysis: this is NOT a security sanitizer. The
 * output is the `text/plain` MIME part of multipart/alternative email, rendered
 * as plain text by mail clients, never as HTML. The tokenizer drops `<script>`,
 * `<style>`, and comment content for output cleanliness, but the function makes no
 * security guarantee about other tag-shaped text — input is controlled notification
 * template HTML, not untrusted user input. The tokenizer-based approach (rather
 * than a regex-based pipeline) follows the official CodeQL recommendation for
 * `js/incomplete-multi-character-sanitization` and `js/bad-tag-filter`.
 *
 * Limitations:
 *   - Tables are flattened: cells separated by tabs, rows by newlines (no column widths)
 *   - Ordered and unordered lists both render with `- ` bullets (counter not tracked)
 *   - Nested lists are not indented (depth not tracked)
 *   - <pre> whitespace is collapsed like any other element
 *   - Bare URLs in text are not auto-linkified
 *
 * Intentional:
 *   - SendGrid substitution placeholder preservation:
 *   Tag-shaped tokens with no attributes whose name contains at least one uppercase
 *   letter and is not in the known HTML tag list (e.g., `<firstName>`, `<loginDetails>`)
 *   are passed through verbatim as literal text. This matches the convention used by
 *   the notification template library: at delivery time SendGrid's substitution layer
 *   replaces these tokens in both the HTML and plain-text parts of the email. Real
 *   HTML tags (`<p>`, `<DIV>`, `<a href=...>`, etc.) are unaffected.
 *
 * @param {string} html - The HTML string to convert to plaintext.
 * @returns {string} Plaintext with leading/trailing whitespace trimmed.
 */
const htmlToPlaintext = (html) => {
    if (!html) return "";

    const tokens = _tokenizeHtml(html);

    // Token-to-text rendering. Anchor handling collects inner content between
    // <a href> and </a> so the closing tag can emit "text (href)" with dedup.
    const out = [];
    let anchorDepth = 0;
    let anchorHref = "";
    let anchorBuffer = [];
    const emit = (chunk) => {
        if (anchorDepth > 0) anchorBuffer.push(chunk);
        else out.push(chunk);
    };

    for (const tok of tokens) {
        if (tok.type === "text") {
            emit(tok.value);
            continue;
        }

        const { name, isClose } = tok;

        if (name === "a") {
            if (!isClose) {
                if (anchorDepth === 0) {
                    anchorHref = (tok.attrs.href || "").trim();
                    anchorBuffer = [];
                }
                anchorDepth++;
                continue;
            }
            if (anchorDepth > 0) anchorDepth--;
            if (anchorDepth === 0) {
                const innerText = anchorBuffer.join("");
                const innerStripped = innerText.trim();
                if (!anchorHref) out.push(innerText);
                else if (!innerStripped) out.push(anchorHref);
                else if (innerStripped === anchorHref) out.push(anchorHref);
                else out.push(`${innerText} (${anchorHref})`);
                anchorHref = "";
                anchorBuffer = [];
            }
            continue;
        }

        if (name === "li" && !isClose) {
            emit("- ");
            continue;
        }

        if ((name === "br" || name === "hr") && !isClose) {
            emit("\n");
            continue;
        }

        if (_HEAVY_BLOCK_TAGS.has(name)) {
            emit("\n\n");
            continue;
        }

        if (_LIGHT_BLOCK_TAGS.has(name)) {
            // <tr> open is suppressed; its row-break comes from </tr>.
            // <li> open was already handled above (becomes a bullet).
            if (name === "tr" && !isClose) continue;
            if (name === "li" && !isClose) continue;
            emit("\n");
            continue;
        }

        if ((name === "td" || name === "th") && isClose) {
            emit("\t");
            continue;
        }
        // Inline and unknown tags (strong, em, span, img, etc.) are dropped silently.
    }

    // Flush an unclosed anchor (malformed input).
    if (anchorDepth > 0) {
        const innerText = anchorBuffer.join("");
        if (anchorHref) out.push(`${innerText} (${anchorHref})`);
        else out.push(innerText);
    }

    let text = out.join("");

    // Decode numeric entities. Reject out-of-range code points (leave the literal intact).
    text = text
        .replace(/&#(\d+);/g, (match, dec) => {
            const code = Number.parseInt(dec, 10);
            if (!Number.isFinite(code) || code < 0 || code > 0x10FFFF) return match;
            try { return String.fromCodePoint(code); } catch { return match; }
        })
        .replace(/&#[xX]([0-9a-fA-F]+);/g, (match, hex) => {
            const code = Number.parseInt(hex, 16);
            if (!Number.isFinite(code) || code < 0 || code > 0x10FFFF) return match;
            try { return String.fromCodePoint(code); } catch { return match; }
        });

    // Decode named entities. `&amp;` LAST so `&amp;lt;` decodes to `&lt;`, not `<`.
    for (const [entity, char] of Object.entries(_NAMED_ENTITIES)) {
        text = text.replaceAll(entity, char);
    }
    text = text.replaceAll("&amp;", "&");

    // Re-scrub the entity-decoded text in case a template wrote `&lt;script&gt;` etc.;
    // the decoder above would have produced literal `<script>` markup that the tokenizer
    // never saw. Character-level scanning, not a regex sanitizer.
    text = _scrubDangerousMarkup(text);

    // Normalize whitespace. Order matters for handlingempty lines and avoiding false indentation.
    return text
        .split("\n")
        .map(line => line.trim())
        .join("\n")
        .replace(/\n{3,}/g, "\n\n")
        .trim();
};

module.exports = {
    htmlToPlaintext,
};
