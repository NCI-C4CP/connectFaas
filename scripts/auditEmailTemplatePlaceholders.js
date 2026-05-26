// Audit: find notification specs whose email body contains HTML-tag-style SendGrid
// substitution placeholders (e.g., `<firstName>`, `<loginDetails>`).
//
// Scope: this only audits `email.{lang}.body` fields on `notificationSpecifications`
// docs. SMS templates are not scanned (SMS bodies are plain text — no HTML, no
// tokenizer involved).
//
// What it checks: a `<word>` token with no attributes whose `word` is NOT in a
// hardcoded set of well-known HTML element names. The `htmlToPlaintext` tokenizer
// (utils/htmlToPlaintext.js) preserves these as literal text when the original-case
// name contains an uppercase letter (i.e., camelCase like `<firstName>`), matching
// SendGrid's substitution convention. This audit surfaces every spec relying on
// that behavior so the team can spot drift (e.g., new placeholder patterns the
// tokenizer doesn't preserve, or stray malformed tags worth cleaning up).
//
// Usage:
//   GCLOUD_PROJECT=nih-nci-dceg-connect-dev node scripts/auditEmailTemplatePlaceholders.js
//
// Tune KNOWN_HTML_TAGS below if you find false positives. Keep it in sync with
// utils/htmlToPlaintext.js's _KNOWN_HTML_TAG_NAMES set so the audit reflects the
// tokenizer's actual behavior.

const admin = require("firebase-admin");
admin.initializeApp();
const db = admin.firestore();

const KNOWN_HTML_TAGS = new Set([
    "a", "abbr", "address", "area", "article", "aside", "audio", "b", "base", "bdi",
    "bdo", "blockquote", "body", "br", "button", "canvas", "caption", "cite", "code",
    "col", "colgroup", "data", "datalist", "dd", "del", "details", "dfn", "dialog",
    "div", "dl", "dt", "em", "embed", "fieldset", "figcaption", "figure", "footer",
    "form", "h1", "h2", "h3", "h4", "h5", "h6", "head", "header", "hgroup", "hr",
    "html", "i", "iframe", "img", "input", "ins", "kbd", "label", "legend", "li",
    "link", "main", "map", "mark", "meta", "meter", "nav", "noscript", "object", "ol",
    "optgroup", "option", "output", "p", "param", "picture", "pre", "progress", "q",
    "rp", "rt", "ruby", "s", "samp", "script", "section", "select", "small", "source",
    "span", "strong", "style", "sub", "summary", "sup", "svg", "table", "tbody", "td",
    "template", "textarea", "tfoot", "th", "thead", "time", "title", "tr", "track",
    "u", "ul", "var", "video", "wbr",
]);

// Catches `<word>` where word starts with a letter and has no spaces/=/`/`.
// Won't match `<a href="x">` (has whitespace) or `</p>` (we filter closings).
const PLACEHOLDER_CANDIDATE = /<([a-zA-Z][a-zA-Z0-9_-]*)>/g;

const findPlaceholdersInBody = (html = "") => {
    const matches = new Set();
    let m;
    while ((m = PLACEHOLDER_CANDIDATE.exec(html)) !== null) {
        const tag = m[1];
        if (!KNOWN_HTML_TAGS.has(tag.toLowerCase())) {
            matches.add(`<${tag}>`);
        }
    }
    return [...matches];
};

(async () => {
    const project = process.env.GCLOUD_PROJECT || "unknown";
    console.log(`Scanning notificationSpecifications in project: ${project}`);

    const snap = await db.collection("notificationSpecifications").get();
    console.log(`Found ${snap.size} total notification specs. Auditing email bodies...\n`);

    let specsWithPlaceholders = 0;
    let totalPlaceholderInstances = 0;
    const placeholderCounts = new Map();
    const specsByPlaceholder = new Map();

    for (const doc of snap.docs) {
        const data = doc.data();
        const bodies = data.email || {};
        const langs = Object.keys(bodies);
        if (langs.length === 0) continue;

        const placeholdersInSpec = new Set();
        for (const lang of langs) {
            const html = bodies[lang]?.body || "";
            for (const p of findPlaceholdersInBody(html)) placeholdersInSpec.add(p);
        }

        if (placeholdersInSpec.size > 0) {
            specsWithPlaceholders++;
            for (const p of placeholdersInSpec) {
                placeholderCounts.set(p, (placeholderCounts.get(p) || 0) + 1);
                if (!specsByPlaceholder.has(p)) specsByPlaceholder.set(p, []);
                specsByPlaceholder.get(p).push({
                    id: doc.id,
                    category: data.category || "—",
                    attempt: data.attempt || "—",
                    isDraft: data.isDraft === true,
                });
                totalPlaceholderInstances++;
            }
        }
    }

    console.log("="
        .repeat(72));
    console.log("SUMMARY");
    console.log(`Total specs scanned: ${snap.size}`);
    console.log(`Specs with HTML-style placeholders: ${specsWithPlaceholders}`);
    console.log(`Total (spec, placeholder) pairs: ${totalPlaceholderInstances}`);
    console.log("");

    if (placeholderCounts.size === 0) {
        console.log("No HTML-style placeholders detected. ✓");
        process.exit(0);
    }

    console.log("Placeholders by frequency:");
    [...placeholderCounts.entries()]
        .sort((a, b) => b[1] - a[1])
        .forEach(([p, count]) => console.log(`  ${p.padEnd(30)} ${count} spec(s)`));

    console.log("\n=".repeat(72));
    console.log("DETAIL BY PLACEHOLDER");
    for (const [p, specs] of [...specsByPlaceholder.entries()].sort()) {
        console.log(`\n${p}  (${specs.length} spec${specs.length === 1 ? "" : "s"})`);
        for (const s of specs) {
            const draftMark = s.isDraft ? " [DRAFT]" : "";
            console.log(`  ${s.id}   ${s.category} / ${s.attempt}${draftMark}`);
        }
    }

    process.exit(0);
})().catch(err => {
    console.error("FATAL:", err.message);
    process.exit(1);
});
