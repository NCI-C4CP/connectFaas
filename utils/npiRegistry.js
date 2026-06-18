const { getResponseJSON, setHeaders, logIPAddress, parseResponseJson } = require('./shared');

/**
 * NPI Registry (NPPES) provider search proxy.
 *
 * Backs the Connect PWA's physician typeahead (self-report cancer diagnosis). NPPES is a
 * free, keyless CMS API but sends no CORS headers, so the browser cannot call it directly.
 * Routed through connectApp (`?api=searchNPIRegistry`), so callers are token-authenticated.
 *
 * No retry on failure: this serves a per-keystroke typeahead. The participant's next
 * keystroke is the retry, and the frontend degrades silently to manual entry on any error.
 */

const NPPES_API_URL = 'https://npiregistry.cms.hhs.gov/api/';
const NPPES_TIMEOUT_MS = 8000;
const DEFAULT_LIMIT = 10;
const MIN_LIMIT = 1;
const MAX_LIMIT = 20;

// Letters, apostrophe, hyphen, period, space. Everything else (digits, *, symbols) is
// stripped. The wildcard is appended server-side so a user-typed * can't double up.
const sanitizeName = (value) => String(value ?? '')
    .replace(/[^A-Za-z'\-. ]/g, '')
    .replace(/\s+/g, ' ')
    .trim();

/**
 * Validate and sanitize the typeahead query params.
 * @param {Object} query - req.query ({ lastName, firstName?, limit? })
 * @returns {Object} - { params: { lastName, firstName?, limit }, errors: [] }
 */
const validateNPIParams = (query = {}) => {
    const errors = [];
    const params = {};

    const lastName = sanitizeName(query.lastName);
    if (lastName.length >= 2) {
        params.lastName = lastName;
    } else {
        errors.push('lastName');
    }

    const firstName = sanitizeName(query.firstName);
    if (firstName) params.firstName = firstName;

    // Clamp silently. A bad limit from a typeahead caller should degrade, never 400.
    const parsedLimit = Number.parseInt(query.limit, 10);
    params.limit = Number.isInteger(parsedLimit)
        ? Math.min(Math.max(parsedLimit, MIN_LIMIT), MAX_LIMIT)
        : DEFAULT_LIMIT;

    return { params, errors };
};

/**
 * Build the NPPES search URL. Trailing * = prefix match, allowed only after >=2 chars
 * (NPPES rule); `use_first_name_alias` is left at its default (true) so nicknames match
 * ("Bob" finds "Robert") — desirable for a typeahead.
 * @param {Object} params - validated params from validateNPIParams
 * @returns {string} - the NPPES request URL
 */
const buildNPPESUrl = (params) => {
    const searchParams = new URLSearchParams({
        version: '2.1',
        enumeration_type: 'NPI-1',
        limit: String(params.limit),
        last_name: `${params.lastName}*`,
    });
    if (params.firstName) {
        searchParams.set('first_name', params.firstName.length >= 2 ? `${params.firstName}*` : params.firstName);
    }
    return `${NPPES_API_URL}?${searchParams.toString()}`;
};

/**
 * Trim NPPES results to the typeahead shape. Practice city/state come from the location
 * address (mailing can be a billing service). Specialty comes from the primary taxonomy.
 * @param {Object} json - parsed NPPES response body
 * @param {number} limit - defensive slice. NPPES ignoring `limit` must not leak.
 * @returns {Array} - [{ npi, firstName, lastName, credential, specialty, city, state }]
 */
const mapNPPESResults = (json, limit) => (json.results ?? []).slice(0, limit).map((result) => {
    const addresses = result.addresses ?? [];
    const location = addresses.find((a) => a.address_purpose === 'LOCATION') ?? addresses[0] ?? {};
    const taxonomies = result.taxonomies ?? [];
    const primaryTaxonomy = taxonomies.find((t) => t.primary) ?? taxonomies[0] ?? {};
    return {
        npi: String(result.number ?? ''),
        firstName: result.basic?.first_name ?? '',
        lastName: result.basic?.last_name ?? '',
        credential: result.basic?.credential ?? '',
        specialty: primaryTaxonomy.desc ?? '',
        city: location.city ?? '',
        state: location.state ?? '',
    };
});

/**
 * Provider search against the NPPES NPI Registry.
 * Endpoint handler (authenticated, dispatched from connectApp).
 */
const searchNPIRegistry = async (req, res) => {
    logIPAddress(req);
    setHeaders(res);

    if (req.method !== 'GET') {
        return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
    }

    const { params, errors } = validateNPIParams(req.query);

    if (errors.length) {
        return res.status(400).json(getResponseJSON(`Invalid or missing fields: ${errors.join(', ')}`, 400));
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), NPPES_TIMEOUT_MS);

    try {
        let nppesResponse;
        try {
            nppesResponse = await fetch(buildNPPESUrl(params), {
                method: 'GET',
                headers: { 'Accept': 'application/json' },
                signal: controller.signal,
            });
        } catch (err) {
            console.warn(`NPI registry call failed (network/timeout): ${err.message}`);
            return res.status(502).json(getResponseJSON('NPI registry lookup failed', 502));
        }

        const body = await parseResponseJson(nppesResponse);

        // NPPES returns HTTP 200 with an Errors array for input problems. Since inputs are
        // validated above, any Errors body (or a missing results array) is an upstream integration failure, not a client error.
        if (!nppesResponse.ok || !body || body.Errors || !Array.isArray(body.results)) {
            console.error('NPI registry lookup failed:', {
                status: nppesResponse.status,
                errors: body?.Errors ?? (body ? 'malformed body' : 'empty body'),
            });
            return res.status(502).json(getResponseJSON('NPI registry lookup failed', 502));
        }

        return res.status(200).json({ data: mapNPPESResults(body, params.limit), code: 200 });

    } catch (error) {
        console.error('Unexpected error at searchNPIRegistry:', error);
        return res.status(500).json(getResponseJSON('Internal Server Error', 500));

    } finally {
        clearTimeout(timeout);
    }
};

module.exports = {
    searchNPIRegistry,
    validateNPIParams,
    buildNPPESUrl,
    mapNPPESResults,
};
