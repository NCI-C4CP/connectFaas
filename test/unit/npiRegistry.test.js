/**
 * NPI Registry (searchNPIRegistry).
 *
 * Strategy: npiRegistry's only dependency is ./shared,whose relevant helpers are test-safe against node-mocks-http
 * objects (the same way apiEndpoints.test.js runs the real connectApp). Only global fetch
 * is stubbed. Requests carry the `x-forwarded-for` header + `connection: {}` pair that
 * logIPAddress dereferences.
 */

const httpMocks = require('node-mocks-http');
const {
    searchNPIRegistry,
    validateNPIParams,
    buildNPPESUrl,
    mapNPPESResults,
} = require('../../utils/npiRegistry');

const makeRequest = (query = {}, method = 'GET') => httpMocks.createRequest({
    method,
    headers: { 'x-forwarded-for': 'dummy' },
    connection: {},
    query,
});

const invoke = async (query, method = 'GET') => {
    const req = makeRequest(query, method);
    const res = httpMocks.createResponse();
    await searchNPIRegistry(req, res);
    return res;
};

// Minimal fetch Response. parseResponseJson consumes .text().
const mockResponse = (data, status = 200, ok = true) => ({
    ok,
    status,
    text: async () => JSON.stringify(data),
});

// Two results: one fully populated (LOCATION + MAILING addresses. primary taxonomy.
// Secondary taxonomy to prove we match on `primary`, not order), one missing credential/addresses/taxonomies.
const nppesFixture = {
    result_count: 2,
    results: [
        {
            number: 1234567890,
            basic: { first_name: 'MAYA', last_name: 'SANTOS', credential: 'M.D.' },
            addresses: [
                { address_purpose: 'MAILING', city: 'ROCKVILLE', state: 'MD' },
                { address_purpose: 'LOCATION', city: 'BETHESDA', state: 'MD' },
            ],
            taxonomies: [
                { desc: 'Internal Medicine', primary: false },
                { desc: 'Medical Oncology', primary: true },
            ],
        },
        {
            number: 1098765432,
            basic: { first_name: 'JON', last_name: 'SANTOSO' },
        },
    ],
};

let fetchStub;

beforeEach(() => {
    fetchStub = vi.fn().mockResolvedValue(mockResponse(nppesFixture));
    vi.stubGlobal('fetch', fetchStub);
});

afterEach(() => {
    vi.unstubAllGlobals();
});

describe('validateNPIParams', () => {
    it('requires lastName', () => {
        const { errors } = validateNPIParams({ firstName: 'Jo' });
        expect(errors).toContain('lastName');
    });

    it('rejects a 1-character lastName', () => {
        const { errors } = validateNPIParams({ lastName: 'S' });
        expect(errors).toContain('lastName');
    });

    it('rejects a lastName that sanitizes to empty', () => {
        const { errors } = validateNPIParams({ lastName: '123$%;' });
        expect(errors).toContain('lastName');
    });

    it("keeps letters, apostrophes, hyphens, periods; collapses whitespace; strips the rest", () => {
        const { params, errors } = validateNPIParams({ lastName: "O'Brien-Smith  Jr.", firstName: 'Sm<i>th9' });
        expect(errors).toHaveLength(0);
        expect(params.lastName).toBe("O'Brien-Smith Jr.");
        expect(params.firstName).toBe('Smith');
    });

    it('omits firstName when absent or sanitized to empty', () => {
        expect(validateNPIParams({ lastName: 'Santos' }).params).not.toHaveProperty('firstName');
        expect(validateNPIParams({ lastName: 'Santos', firstName: '42' }).params).not.toHaveProperty('firstName');
    });

    it('clamps limit silently: 50→20, 0→1, non-numeric/absent→10', () => {
        expect(validateNPIParams({ lastName: 'Santos', limit: '50' }).params.limit).toBe(20);
        expect(validateNPIParams({ lastName: 'Santos', limit: '0' }).params.limit).toBe(1);
        expect(validateNPIParams({ lastName: 'Santos', limit: 'abc' }).params.limit).toBe(10);
        expect(validateNPIParams({ lastName: 'Santos' }).params.limit).toBe(10);
    });
});

describe('buildNPPESUrl', () => {
    const paramsOf = (urlString) => new URL(urlString).searchParams;

    it('builds the NPPES query: version, NPI-1, limit, wildcarded names', () => {
        const sp = paramsOf(buildNPPESUrl({ lastName: 'Smi', firstName: 'Jo', limit: 10 }));
        expect(sp.get('version')).toBe('2.1');
        expect(sp.get('enumeration_type')).toBe('NPI-1');
        expect(sp.get('limit')).toBe('10');
        expect(sp.get('last_name')).toBe('Smi*');
        expect(sp.get('first_name')).toBe('Jo*');
    });

    it('sends a 1-character firstName without a wildcard (NPPES requires ≥2 chars before *)', () => {
        const sp = paramsOf(buildNPPESUrl({ lastName: 'Smi', firstName: 'J', limit: 10 }));
        expect(sp.get('first_name')).toBe('J');
    });

    it('omits first_name entirely when not provided', () => {
        const sp = paramsOf(buildNPPESUrl({ lastName: 'Smi', limit: 10 }));
        expect(sp.has('first_name')).toBe(false);
    });

    it('never doubles a wildcard (user-typed * is stripped by validation upstream)', () => {
        const { params } = validateNPIParams({ lastName: 'Smi*' });
        const sp = paramsOf(buildNPPESUrl(params));
        expect(sp.get('last_name')).toBe('Smi*');
    });
});

describe('mapNPPESResults', () => {
    it('maps npi/name/credential, the PRIMARY taxonomy, and the LOCATION address', () => {
        const [first] = mapNPPESResults(nppesFixture, 10);
        expect(first).toEqual({
            npi: '1234567890',
            firstName: 'MAYA',
            lastName: 'SANTOS',
            credential: 'M.D.',
            specialty: 'Medical Oncology',
            city: 'BETHESDA',
            state: 'MD',
        });
    });

    it('falls back to empty strings when credential/addresses/taxonomies are missing', () => {
        const [, second] = mapNPPESResults(nppesFixture, 10);
        expect(second).toEqual({
            npi: '1098765432',
            firstName: 'JON',
            lastName: 'SANTOSO',
            credential: '',
            specialty: '',
            city: '',
            state: '',
        });
    });

    it('slices results beyond the limit (NPPES ignoring limit must not leak)', () => {
        expect(mapNPPESResults(nppesFixture, 1)).toHaveLength(1);
    });
});

describe('searchNPIRegistry handler', () => {
    it('rejects non-GET requests with 405 and never calls NPPES', async () => {
        const res = await invoke({ lastName: 'Santos' }, 'POST');
        expect(res.statusCode).toBe(405);
        expect(res._getJSONData().message).toBe('Only GET requests are accepted!');
        expect(fetchStub).not.toHaveBeenCalled();
    });

    it('rejects an invalid lastName with 400 and never calls NPPES', async () => {
        const res = await invoke({ firstName: 'Maya' });
        expect(res.statusCode).toBe(400);
        expect(res._getJSONData().message).toContain('lastName');
        expect(fetchStub).not.toHaveBeenCalled();
    });

    it('returns the mapped providers in the house { data, code } envelope', async () => {
        const res = await invoke({ lastName: 'Santos', firstName: 'Maya' });
        expect(res.statusCode).toBe(200);
        const body = res._getJSONData();
        expect(body.code).toBe(200);
        expect(body.data).toHaveLength(2);
        expect(body.data[0].npi).toBe('1234567890');
        expect(body.data[0].specialty).toBe('Medical Oncology');
    });

    it('treats zero results as a normal 200 with an empty data array', async () => {
        fetchStub.mockResolvedValue(mockResponse({ result_count: 0, results: [] }));
        const res = await invoke({ lastName: 'Zz' });
        expect(res.statusCode).toBe(200);
        expect(res._getJSONData().data).toEqual([]);
    });

    it('maps an NPPES Errors body (HTTP 200) to 502 — our validation should prevent these', async () => {
        fetchStub.mockResolvedValue(mockResponse({ Errors: [{ description: 'invalid' }] }));
        const res = await invoke({ lastName: 'Santos' });
        expect(res.statusCode).toBe(502);
        expect(res._getJSONData().message).toBe('NPI registry lookup failed');
    });

    it('maps a malformed NPPES body (no results array) to 502', async () => {
        fetchStub.mockResolvedValue(mockResponse({ unexpected: true }));
        const res = await invoke({ lastName: 'Santos' });
        expect(res.statusCode).toBe(502);
    });

    it('maps an NPPES HTTP error to 502', async () => {
        fetchStub.mockResolvedValue(mockResponse({}, 500, false));
        const res = await invoke({ lastName: 'Santos' });
        expect(res.statusCode).toBe(502);
    });

    it('maps a network failure to 502', async () => {
        fetchStub.mockRejectedValue(new Error('socket hang up'));
        const res = await invoke({ lastName: 'Santos' });
        expect(res.statusCode).toBe(502);
    });

    it('maps a timeout abort to 502', async () => {
        fetchStub.mockRejectedValue(Object.assign(new Error('This operation was aborted'), { name: 'AbortError' }));
        const res = await invoke({ lastName: 'Santos' });
        expect(res.statusCode).toBe(502);
    });

    it('passes the validated params through to the NPPES URL', async () => {
        await invoke({ lastName: 'santos', firstName: 'ma', limit: '5' });
        const url = new URL(fetchStub.mock.calls[0][0]);
        expect(url.origin + url.pathname).toBe('https://npiregistry.cms.hhs.gov/api/');
        expect(url.searchParams.get('last_name')).toBe('santos*');
        expect(url.searchParams.get('first_name')).toBe('ma*');
        expect(url.searchParams.get('limit')).toBe('5');
    });
});
