const httpMocks = require('node-mocks-http');
const { setupTestSuite } = require('../shared/testHelpers');
const { validateKeyCreation } = require('../../utils/iam');

let dashboard;

beforeAll(() => {
    setupTestSuite({
        setupConsole: false,
        setupModuleMocks: true,
    });

    dashboard = require('../../utils/dashboard').dashboard;
});

beforeEach(() => {
    vi.clearAllMocks();
});

const createDashboardRequest = (method, api, overrides = {}) => {
    const { headers = {}, ...rest } = overrides;
    return httpMocks.createRequest({
        method,
        query: { api },
        headers: {
            'x-forwarded-for': 'dummy',
            ...headers,
        },
        connection: {},
        ...rest,
    });
};

describe('validateKeyCreation', () => {
    it('should block when 2 active keys exist', () => {
        const mockKeys = [
            { keyId: 'key1', isLegacy: true, expiresAt: null },
            { keyId: 'key2', isLegacy: true, expiresAt: null },
        ];
        const result = validateKeyCreation(mockKeys);
        expect(result.allowed).toBe(false);
        expect(result.reason).toContain('Maximum of 2');
    });

    it('should block when a non-legacy key has >2 weeks remaining', () => {
        const futureDate = new Date();
        futureDate.setDate(futureDate.getDate() + 30);
        const mockKeys = [
            { keyId: 'key1', isLegacy: false, expiresAt: futureDate.toISOString() },
        ];
        const result = validateKeyCreation(mockKeys);
        expect(result.allowed).toBe(false);
        expect(result.reason).toContain('2 weeks');
    });

    it('should allow when a non-legacy key has <2 weeks remaining', () => {
        const soonDate = new Date();
        soonDate.setDate(soonDate.getDate() + 5);
        const mockKeys = [
            { keyId: 'key1', isLegacy: false, expiresAt: soonDate.toISOString() },
        ];
        const result = validateKeyCreation(mockKeys);
        expect(result.allowed).toBe(true);
    });

    it('should allow when only 1 legacy key exists (no 2-week check)', () => {
        const mockKeys = [
            { keyId: 'key1', isLegacy: true, expiresAt: null },
        ];
        const result = validateKeyCreation(mockKeys);
        expect(result.allowed).toBe(true);
    });

    it('should allow when 0 keys exist', () => {
        const result = validateKeyCreation([]);
        expect(result.allowed).toBe(true);
    });

    it('should block when 1 legacy + 1 non-legacy (2-key max)', () => {
        const futureDate = new Date();
        futureDate.setDate(futureDate.getDate() + 60);
        const mockKeys = [
            { keyId: 'key1', isLegacy: true, expiresAt: null },
            { keyId: 'key2', isLegacy: false, expiresAt: futureDate.toISOString() },
        ];
        const result = validateKeyCreation(mockKeys);
        expect(result.allowed).toBe(false);
        expect(result.reason).toContain('Maximum of 2');
    });

    it('should allow when 1 legacy + no other keys', () => {
        const mockKeys = [
            { keyId: 'key1', isLegacy: true, expiresAt: null },
        ];
        const result = validateKeyCreation(mockKeys);
        expect(result.allowed).toBe(true);
        expect(result.reason).toBeNull();
    });
});

describe('dashboard IAM endpoint guards', () => {
    it('should return 401 for listServiceAccountKeys without auth', async () => {
        const req = createDashboardRequest('GET', 'listServiceAccountKeys');
        const res = httpMocks.createResponse();
        await dashboard(req, res);
        expect(res.statusCode).toBe(401);
    });

    it('should return 401 for generateServiceAccountKey without auth', async () => {
        const req = createDashboardRequest('POST', 'generateServiceAccountKey');
        const res = httpMocks.createResponse();
        await dashboard(req, res);
        expect(res.statusCode).toBe(401);
    });
});
