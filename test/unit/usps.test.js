/**
 * USPS Unit Tests
 *
 * Strategy: Since vi.mock() does not intercept CJS require(), we use
 * require.cache manipulation to install vi.fn()-based mocks for the
 * dependencies BEFORE requiring the module under test. The usps module
 * is re-required in each beforeEach to pick up fresh mocks.
 */

// --- Step 1: Ensure dependency modules are cached ---
require('../../utils/shared');
require('../../utils/firestore');

// --- Step 2: Build mock objects with vi.fn() ---
const sharedMock = {
    getSecret: vi.fn(),
    logIPAddress: vi.fn(),
    setHeaders: vi.fn(),
    delay: vi.fn().mockResolvedValue(undefined),
    backoffMs: vi.fn().mockReturnValue(0),
    uspsUrl: {
        auth: 'https://apis.usps.com/oauth2/v3/token',
        addresses: 'https://apis.usps.com/addresses/v3/address',
    },
    getResponseJSON: (msg, code) => ({ message: msg, code }),
    safeJSONParse: (str) => {
        try { return JSON.parse(str); } catch { return null; }
    },
    parseResponseJson: async (res) => {
        try {
            const text = await res.text();
            if (!text) return null;
            return JSON.parse(text);
        } catch { return null; }
    },
};

const createFirestoreMock = () => {
    const mockDocRef = {
        get: vi.fn(),
        set: vi.fn().mockResolvedValue(undefined),
    };
    mockDocRef.ref = mockDocRef;

    const mockQuerySnapshot = {
        empty: false,
        docs: [mockDocRef],
    };

    const mockCollection = {
        doc: vi.fn().mockReturnValue(mockDocRef),
        where: vi.fn().mockReturnThis(),
        select: vi.fn().mockReturnThis(),
        limit: vi.fn().mockReturnThis(),
        get: vi.fn().mockResolvedValue(mockQuerySnapshot),
        onSnapshot: vi.fn(),
        count: vi.fn().mockReturnValue({
            get: vi.fn().mockResolvedValue({ data: () => ({ count: 0 }) }),
        }),
    };

    return {
        db: {
            collection: vi.fn().mockReturnValue(mockCollection),
        },
    };
};

let firestoreMock = createFirestoreMock();

// --- Step 3: Resolve paths once ---
const sharedPath = require.resolve('../../utils/shared');
const firestorePath = require.resolve('../../utils/firestore');
const uspsPath = require.resolve('../../utils/usps');

// Save original exports for cleanup
const origSharedExports = require.cache[sharedPath].exports;
const origFirestoreExports = require.cache[firestorePath].exports;

// --- Tests ---
describe('USPS Unit Tests', () => {
    let uspsModule;
    let fetchStub;
    let originalFetch;
    let originalAbortController;

    const validAddressPayload = {
        streetAddress: '123 Main St',
        city: 'Anytown',
        state: 'NY',
        zipCode: '12345',
    };

    const mockToken = 'mock-access-token';
    const mockExpiresIn = 28799;

    const mockResponse = (data, status = 200, ok = true) => ({
        ok,
        status,
        json: async () => data,
        text: async () => JSON.stringify(data),
    });

    beforeEach(() => {
        originalFetch = global.fetch;
        originalAbortController = global.AbortController;

        // Reset vi.fn() mocks
        sharedMock.getSecret.mockReset();
        sharedMock.logIPAddress.mockReset();
        sharedMock.setHeaders.mockReset();
        sharedMock.delay.mockReset().mockResolvedValue(undefined);
        sharedMock.backoffMs.mockReset().mockReturnValue(0);

        // Reset firestore mock
        firestoreMock = createFirestoreMock();

        // Install mocks into require.cache
        require.cache[sharedPath].exports = sharedMock;
        require.cache[firestorePath].exports = firestoreMock;

        // Clear usps module cache so it re-requires mocked deps
        delete require.cache[uspsPath];

        // Mock global fetch
        fetchStub = vi.fn();
        global.fetch = fetchStub;
        global.AbortController = class {
            constructor() {
                this.signal = { aborted: false };
            }
            abort() {
                this.signal.aborted = true;
            }
        };

        // Setup env vars
        process.env.USPS_CLIENT_ID = 'usps-client-id';
        process.env.USPS_CLIENT_SECRET = 'usps-client-secret';

        sharedMock.getSecret.mockImplementation((key) => {
            if (key === 'usps-client-id') return Promise.resolve('mock-client-id');
            if (key === 'usps-client-secret') return Promise.resolve('mock-client-secret');
            return Promise.resolve(null);
        });

        // Setup fake timers
        vi.useFakeTimers({ now: Date.now() });

        // Now require the module under test (picks up mocked deps)
        uspsModule = require('../../utils/usps');
    });

    afterEach(() => {
        vi.useRealTimers();
        vi.restoreAllMocks();
        if (originalFetch === undefined) {
            delete global.fetch;
        } else {
            global.fetch = originalFetch;
        }
        if (originalAbortController === undefined) {
            delete global.AbortController;
        } else {
            global.AbortController = originalAbortController;
        }
        delete process.env.USPS_CLIENT_ID;
        delete process.env.USPS_CLIENT_SECRET;
    });

    afterAll(() => {
        // Restore original module exports
        require.cache[sharedPath].exports = origSharedExports;
        require.cache[firestorePath].exports = origFirestoreExports;
        delete require.cache[uspsPath];
    });

    describe('Address Param Validation', () => {
        it('should validate a complete address successfully', () => {
            const result = uspsModule.validateAddressParams(validAddressPayload);

            expect(result.errors).toHaveLength(0);
            expect(result.params.streetAddress).toBe('123 Main St');
            expect(result.params.city).toBe('Anytown');
            expect(result.params.state).toBe('NY');
            expect(result.params.ZIPCode).toBe('12345');
        });

        it('should validate with city and no ZIPCode', () => {
            const payload = { ...validAddressPayload };
            delete payload.zipCode;

            const result = uspsModule.validateAddressParams(payload);

            expect(result.errors).toHaveLength(0);
            expect(result.params.city).toBe('Anytown');
            expect(result.params.ZIPCode).toBeUndefined();
        });

        it('should validate with ZIPCode and no city', () => {
            const payload = { ...validAddressPayload };
            delete payload.city;

            const result = uspsModule.validateAddressParams(payload);

            expect(result.errors).toHaveLength(0);
            expect(result.params.city).toBeUndefined();
            expect(result.params.ZIPCode).toBe('12345');
        });

        it('should handle optional secondaryAddress field', () => {
            const payload = { ...validAddressPayload, secondaryAddress: 'Apt 4B' };
            const result = uspsModule.validateAddressParams(payload);

            expect(result.errors).toHaveLength(0);
            expect(result.params.secondaryAddress).toBe('Apt 4B');
        });

        it('should normalize non-string address fields without throwing', () => {
            const payload = {
                ...validAddressPayload,
                streetAddress: 123,
                secondaryAddress: 4,
                city: 567,
            };

            const result = uspsModule.validateAddressParams(payload);

            expect(result.errors).toHaveLength(0);
            expect(result.params.streetAddress).toBe('123');
            expect(result.params.secondaryAddress).toBe('4');
            expect(result.params.city).toBe('567');
        });

        it('should ignore unsupported optional address fields', () => {
            const payload = {
                ...validAddressPayload,
                firm: 'Acme Inc',
                urbanization: 'URB LAS GLADIOLAS',
                ZIPPlus4: '6789',
            };

            const result = uspsModule.validateAddressParams(payload);

            expect(result.errors).toHaveLength(0);
            expect(result.params.firm).toBeUndefined();
            expect(result.params.urbanization).toBeUndefined();
            expect(result.params.ZIPPlus4).toBeUndefined();
        });

        it('should ignore USPS ZIPCode casing in request payloads', () => {
            const payload = { ...validAddressPayload, ZIPCode: '54321' };
            delete payload.zipCode;

            const result = uspsModule.validateAddressParams(payload);

            expect(result.errors).toHaveLength(0);
            expect(result.params.ZIPCode).toBeUndefined();
        });

        it('should convert lowercase state to uppercase', () => {
            const payload = { ...validAddressPayload, state: 'ny' };
            const result = uspsModule.validateAddressParams(payload);

            expect(result.errors).toHaveLength(0);
            expect(result.params.state).toBe('NY');
        });

        it('should handle numeric state input', () => {
            const payload = { ...validAddressPayload, state: 12 };
            const result = uspsModule.validateAddressParams(payload);

            expect(result.errors).toContain('invalid state');
            expect(result.params.state).toBeUndefined();
        });

        it('should reject unsupported USPS state codes', () => {
            const payload = { ...validAddressPayload, state: 'ZZ' };
            const result = uspsModule.validateAddressParams(payload);

            expect(result.errors).toContain('invalid state');
            expect(result.params.state).toBeUndefined();
        });

        it('should handle numeric zipCode input', () => {
            const payload = { ...validAddressPayload, zipCode: 12345 };
            const result = uspsModule.validateAddressParams(payload);

            expect(result.errors).toHaveLength(0);
            expect(result.params.ZIPCode).toBe('12345');
        });

        it('should reject null/undefined state without throwing', () => {
            const payload = { ...validAddressPayload, state: null };
            const result = uspsModule.validateAddressParams(payload);

            expect(result.errors).toContain('missing state');
            expect(result.params.state).toBeUndefined();
        });

        it('should allow missing zipCode when city is present', () => {
            const payload = { ...validAddressPayload, zipCode: null };
            const result = uspsModule.validateAddressParams(payload);

            expect(result.errors).toHaveLength(0);
            expect(result.params.ZIPCode).toBeUndefined();
        });

        it('should reject undefined state without throwing', () => {
            const payload = { ...validAddressPayload };
            delete payload.state;
            const result = uspsModule.validateAddressParams(payload);

            expect(result.errors).toContain('missing state');
            expect(result.params.state).toBeUndefined();
        });

        it('should reject state with invalid length', () => {
            const payload = { ...validAddressPayload, state: 'NYC' };
            const result = uspsModule.validateAddressParams(payload);

            expect(result.errors).toContain('invalid state');
            expect(result.params.state).toBeUndefined();
        });

        it('should ignore invalid zipCode when city is present', () => {
            const payload = { ...validAddressPayload, zipCode: '1234' };
            const result = uspsModule.validateAddressParams(payload);

            expect(result.errors).toHaveLength(0);
            expect(result.params.ZIPCode).toBeUndefined();
        });

        it('should reject invalid zipCode when city is missing', () => {
            const payload = { ...validAddressPayload, zipCode: '1234' };
            delete payload.city;

            const result = uspsModule.validateAddressParams(payload);

            expect(result.errors).toContain('invalid zipCode');
            expect(result.params.ZIPCode).toBeUndefined();
        });

        it('should ignore zipCode with non-digits when city is present', () => {
            const payload = { ...validAddressPayload, zipCode: '1234A' };
            const result = uspsModule.validateAddressParams(payload);

            expect(result.errors).toHaveLength(0);
            expect(result.params.ZIPCode).toBeUndefined();
        });

        it('should ignore invalid optional ZIPPlus4 values', () => {
            const payload = { ...validAddressPayload, ZIPPlus4: '12345' };
            const result = uspsModule.validateAddressParams(payload);

            expect(result.errors).toHaveLength(0);
            expect(result.params.ZIPPlus4).toBeUndefined();
        });

        it('should ignore optional firm values longer than 50 characters', () => {
            const payload = { ...validAddressPayload, firm: 'a'.repeat(51) };
            const result = uspsModule.validateAddressParams(payload);

            expect(result.errors).toHaveLength(0);
            expect(result.params.firm).toBeUndefined();
        });

        it('should trim whitespace from all fields', () => {
            const payload = {
                firm: '  Acme Inc  ',
                streetAddress: '  123 Main St  ',
                secondaryAddress: '  Apt 4B  ',
                city: '  Anytown  ',
                state: '  ny  ',
                zipCode: '  12345  ',
                zipPlus4: '  6789  ',
                urbanization: '  URB LAS GLADIOLAS  ',
            };
            const result = uspsModule.validateAddressParams(payload);

            expect(result.errors).toHaveLength(0);
            expect(result.params.streetAddress).toBe('123 Main St');
            expect(result.params.secondaryAddress).toBe('Apt 4B');
            expect(result.params.city).toBe('Anytown');
            expect(result.params.state).toBe('NY');
            expect(result.params.ZIPCode).toBe('12345');
        });

        it('should return all missing required fields', () => {
            const result = uspsModule.validateAddressParams({});

            expect(result.errors).toHaveLength(3);
            expect(result.errors).toEqual(expect.arrayContaining(['missing streetAddress', 'missing state', 'missing city or zipCode']));
        });

        it('should handle string JSON input', () => {
            const result = uspsModule.validateAddressParams(JSON.stringify(validAddressPayload));

            expect(result.errors).toHaveLength(0);
            expect(result.params.streetAddress).toBe('123 Main St');
        });

        it('should handle null payload', () => {
            const result = uspsModule.validateAddressParams(null);

            expect(result.errors).toHaveLength(1);
            expect(result.errors[0]).toContain('Invalid or missing all fields');
        });

        it('should handle non-object payload', () => {
            const result = uspsModule.validateAddressParams('invalid');

            expect(result.errors).toHaveLength(1);
            expect(result.errors[0]).toContain('Invalid or missing all fields');
        });
    });

    describe('Parameter Validation - Integration Tests', () => {
        it('should return error for missing body', async () => {
            const req = { method: 'POST' };
            const res = {
                status: vi.fn().mockReturnThis(),
                json: vi.fn(),
            };

            await uspsModule.addressValidation(req, res);

            expect(res.status).toHaveBeenCalledWith(400);
            expect(res.json.mock.calls[0][0].message).toBe('Bad Request');
        });

        it('should return error for invalid fields', async () => {
            const req = {
                method: 'POST',
                body: { invalidField: 'value' },
            };
            const res = {
                status: vi.fn().mockReturnThis(),
                json: vi.fn(),
            };

            await uspsModule.addressValidation(req, res);

            expect(res.status).toHaveBeenCalledWith(400);
            expect(res.json.mock.calls[0][0].message).toContain('Invalid or missing fields');
        });

        it('should parse string body correctly', async () => {
            const req = {
                method: 'POST',
                body: JSON.stringify(validAddressPayload),
            };
            const res = {
                status: vi.fn().mockReturnThis(),
                json: vi.fn(),
            };

            fetchStub.mockResolvedValueOnce(mockResponse({ access_token: mockToken, expires_in: mockExpiresIn }));
            fetchStub.mockResolvedValueOnce(mockResponse({ address: 'validated' }));

            await uspsModule.addressValidation(req, res);

            expect(res.status).toHaveBeenCalledWith(200);
        });
    });

    describe('Token Management & Authentication', () => {
        it('should fetch new token when cache is empty', async () => {
            const req = { method: 'POST', body: validAddressPayload };
            const res = { status: vi.fn().mockReturnThis(), json: vi.fn() };

            // Mock Firestore empty cache result
            firestoreMock.db.collection().get.mockResolvedValue({ empty: true });

            // Mock Auth Call
            fetchStub.mockResolvedValueOnce(mockResponse({ access_token: mockToken, expires_in: mockExpiresIn }));

            // Mock Address Call
            fetchStub.mockResolvedValueOnce(mockResponse({ result: 'success' }));

            await uspsModule.addressValidation(req, res);

            // Verify Auth Call
            const authCall = fetchStub.mock.calls[0];
            const body = JSON.parse(authCall[1].body);
            expect(authCall[1].headers['Content-Type']).toBe('application/json');
            expect(body.grant_type).toBe('client_credentials');
            expect(body.client_id).toBe('mock-client-id');
            expect(body.client_secret).toBe('mock-client-secret');
            expect(body.scope).toBe('addresses');

            // Verify Address Call
            const addressCall = fetchStub.mock.calls[1];
            const addressUrl = new URL(addressCall[0]);
            expect(`${addressUrl.origin}${addressUrl.pathname}`).toBe(sharedMock.uspsUrl.addresses);
            expect(addressUrl.searchParams.get('streetAddress')).toBe(validAddressPayload.streetAddress);
            expect(addressUrl.searchParams.get('city')).toBe(validAddressPayload.city);
            expect(addressUrl.searchParams.get('state')).toBe(validAddressPayload.state);
            expect(addressUrl.searchParams.get('ZIPCode')).toBe(validAddressPayload.zipCode);
        });

        it('should coalesce concurrent token fetches when cache is empty', async () => {
            const requests = [
                { method: 'POST', body: validAddressPayload },
                { method: 'POST', body: validAddressPayload },
                { method: 'POST', body: validAddressPayload },
            ];
            const responses = requests.map(() => ({
                status: vi.fn().mockReturnThis(),
                json: vi.fn(),
            }));
            let resolveAuthResponse;
            const authResponsePromise = new Promise((resolve) => {
                resolveAuthResponse = resolve;
            });

            firestoreMock.db.collection().get.mockResolvedValue({ empty: true });
            fetchStub.mockImplementation((url) => {
                if (url === sharedMock.uspsUrl.auth) {
                    return authResponsePromise;
                }

                return Promise.resolve(mockResponse({ result: 'success' }));
            });

            const validationPromises = requests.map((req, index) => (
                uspsModule.addressValidation(req, responses[index])
            ));

            for (let i = 0; i < 10; i++) {
                await Promise.resolve();
            }

            expect(fetchStub.mock.calls.filter(([url]) => url === sharedMock.uspsUrl.auth)).toHaveLength(1);

            resolveAuthResponse(mockResponse({ access_token: mockToken, expires_in: mockExpiresIn }));
            await Promise.all(validationPromises);

            expect(fetchStub.mock.calls.filter(([url]) => url === sharedMock.uspsUrl.auth)).toHaveLength(1);
            responses.forEach((res) => {
                expect(res.status).toHaveBeenCalledWith(200);
            });
        });

        it('should use cached token from Firestore if available', async () => {
            const req = { method: 'POST', body: validAddressPayload };
            const res = { status: vi.fn().mockReturnThis(), json: vi.fn() };

            const mockDoc = {
                exists: true,
                data: () => ({
                    usps: {
                        token: 'firestore-token',
                        expiresAt: Date.now() + 1000000,
                    },
                }),
                ref: { set: vi.fn().mockResolvedValue(undefined) },
            };
            mockDoc.ref.get = vi.fn().mockResolvedValue(mockDoc);
            mockDoc.get = vi.fn().mockResolvedValue(mockDoc);

            firestoreMock.db.collection().get.mockResolvedValue({
                empty: false,
                docs: [mockDoc],
            });

            // Mock Address Call directly (no auth call expected)
            fetchStub.mockResolvedValue(mockResponse({ result: 'success' }));

            await uspsModule.addressValidation(req, res);

            // Expect only 1 call for address validation (no auth call due to cached token)
            expect(fetchStub.mock.calls.length).toBe(1);
            expect(fetchStub.mock.calls[0][1].headers.Authorization).toBe('Bearer firestore-token');
        });

        it('should refresh token on 401 Unauthorized', async () => {
            const req = { method: 'POST', body: validAddressPayload };
            const res = { status: vi.fn().mockReturnThis(), json: vi.fn() };

            // Initial Auth Call (Success)
            fetchStub.mockResolvedValueOnce(mockResponse({ access_token: 'expired-token', expires_in: mockExpiresIn }));

            // Address Call (Fail with 401)
            fetchStub.mockResolvedValueOnce(mockResponse({ error: 'Unauthorized' }, 401, false));

            // Auth Refresh Call (Success)
            fetchStub.mockResolvedValueOnce(mockResponse({ access_token: 'new-token', expires_in: mockExpiresIn }));

            // Retry Address Call (Success)
            fetchStub.mockResolvedValueOnce(mockResponse({ result: 'success' }));

            await uspsModule.addressValidation(req, res);

            expect(fetchStub.mock.calls.length).toBe(4);
            // 3rd call (index 2) should be the auth refresh
            expect(fetchStub.mock.calls[2][0]).toBe(sharedMock.uspsUrl.auth);
            // 4th call (index 3) should use the new token
            expect(fetchStub.mock.calls[3][1].headers.Authorization).toBe('Bearer new-token');
        });
    });

    describe('Error Handling & Retries', () => {
        it('should retry on network error', async () => {
            const req = { method: 'POST', body: validAddressPayload };
            const res = { status: vi.fn().mockReturnThis(), json: vi.fn() };

            // Auth Success
            fetchStub.mockResolvedValueOnce(mockResponse({ access_token: mockToken, expires_in: mockExpiresIn }));

            // Address Network Error
            fetchStub.mockRejectedValueOnce(new Error('Network Error'));

            // Retry Address Success
            fetchStub.mockResolvedValueOnce(mockResponse({ result: 'success' }));

            await uspsModule.addressValidation(req, res);

            expect(sharedMock.delay).toHaveBeenCalled();
            expect(res.status).toHaveBeenCalledWith(200);
        });

        it('should fail gracefully after exhausting retries', async () => {
            const req = { method: 'POST', body: validAddressPayload };
            const res = { status: vi.fn().mockReturnThis(), json: vi.fn() };

            fetchStub.mockResolvedValueOnce(mockResponse({ access_token: mockToken, expires_in: mockExpiresIn }));

            // Fail all subsequent calls
            fetchStub.mockImplementation(async (url) => {
                if (typeof url === 'string' && url.includes('oauth')) {
                    return mockResponse({ access_token: mockToken });
                }
                throw new Error('Persistent Error');
            });

            await uspsModule.addressValidation(req, res);

            expect(res.status).toHaveBeenCalledWith(502);
            expect(res.json.mock.calls[0][0].message).toContain('temporarily unavailable');
        });

        it('should handle non-retryable 4xx errors immediately', async () => {
            const req = { method: 'POST', body: validAddressPayload };
            const res = { status: vi.fn().mockReturnThis(), json: vi.fn() };

            // Mock Auth Success
            fetchStub.mockResolvedValueOnce(mockResponse({ access_token: mockToken, expires_in: mockExpiresIn }));

            // Mock 422 Unprocessable Entity
            fetchStub.mockResolvedValueOnce(mockResponse({ error: 'Invalid Address' }, 422, false));

            await uspsModule.addressValidation(req, res);

            expect(fetchStub.mock.calls.length).toBe(2);
            expect(res.status).toHaveBeenCalledWith(422);
            expect(res.json.mock.calls[0][0].message).toContain('Invalid Address');
        });

        it('should preserve USPS 404 address-not-found responses', async () => {
            const req = { method: 'POST', body: validAddressPayload };
            const res = { status: vi.fn().mockReturnThis(), json: vi.fn() };

            fetchStub.mockResolvedValueOnce(mockResponse({ access_token: mockToken, expires_in: mockExpiresIn }));
            fetchStub.mockResolvedValueOnce(mockResponse({
                apiVersion: 'v3',
                error: {
                    code: '404',
                    message: 'There is no match for the address requested.',
                    errors: [],
                },
            }, 404, false));

            await uspsModule.addressValidation(req, res);

            expect(fetchStub.mock.calls.length).toBe(2);
            expect(res.status).toHaveBeenCalledWith(404);
            expect(res.json.mock.calls[0][0].message).toBe('There is no match for the address requested.');
        });

        it('should preserve USPS 429 responses after retries are exhausted', async () => {
            const req = { method: 'POST', body: validAddressPayload };
            const res = { status: vi.fn().mockReturnThis(), json: vi.fn() };

            fetchStub.mockResolvedValueOnce(mockResponse({ access_token: mockToken, expires_in: mockExpiresIn }));
            fetchStub
                .mockResolvedValueOnce(mockResponse({ error: { message: 'Too many requests' } }, 429, false))
                .mockResolvedValueOnce(mockResponse({ error: { message: 'Too many requests' } }, 429, false))
                .mockResolvedValueOnce(mockResponse({ error: { message: 'Too many requests' } }, 429, false));

            await uspsModule.addressValidation(req, res);

            expect(sharedMock.delay).toHaveBeenCalledTimes(2);
            expect(res.status).toHaveBeenCalledWith(429);
            expect(res.json.mock.calls[0][0].message).toBe('Too many requests');
        });
    });
});
