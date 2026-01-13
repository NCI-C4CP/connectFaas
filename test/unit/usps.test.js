const { expect } = require('chai');
const sinon = require('sinon');

const sharedModule = require('../../utils/shared');
const firestoreModule = require('../../utils/firestore');

describe('USPS Unit Tests', () => {
    let uspsModule;
    let fetchStub;
    let clock;
    
    const validAddressPayload = {
        streetAddress: '123 Main St',
        city: 'Anytown',
        state: 'NY',
        zipCode: '12345'
    };

    const mockToken = 'mock-access-token';
    const mockExpiresIn = 28799; // ~8 hours in seconds

    // Helper to create mock response matching what utils/shared.js expects
    const mockResponse = (data, status = 200, ok = true) => ({
        ok,
        status,
        json: async () => data,
        text: async () => JSON.stringify(data)
    });

    beforeEach(() => {
        // Clear cache and re-require with fresh stubs
        delete require.cache[require.resolve('../../utils/usps')];
        
        // Stub Shared Dependencies
        sinon.stub(sharedModule, 'getSecret');
        sinon.stub(sharedModule, 'logIPAddress');
        sinon.stub(sharedModule, 'setHeaders');
        sinon.stub(sharedModule, 'delay').resolves();
        sinon.stub(sharedModule, 'backoffMs').returns(0);

        // Stub Firestore
        const mockDocRef = {
            get: sinon.stub(),
            set: sinon.stub().resolves(),
        };
        mockDocRef.ref = mockDocRef; // Self-reference for Firestore

        const mockQuerySnapshot = {
            empty: false,
            docs: [mockDocRef]
        };

        const mockCollection = {
            doc: sinon.stub().returns(mockDocRef),
            where: sinon.stub().returnsThis(),
            select: sinon.stub().returnsThis(),
            limit: sinon.stub().returnsThis(),
            get: sinon.stub().resolves(mockQuerySnapshot),
            onSnapshot: sinon.stub(),
            count: sinon.stub().returns({
                get: sinon.stub().resolves({ data: () => ({ count: 0 }) })
            })
        };

        sinon.stub(firestoreModule, 'db').value({
            collection: sinon.stub().returns(mockCollection)
        });

        // Mock global fetch
        fetchStub = sinon.stub();
        global.fetch = fetchStub;
        global.AbortController = class {
            constructor() {
                this.signal = { aborted: false };
            }
            abort() {
                this.signal.aborted = true;
            }
        };

        // Setup secrets resolution
        process.env.USPS_CLIENT_ID = 'usps-client-id';
        process.env.USPS_CLIENT_SECRET = 'usps-client-secret';
        
        sharedModule.getSecret.withArgs(process.env.USPS_CLIENT_ID).resolves('mock-client-id');
        sharedModule.getSecret.withArgs(process.env.USPS_CLIENT_SECRET).resolves('mock-client-secret');

        // Setup clock
        clock = sinon.useFakeTimers(Date.now());

        // Now require the module under test
        uspsModule = require('../../utils/usps');
    });

    afterEach(() => {
        sinon.restore();
        clock.restore();
        delete global.fetch;
        delete global.AbortController;
        delete process.env.USPS_CLIENT_ID;
        delete process.env.USPS_CLIENT_SECRET;
    });

    describe('Address Param Validation', () => {
        it('should validate all required fields successfully', () => {
            const result = uspsModule.validateAddressParams(validAddressPayload);
            
            expect(result.errors).to.be.empty;
            expect(result.params.streetAddress).to.equal('123 Main St');
            expect(result.params.city).to.equal('Anytown');
            expect(result.params.state).to.equal('NY');
            expect(result.params.ZIPCode).to.equal('12345');
        });

        it('should handle optional secondaryAddress field', () => {
            const payload = { ...validAddressPayload, secondaryAddress: 'Apt 4B' };
            const result = uspsModule.validateAddressParams(payload);
            
            expect(result.errors).to.be.empty;
            expect(result.params.secondaryAddress).to.equal('Apt 4B');
        });

        it('should convert lowercase state to uppercase', () => {
            const payload = { ...validAddressPayload, state: 'ny' };
            const result = uspsModule.validateAddressParams(payload);
            
            expect(result.errors).to.be.empty;
            expect(result.params.state).to.equal('NY');
        });

        it('should handle numeric state input', () => {
            const payload = { ...validAddressPayload, state: 12 };
            const result = uspsModule.validateAddressParams(payload);
            
            expect(result.errors).to.include('state');
            expect(result.params.state).to.be.undefined;
        });

        it('should handle numeric zipCode input', () => {
            const payload = { ...validAddressPayload, zipCode: 12345 };
            const result = uspsModule.validateAddressParams(payload);
            
            expect(result.errors).to.be.empty;
            expect(result.params.ZIPCode).to.equal('12345');
        });

        it('should reject null/undefined state without throwing', () => {
            const payload = { ...validAddressPayload, state: null };
            const result = uspsModule.validateAddressParams(payload);
            
            expect(result.errors).to.include('state');
            expect(result.params.state).to.be.undefined;
        });

        it('should reject null/undefined zipCode without throwing', () => {
            const payload = { ...validAddressPayload, zipCode: null };
            const result = uspsModule.validateAddressParams(payload);
            
            expect(result.errors).to.include('zipCode');
            expect(result.params.ZIPCode).to.be.undefined;
        });

        it('should reject undefined state without throwing', () => {
            const payload = { ...validAddressPayload };
            delete payload.state;
            const result = uspsModule.validateAddressParams(payload);
            
            expect(result.errors).to.include('state');
            expect(result.params.state).to.be.undefined;
        });

        it('should reject state with invalid length', () => {
            const payload = { ...validAddressPayload, state: 'NYC' };
            const result = uspsModule.validateAddressParams(payload);
            
            expect(result.errors).to.include('state');
            expect(result.params.state).to.be.undefined;
        });

        it('should reject zipCode with invalid length', () => {
            const payload = { ...validAddressPayload, zipCode: '1234' };
            const result = uspsModule.validateAddressParams(payload);
            
            expect(result.errors).to.include('zipCode');
            expect(result.params.ZIPCode).to.be.undefined;
        });

        it('should reject zipCode with non-digits', () => {
            const payload = { ...validAddressPayload, zipCode: '1234A' };
            const result = uspsModule.validateAddressParams(payload);
            
            expect(result.errors).to.include('zipCode');
            expect(result.params.ZIPCode).to.be.undefined;
        });

        it('should trim whitespace from all fields', () => {
            const payload = {
                streetAddress: '  123 Main St  ',
                secondaryAddress: '  Apt 4B  ',
                city: '  Anytown  ',
                state: '  ny  ',
                zipCode: '  12345  '
            };
            const result = uspsModule.validateAddressParams(payload);
            
            expect(result.errors).to.be.empty;
            expect(result.params.streetAddress).to.equal('123 Main St');
            expect(result.params.secondaryAddress).to.equal('Apt 4B');
            expect(result.params.city).to.equal('Anytown');
            expect(result.params.state).to.equal('NY');
            expect(result.params.ZIPCode).to.equal('12345');
        });

        it('should return all missing required fields', () => {
            const result = uspsModule.validateAddressParams({});
            
            expect(result.errors).to.have.lengthOf(4);
            expect(result.errors).to.include.members(['streetAddress', 'city', 'state', 'zipCode']);
        });

        it('should handle string JSON input', () => {
            const result = uspsModule.validateAddressParams(JSON.stringify(validAddressPayload));
            
            expect(result.errors).to.be.empty;
            expect(result.params.streetAddress).to.equal('123 Main St');
        });

        it('should handle null payload', () => {
            const result = uspsModule.validateAddressParams(null);
            
            expect(result.errors).to.have.lengthOf(1);
            expect(result.errors[0]).to.include('Invalid or missing all fields');
        });

        it('should handle non-object payload', () => {
            const result = uspsModule.validateAddressParams('invalid');
            
            expect(result.errors).to.have.lengthOf(1);
            expect(result.errors[0]).to.include('Invalid or missing all fields');
        });
    });

    describe('Parameter Validation - Integration Tests', () => {
        it('should return error for missing body', async () => {
            const req = { method: 'POST' }; 
            const res = {
                status: sinon.stub().returnsThis(),
                json: sinon.stub()
            };

            await uspsModule.addressValidation(req, res);

            expect(res.status.calledWith(400)).to.be.true;
            expect(res.json.firstCall.args[0].message).to.equal('Bad Request');
        });

        it('should return error for invalid fields', async () => {
            const req = { 
                method: 'POST', 
                body: { invalidField: 'value' } 
            };
            const res = {
                status: sinon.stub().returnsThis(),
                json: sinon.stub()
            };

            await uspsModule.addressValidation(req, res);

            expect(res.status.calledWith(400)).to.be.true;
            expect(res.json.firstCall.args[0].message).to.include('Invalid or missing fields');
        });

        it('should parse string body correctly', async () => {
            const req = { 
                method: 'POST', 
                body: JSON.stringify(validAddressPayload) 
            };
            const res = {
                status: sinon.stub().returnsThis(),
                json: sinon.stub()
            };

            fetchStub.onFirstCall().resolves(mockResponse({ access_token: mockToken, expires_in: mockExpiresIn }));
            fetchStub.onSecondCall().resolves(mockResponse({ address: 'validated' }));

            await uspsModule.addressValidation(req, res);

            expect(res.status.calledWith(200)).to.be.true;
        });
    });

    describe('Token Management & Authentication', () => {
        it('should fetch new token when cache is empty', async () => {
            const req = { method: 'POST', body: validAddressPayload };
            const res = { status: sinon.stub().returnsThis(), json: sinon.stub() };

            // Mock Firestore empty cache result
            firestoreModule.db.collection().get.resolves({ empty: true });

            // Mock Auth Call
            fetchStub.onFirstCall().resolves(mockResponse({ access_token: mockToken, expires_in: mockExpiresIn }));

            // Mock Address Call
            fetchStub.onSecondCall().resolves(mockResponse({ result: 'success' }));

            await uspsModule.addressValidation(req, res);

            // Verify Auth Call
            const authCall = fetchStub.firstCall;
            const body = authCall.args[1].body;
            expect(body.get('grant_type')).to.equal('client_credentials');
            expect(body.get('client_id')).to.equal('mock-client-id');
        });

        it('should use cached token from Firestore if available', async () => {
            const req = { method: 'POST', body: validAddressPayload };
            const res = { status: sinon.stub().returnsThis(), json: sinon.stub() };

            const mockDoc = {
                exists: true,
                data: () => ({
                    usps: {
                        token: 'firestore-token',
                        expiresAt: Date.now() + 1000000
                    }
                }),
                ref: { set: sinon.stub().resolves() }
            };
            mockDoc.ref.get = sinon.stub().resolves(mockDoc); // ensure get returns the doc
            
            firestoreModule.db.collection().get.resolves({
                empty: false,
                docs: [mockDoc]
            });
            // Also need to stub the .get() on the docRef returned by getAppSettingsDocAndSnapshot
            mockDoc.get = sinon.stub().resolves(mockDoc); 


            // Mock Address Call directly (no auth call expected)
            fetchStub.resolves(mockResponse({ result: 'success' }));

            await uspsModule.addressValidation(req, res);

            // Expect only 1 call for address validation (no auth call due to cached token)
            expect(fetchStub.callCount).to.equal(1);
            expect(fetchStub.firstCall.args[1].headers.Authorization).to.equal('Bearer firestore-token');
        });

        it('should refresh token on 401 Unauthorized', async () => {
            const req = { method: 'POST', body: validAddressPayload };
            const res = { status: sinon.stub().returnsThis(), json: sinon.stub() };

            // Initial Auth Call (Success)
            fetchStub.onFirstCall().resolves(mockResponse({ access_token: 'expired-token', expires_in: mockExpiresIn }));

            // Address Call (Fail with 401)
            fetchStub.onSecondCall().resolves(mockResponse({ error: 'Unauthorized' }, 401, false));

            // Auth Refresh Call (Success)
            fetchStub.onThirdCall().resolves(mockResponse({ access_token: 'new-token', expires_in: mockExpiresIn }));

            // Retry Address Call (Success)
            fetchStub.onCall(3).resolves(mockResponse({ result: 'success' }));

            await uspsModule.addressValidation(req, res);

            expect(fetchStub.callCount).to.equal(4);
            // 3rd call (index 2) should be the auth refresh
            expect(fetchStub.getCall(2).args[0]).to.equal(sharedModule.uspsUrl.auth);
            // 4th call (index 3) should use the new token
            expect(fetchStub.getCall(3).args[1].headers.Authorization).to.equal('Bearer new-token');
        });
    });

    describe('Error Handling & Retries', () => {
        it('should retry on network error', async () => {
            const req = { method: 'POST', body: validAddressPayload };
            const res = { status: sinon.stub().returnsThis(), json: sinon.stub() };

            // Auth Success
            fetchStub.onFirstCall().resolves(mockResponse({ access_token: mockToken, expires_in: mockExpiresIn }));

            // Address Network Error
            fetchStub.onSecondCall().rejects(new Error('Network Error'));

            // Retry Address Success
            fetchStub.onThirdCall().resolves(mockResponse({ result: 'success' }));

            await uspsModule.addressValidation(req, res);

            expect(sharedModule.delay.called).to.be.true;
            expect(res.status.calledWith(200)).to.be.true;
        });

        it('should fail gracefully after exhausting retries', async () => {
            const req = { method: 'POST', body: validAddressPayload };
            const res = { status: sinon.stub().returnsThis(), json: sinon.stub() };

            fetchStub.onFirstCall().resolves(mockResponse({ access_token: mockToken, expires_in: mockExpiresIn }));

            // Fail all subsequent calls
            fetchStub.callsFake(async (url) => {
                // If it's an auth URL (string check), return success
                if (typeof url === 'string' && url.includes('oauth')) {
                    return mockResponse({ access_token: mockToken });
                }
                // Otherwise (address URL), fail
                throw new Error('Persistent Error');
            });

            await uspsModule.addressValidation(req, res);

            expect(res.status.calledWith(502)).to.be.true;
            expect(res.json.firstCall.args[0].message).to.include('temporarily unavailable');
        });

        it('should handle non-retryable 4xx errors immediately', async () => {
            const req = { method: 'POST', body: validAddressPayload };
            const res = { status: sinon.stub().returnsThis(), json: sinon.stub() };

            // Mock Auth Success
            fetchStub.onFirstCall().resolves(mockResponse({ access_token: mockToken, expires_in: mockExpiresIn }));

            // Mock 422 Unprocessable Entity
            fetchStub.onSecondCall().resolves(mockResponse({ error: 'Invalid Address' }, 422, false));

            await uspsModule.addressValidation(req, res);

            expect(fetchStub.callCount).to.equal(2); 
            expect(res.status.calledWith(502)).to.be.true;
        });
    });
});
