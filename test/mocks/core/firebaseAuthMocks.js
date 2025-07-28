const sinon = require('sinon');

/**
 * Firebase Auth mock
 */
class FirebaseAuthMocks {
    constructor() {
        this.reset();
    }

    reset() {
        if (this.sandbox) {
            this.sandbox.restore();
        }
        this.sandbox = sinon.createSandbox();
        this.createBaseMocks();
    }

    createBaseMocks() {
        // Mock User
        this.mockUser = {
            uid: 'test-uid',
            email: 'test@example.com',
            displayName: 'Test User',
            emailVerified: true,
            getIdToken: sinon.stub().resolves('mock-token'),
            delete: sinon.stub().resolves(),
            updateProfile: sinon.stub().resolves(),
            updateEmail: sinon.stub().resolves(),
            updatePassword: sinon.stub().resolves()
        };

        // Mock UserRecord
        this.mockUserRecord = {
            uid: 'test-uid',
            email: 'test@example.com',
            displayName: 'Test User',
            emailVerified: true,
            disabled: false,
            metadata: {
                creationTime: new Date().toISOString(),
                lastSignInTime: new Date().toISOString()
            },
            customClaims: {},
            providerData: []
        };

        // Mock Auth
        this.mockAuth = {
            createUser: sinon.stub().resolves(this.mockUserRecord),
            getUser: sinon.stub().resolves(this.mockUserRecord),
            getUserByEmail: sinon.stub().resolves(this.mockUserRecord),
            updateUser: sinon.stub().resolves(this.mockUserRecord),
            deleteUser: sinon.stub().resolves(),
            listUsers: sinon.stub().resolves({
                users: [this.mockUserRecord],
                pageToken: undefined
            }),
            createCustomToken: sinon.stub().resolves('custom-token'),
            verifyIdToken: sinon.stub().resolves({
                uid: 'test-uid',
                email: 'test@example.com',
                email_verified: true
            }),
            setCustomUserClaims: sinon.stub().resolves(),
            revokeRefreshTokens: sinon.stub().resolves()
        };
    }

    setupUser(userData) {
        Object.assign(this.mockUser, userData);
        Object.assign(this.mockUserRecord, userData);
        return this.mockUser;
    }

    setupAuthError(method, error) {
        this.mockAuth[method].rejects(error);
    }

    getMocks() {
        return {
            auth: this.mockAuth,
            user: this.mockUser,
            userRecord: this.mockUserRecord
        };
    }
}

module.exports = FirebaseAuthMocks;