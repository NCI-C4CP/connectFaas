/**
 * Firebase Auth mock
 */
class FirebaseAuthMocks {
    constructor() {
        this.reset();
    }

    reset() {
        this.createBaseMocks();
    }

    createBaseMocks() {
        // Mock User
        this.mockUser = {
            uid: 'test-uid',
            email: 'test@example.com',
            displayName: 'Test User',
            emailVerified: true,
            getIdToken: vi.fn().mockResolvedValue('mock-token'),
            delete: vi.fn().mockResolvedValue(undefined),
            updateProfile: vi.fn().mockResolvedValue(undefined),
            updateEmail: vi.fn().mockResolvedValue(undefined),
            updatePassword: vi.fn().mockResolvedValue(undefined)
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
            createUser: vi.fn().mockResolvedValue(this.mockUserRecord),
            getUser: vi.fn().mockResolvedValue(this.mockUserRecord),
            getUserByEmail: vi.fn().mockResolvedValue(this.mockUserRecord),
            updateUser: vi.fn().mockResolvedValue(this.mockUserRecord),
            deleteUser: vi.fn().mockResolvedValue(undefined),
            listUsers: vi.fn().mockResolvedValue({
                users: [this.mockUserRecord],
                pageToken: undefined
            }),
            createCustomToken: vi.fn().mockResolvedValue('custom-token'),
            verifyIdToken: vi.fn().mockResolvedValue({
                uid: 'test-uid',
                email: 'test@example.com',
                email_verified: true
            }),
            setCustomUserClaims: vi.fn().mockResolvedValue(undefined),
            revokeRefreshTokens: vi.fn().mockResolvedValue(undefined)
        };
    }

    setupUser(userData) {
        Object.assign(this.mockUser, userData);
        Object.assign(this.mockUserRecord, userData);
        return this.mockUser;
    }

    setupAuthError(method, error) {
        this.mockAuth[method].mockRejectedValue(error);
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
