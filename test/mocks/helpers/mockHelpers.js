/**
 * Mock helper utilities
 */
class MockHelpers {
    constructor() {
        this.reset();
    }

    reset() {
        // No sandbox needed with vi.fn()
    }

    /**
     * Setup environment variables for testing
     */
    setupEnvironment() {
        const TEST_CONSTANTS = require('../../constants');
        process.env.NODE_ENV = TEST_CONSTANTS.ENV.NODE_ENV;
        process.env.DHQ_TOKEN = TEST_CONSTANTS.ENV.TEST_TOKEN;
        global.fetch = vi.fn();
    }

    /**
     * Module mocking for Firebase Admin SDK
     * Uses require.cache replacement instead of Module.prototype.require hijacking.
     * This approach is compatible with Vitest's parallel execution and test isolation.
     */
    setupModuleMocks(mockAdmin) {
        const adminPath = require.resolve('firebase-admin');
        const firestorePath = require.resolve('firebase-admin/firestore');
        const fieldValue = mockAdmin.firestore().FieldValue || {
            arrayUnion: vi.fn().mockReturnValue('arrayUnion'),
            arrayRemove: vi.fn().mockReturnValue('arrayRemove'),
            increment: vi.fn().mockReturnValue('increment'),
            serverTimestamp: vi.fn().mockReturnValue('serverTimestamp'),
            delete: vi.fn().mockReturnValue('delete'),
        };

        // Save original cache entries (if they exist)
        const origAdmin = require.cache[adminPath];
        const origFirestore = require.cache[firestorePath];

        // Install mock admin into require.cache
        require.cache[adminPath] = {
            id: adminPath,
            filename: adminPath,
            loaded: true,
            exports: mockAdmin,
        };

        // Install mock firestore into require.cache
        require.cache[firestorePath] = {
            id: firestorePath,
            filename: firestorePath,
            loaded: true,
            exports: {
                FieldValue: fieldValue,
                FieldPath: {
                    documentId: vi.fn().mockReturnValue('__name__'),
                },
                Transaction: function Transaction() {},
                Filter: {},
            },
        };

        // Return restore function
        return () => {
            if (origAdmin) {
                require.cache[adminPath] = origAdmin;
            } else {
                delete require.cache[adminPath];
            }
            if (origFirestore) {
                require.cache[firestorePath] = origFirestore;
            } else {
                delete require.cache[firestorePath];
            }
        };
    }

    /**
     * Console mocking and stubbing
     */
    setupConsoleMocks() {
        const { createConsoleSafeStub } = require('../../shared/testHelpers');

        const consoleMocks = {
            log: createConsoleSafeStub('log'),
            warn: createConsoleSafeStub('warn'),
            error: createConsoleSafeStub('error')
        };

        return {
            ...consoleMocks,
            restore: () => {
                vi.restoreAllMocks();
            }
        };
    }

    /**
     * Test helper for Firebase operations
     */
    createTestHelper() {
        const fieldMapping = require('../../../utils/fieldToConceptIdMapping');
        const TEST_CONSTANTS = require('../../constants');

        return {
            // Helper to create a mock DHQ participant
            createMockDHQParticipant: (uid, data = {}) => ({
                id: uid,
                data: () => ({
                    state: { uid },
                    [fieldMapping.dhq3StudyID]: TEST_CONSTANTS.STUDY_IDS.DEFAULT,
                    [fieldMapping.dhq3Username]: `user_${uid}`,
                    [fieldMapping.dhq3UUID]: `uuid_${uid}`,
                    [fieldMapping.dhq3SurveyStatus]: fieldMapping.notStarted,
                    ...data
                }),
                exists: true,
                ref: { id: uid }
            }),

            // Create mock participants collection
            createMockParticipantsCollection: (participants = []) => {
                const mockDocs = participants.map((p) => ({
                    id: p.uid,
                    data: () => ({
                        state: { uid: p.uid },
                        [fieldMapping.dhq3StudyID]: TEST_CONSTANTS.STUDY_IDS.DEFAULT,
                        [fieldMapping.dhq3Username]: `user_${p.uid}`,
                        [fieldMapping.dhq3UUID]: `uuid_${p.uid}`,
                        [fieldMapping.dhq3SurveyStatus]: fieldMapping.notStarted,
                        ...p.data
                    }),
                    exists: true,
                    ref: { id: p.uid }
                }));
                return {
                    empty: mockDocs.length === 0,
                    size: mockDocs.length,
                    docs: mockDocs,
                    forEach: (callback) => mockDocs.forEach(callback)
                };
            },

            // Create mock app settings
            createMockAppSettings: (settings = {}) => ({
                id: TEST_CONSTANTS.DOCS.CONNECT_APP,
                data: () => ({
                    appName: 'connectApp',
                    dhq: {
                        dhqStudyIDs: [TEST_CONSTANTS.STUDY_IDS.DEFAULT, TEST_CONSTANTS.STUDY_IDS.SECONDARY],
                        lookbackDays: 30,
                        lowCredentialWarningThreshold: 1000,
                        dhqDepletedCredentials: [],
                        ...settings.dhq
                    },
                    ...settings
                }),
                exists: true
            })
        };
    }
}

module.exports = MockHelpers;
