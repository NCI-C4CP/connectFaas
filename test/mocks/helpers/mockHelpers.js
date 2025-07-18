const sinon = require('sinon');

/**
 * Mock helper utilities
 */
class MockHelpers {
    constructor() {
        this.reset();
    }

    reset() {
        if (this.sandbox) {
            this.sandbox.restore();
        }
        this.sandbox = sinon.createSandbox();
    }

    /**
     * Setup environment variables for testing
     */
    setupEnvironment() {
        const TEST_CONSTANTS = require('../../constants');
        process.env.NODE_ENV = TEST_CONSTANTS.ENV.NODE_ENV;
        process.env.DHQ_TOKEN = TEST_CONSTANTS.ENV.TEST_TOKEN;
        global.fetch = sinon.stub();
    }

    /**
     * Module mocking for Firebase Admin SDK
     */
    setupModuleMocks(mockAdmin) {
        const Module = require('module');
        const originalRequire = Module.prototype.require;

        Module.prototype.require = function(id) {
            if (id === 'firebase-admin') {
                return mockAdmin;
            }
            if (id === 'firebase-admin/firestore') {
                return { FieldValue: mockAdmin.firestore().FieldValue };
            }
            return originalRequire.apply(this, arguments);
        };

        return () => {
            Module.prototype.require = originalRequire;
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
                if (consoleMocks.log && consoleMocks.log.restore) consoleMocks.log.restore();
                if (consoleMocks.warn && consoleMocks.warn.restore) consoleMocks.warn.restore();
                if (consoleMocks.error && consoleMocks.error.restore) consoleMocks.error.restore();
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
                const mockDocs = participants.map(p => this.createMockDHQParticipant(p.uid, p.data));
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