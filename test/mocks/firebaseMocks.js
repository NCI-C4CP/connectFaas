const sinon = require('sinon');

/**
 * Firebase Mock Factory
 * Mocking for Firebase Admin SDK and Firestore operations
 */
class FirebaseMockFactory {
    constructor() {
        this.reset();
    }

    // Reset all mocks to initial state    
    reset() {
        this.mockDocs = new Map();              // Map of mock documents
        this.mockCollections = new Map();       // Map of mock collections
        this.mockQueries = new Map();           // Map of mock queries (where, select, limit, etc.)
        this.transactionResults = new Map();    // Map of mock transaction results
        this.batchResults = new Map();          // Map of mock batch results (set, update, delete)
        
        // Create base mocks
        this.createBaseMocks();
    }

    // Create base mock objects
    createBaseMocks() {
        // Mock FieldValue
        this.mockFieldValue = {
            arrayUnion: sinon.stub().returns('arrayUnion'),
            arrayRemove: sinon.stub().returns('arrayRemove'),
            increment: sinon.stub().returns('increment'),
            serverTimestamp: sinon.stub().returns('serverTimestamp')
        };

        // Mock Document Reference
        this.mockFirestoreDoc = {
            get: sinon.stub(),
            set: sinon.stub(),
            update: sinon.stub(),
            delete: sinon.stub(),
            onSnapshot: sinon.stub(),
            ref: null
        };

        // Mock Query
        this.mockFirestoreQuery = {
            where: sinon.stub().returnsThis(),
            select: sinon.stub().returnsThis(),
            limit: sinon.stub().returnsThis(),
            orderBy: sinon.stub().returnsThis(),
            offset: sinon.stub().returnsThis(),
            startAfter: sinon.stub().returnsThis(),
            endBefore: sinon.stub().returnsThis(),
            get: sinon.stub().resolves({ empty: true, size: 0, docs: [] }),
            onSnapshot: sinon.stub(),
            count: sinon.stub().returns({
                get: sinon.stub().resolves({ data: () => ({ count: 0 }) })
            })
        };

        // Mock Collection Reference
        this.mockFirestoreCollection = {
            doc: sinon.stub().returns(this.mockFirestoreDoc),
            where: sinon.stub().returns(this.mockFirestoreQuery),
            add: sinon.stub(),
            get: sinon.stub().resolves({ empty: true, size: 0, docs: [] }),
            onSnapshot: sinon.stub(),
            count: sinon.stub().returns({
                get: sinon.stub().resolves({ data: () => ({ count: 0 }) })
            })
        };

        // Mock Firestore Database
        this.mockFirestore = {
            collection: sinon.stub().returns(this.mockFirestoreCollection),
            batch: sinon.stub().returns(this.createMockBatch()),
            runTransaction: sinon.stub().callsFake(async (updateFunction) => {
                const transaction = this.createMockTransaction();
                return await updateFunction(transaction);
            }),
            settings: sinon.stub()
        };

        // Mock Firebase Admin
        this.mockAdmin = {
            firestore: sinon.stub().returns(this.mockFirestore),
            initializeApp: sinon.stub(),
            apps: []
        };
    }

    // Create a mock batch operation
    createMockBatch() {
        const batch = {
            set: sinon.stub(),
            update: sinon.stub(),
            delete: sinon.stub(),
            commit: sinon.stub().resolves()
        };
        
        // Method chaining after batch object is created
        batch.set.returns(batch);
        batch.update.returns(batch);
        batch.delete.returns(batch);
        
        return batch;
    }

    // Create a mock transaction
    createMockTransaction() {
        const transaction = {
            get: sinon.stub(),
            set: sinon.stub(),
            update: sinon.stub(),
            delete: sinon.stub(),
            where: sinon.stub().returns({
                select: sinon.stub().returnsThis(),
                limit: sinon.stub().returnsThis(),
                get: sinon.stub().resolves({ empty: true, size: 0, docs: [] })
            })
        };
        return transaction;
    }

    /**
     * Mock data for a specific collection
     * @param {string} collectionPath - The collection path (e.g., 'participants')
     * @param {Array} documents - Array of document data
     * @param {string} idField - Field to use as document ID (default: 'id')
     */
    setupCollectionData(collectionPath, documents = [], idField = 'id') {
        const collectionRef = this.mockFirestore.collection.withArgs(collectionPath);
        
        // Create mock documents
        const mockDocs = documents.map(doc => ({
            id: doc[idField] || doc.id,
            data: () => doc,
            exists: true,
            ref: { id: doc[idField] || doc.id }
        }));

        // Mock collection query results
        const mockQuerySnapshot = {
            empty: mockDocs.length === 0,
            size: mockDocs.length,
            docs: mockDocs,
            forEach: (callback) => mockDocs.forEach(callback)
        };

        // Set up collection methods
        collectionRef.returns({
            ...this.mockFirestoreCollection,
            get: sinon.stub().resolves(mockQuerySnapshot),
            where: sinon.stub().returns({
                ...this.mockFirestoreQuery,
                get: sinon.stub().resolves(mockQuerySnapshot)
            }),
            doc: (docId) => {
                const doc = mockDocs.find(d => d.id === docId);
                return {
                    ...this.mockFirestoreDoc,
                    get: sinon.stub().resolves(doc || { exists: false, data: () => null }),
                    set: sinon.stub().resolves(),
                    update: sinon.stub().resolves(),
                    delete: sinon.stub().resolves()
                };
            }
        });

        return collectionRef;
    }

    /**
     * Mock data for a specific document
     * @param {string} collectionPath - The collection path
     * @param {string} docId - The document ID
     * @param {Object} data - The document data
     */
    setupDocumentData(collectionPath, docId, data) {
        const docRef = this.mockFirestore.collection(collectionPath).doc(docId);
        docRef.get.resolves({
            exists: true,
            data: () => data,
            id: docId
        });
        return docRef;
    }

    /**
     * Mock query results
     * @param {string} collectionPath - The collection path
     * @param {Array} documents - Array of documents to return
     * @param {Object} queryOptions - Query options (where, select, limit, etc.)
     */
    setupQueryResults(collectionPath, documents = [], queryOptions = {}) {
        const mockDocs = documents.map(doc => ({
            id: doc.id || doc.docId,
            data: () => doc,
            exists: true,
            ref: { id: doc.id || doc.docId }
        }));

        const mockQuerySnapshot = {
            empty: mockDocs.length === 0,
            size: mockDocs.length,
            docs: mockDocs,
            forEach: (callback) => mockDocs.forEach(callback)
        };

        const collectionRef = this.mockFirestore.collection.withArgs(collectionPath);
        collectionRef.returns({
            ...this.mockFirestoreCollection,
            where: sinon.stub().returns({
                ...this.mockFirestoreQuery,
                select: sinon.stub().returnsThis(),
                limit: sinon.stub().returnsThis(),
                get: sinon.stub().resolves(mockQuerySnapshot)
            })
        });

        return mockQuerySnapshot;
    }

    /**
     * Mock transaction behavior
     * @param {Function} transactionHandler - Function to handle transaction logic
     */
    setupTransaction(transactionHandler) {
        this.mockFirestore.runTransaction.callsFake(async (updateFunction) => {
            const transaction = this.createMockTransaction();
            return await updateFunction(transaction);
        });
    }

    /**
     * Mock batch behavior
     * @param {Object} batchResults - Expected batch results
     */
    setupBatch(batchResults = { success: true, errorCount: 0 }) {
        const batch = this.createMockBatch();
        batch.commit.resolves(batchResults);
        this.mockFirestore.batch.returns(batch);
        return batch;
    }

    /**
     * Mock for count() operation
     * @param {string} collectionPath - The collection path
     * @param {number} count - The count to return
     */
    setupCount(collectionPath, count = 0) {
        const collectionRef = this.mockFirestore.collection.withArgs(collectionPath);
        collectionRef.returns({
            ...this.mockFirestoreCollection,
            count: sinon.stub().returns({
                get: sinon.stub().resolves({ data: () => ({ count }) })
            })
        });
    }

    /**
     * Testing environment variables
     */
    setupEnvironment() {
        process.env.NODE_ENV = 'test';
        process.env.DHQ_TOKEN = 'test-token';
        global.fetch = sinon.stub();
    }

    /**
     * Module mocking for Firebase Admin SDK
     */
    setupModuleMocks() {
        const Module = require('module');
        const originalRequire = Module.prototype.require;
        const self = this;

        Module.prototype.require = function(id) {
            if (id === 'firebase-admin') {
                return self.mockAdmin;
            }
            if (id === 'firebase-admin/firestore') {
                return { FieldValue: self.mockFieldValue };
            }
            // Only intercept the above, otherwise use the original require
            return originalRequire.apply(this, arguments);
        };

        return () => {
            Module.prototype.require = originalRequire;
        };
    }

    // Mock for console methods
    setupConsoleMocks() {
        const consoleMocks = {
            log: sinon.spy(console, 'log'),
            warn: sinon.spy(console, 'warn'),
            error: sinon.spy(console, 'error')
        };

        return {
            ...consoleMocks,
            restore: () => {
                consoleMocks.log.restore();
                consoleMocks.warn.restore();
                consoleMocks.error.restore();
            }
        };
    }

    // Get all mock objects for external use
    getMocks() {
        return {
            admin: this.mockAdmin,
            firestore: this.mockFirestore,
            fieldValue: this.mockFieldValue,
            doc: this.mockFirestoreDoc,
            collection: this.mockFirestoreCollection,
            query: this.mockFirestoreQuery
        };
    }

    /**
     * Test helper for common Firestore operations
     */
    createTestHelper() {
        const fieldMapping = require('../../utils/fieldToConceptIdMapping');
        return {
            // Helper to create a mock participant
            createMockDHQParticipant: (uid, data = {}) => ({
                id: uid,
                data: () => ({
                    state: { uid },
                    [fieldMapping.dhq3StudyID]: 'study_123',
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
                const mockDocs = participants.map(p => this.createTestHelper().createMockDHQParticipant(p.uid, p.data));
                return {
                    empty: mockDocs.length === 0,
                    size: mockDocs.length,
                    docs: mockDocs,
                    forEach: (callback) => mockDocs.forEach(callback)
                };
            },

            // Create mock app settings
            createMockAppSettings: (settings = {}) => ({
                id: 'connectApp',
                data: () => ({
                    appName: 'connectApp',
                    dhq: {
                        dhqStudyIDs: ['study_123', 'study_456'],
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

/**
 * Create and configure a Firebase mock factory instance
 * @param {Object} options - Configuration options
 * @returns {FirebaseMockFactory} Configured mock factory
 */
function createFirebaseMocks(options = {}) {
    const factory = new FirebaseMockFactory();
    
    // Set up environment
    factory.setupEnvironment();
    
    // Set up module mocks (optional)
    let restoreRequire = null;
    if (options.setupModuleMocks !== false) {
        restoreRequire = factory.setupModuleMocks();
    }
    
    // Set up console mocks (optional)
    const consoleMocks = options.setupConsole ? factory.setupConsoleMocks() : null;
    
    return {
        factory,
        mocks: factory.getMocks(),
        helper: factory.createTestHelper(),
        restore: () => {
            if (restoreRequire) {
                restoreRequire();
            }
            if (consoleMocks) {
                consoleMocks.restore();
            }
            factory.reset();
        }
    };
}

module.exports = {
    FirebaseMockFactory,
    createFirebaseMocks
};
