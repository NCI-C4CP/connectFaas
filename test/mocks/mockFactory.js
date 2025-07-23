const FirestoreMocks = require('./core/firestoreMocks');
const FirebaseAuthMocks = require('./core/firebaseAuthMocks');
const StorageMocks = require('./core/storageMocks');
const MockHelpers = require('./helpers/mockHelpers');

/**
 * Firebase Mock Factory
 * Orchestrates all mock services
 */
class FirebaseMockFactory {
    constructor() {
        this.firestoreMocks = new FirestoreMocks();
        this.firebaseAuthMocks = new FirebaseAuthMocks();
        this.storageMocks = new StorageMocks();
        this.helpers = new MockHelpers();
        
        this.createMockAdmin();
    }

    createMockAdmin() {
        this.mockAdmin = {
            firestore: () => this.firestoreMocks.getMocks().firestore,
            auth: () => this.firebaseAuthMocks.getMocks().auth,
            storage: () => this.storageMocks.getMocks().storage,
            initializeApp: () => {},
            apps: []
        };
    }

    reset() {
        this.firestoreMocks.reset();
        this.firebaseAuthMocks.reset();
        this.storageMocks.reset();
        this.helpers.reset();
        this.createMockAdmin();
    }

    // Delegate Firestore methods
    setupCollectionData(collectionPath, documents, idField) {
        return this.firestoreMocks.setupCollectionData(collectionPath, documents, idField);
    }

    setupDocumentRetrieval(collectionPath, docId, data) {
        return this.firestoreMocks.setupDocumentRetrieval(collectionPath, docId, data);
    }

    setupQueryResults(collectionPath, documents, queryOptions) {
        return this.firestoreMocks.setupQueryResults(collectionPath, documents, queryOptions);
    }

    setupTransaction(transactionHandler) {
        return this.firestoreMocks.setupTransaction(transactionHandler);
    }

    setupBatch(batchResults) {
        return this.firestoreMocks.setupBatch(batchResults);
    }

    setupCount(collectionPath, count) {
        return this.firestoreMocks.setupCount(collectionPath, count);
    }

    clearDocumentRegistry() {
        return this.firestoreMocks.clearDocumentRegistry();
    }

    // Firebase Auth methods
    setupUser(userData) {
        return this.firebaseAuthMocks.setupUser(userData);
    }

    setupAuthError(method, error) {
        return this.firebaseAuthMocks.setupAuthError(method, error);
    }

    // Firebase Storage methods
    setupFile(filePath, content) {
        return this.storageMocks.setupFile(filePath, content);
    }

    setupFileError(filePath, error) {
        return this.storageMocks.setupFileError(filePath, error);
    }

    // Firebase Storage methods
    setupBucket(bucketName) {
        return this.storageMocks.setupBucket(bucketName);
    }

    // Helper methods
    setupEnvironment() {
        return this.helpers.setupEnvironment();
    }

    setupModuleMocks() {
        return this.helpers.setupModuleMocks(this.mockAdmin);
    }

    setupConsoleMocks() {
        return this.helpers.setupConsoleMocks();
    }

    createTestHelper() {
        return this.helpers.createTestHelper();
    }

    getMocks() {
        return {
            admin: this.mockAdmin,
            ...this.firestoreMocks.getMocks(),
            ...this.firebaseAuthMocks.getMocks(),
            ...this.storageMocks.getMocks()
        };
    }
}

/**
 * Create and configure a Firebase mock factory instance
 */
function createFirebaseMocks(options = {}) {
    const factory = new FirebaseMockFactory();
    
    // Set up environment
    factory.setupEnvironment();
    
    // Set up module mocks (optional)
    if (options.setupModuleMocks !== false) {
        factory.setupModuleMocks();
    }
    
    // Set up console mocks (optional)
    if (options.setupConsole) {
        factory.setupConsoleMocks();
    }
    
    // Enable per-test-file isolation if requested
    if (options.isolatePerTestFile) {
        const originalReset = factory.reset;
        factory.reset = function() {
            originalReset.call(this);
            this.clearDocumentRegistry();
        };
    }
    
    return {
        factory,
        mocks: factory.getMocks(),
        helper: factory.createTestHelper(),
        restore: () => {
            factory.reset();
        }
    };
}

module.exports = {
    FirebaseMockFactory,
    createFirebaseMocks
};