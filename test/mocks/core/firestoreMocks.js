const sinon = require('sinon');

/**
 * Firestore mocks
 */
class FirestoreMocks {
    constructor() {
        this.reset();
    }

    reset() {
        if (this.sandbox) {
            this.sandbox.restore();
        }
        this.sandbox = sinon.createSandbox();
        this.mockDocs = new Map();
        this.mockCollections = new Map();
        this.mockQueries = new Map();
        this.testDocumentRegistry = new Map();
        
        this.createBaseMocks();
    }

    clearDocumentRegistry() {
        this.testDocumentRegistry = new Map();
    }

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
            collection: sinon.stub().callsFake((collectionPath) => {
                const self = this;
                const collectionMock = {
                    ...this.mockFirestoreCollection,
                    doc: sinon.stub().callsFake((docId) => {
                        const currentCollectionRegistry = self.testDocumentRegistry.get(collectionPath);
                        const retrievedMockDoc = currentCollectionRegistry && currentCollectionRegistry.has(docId) 
                            ? currentCollectionRegistry.get(docId)
                            : { exists: false, data: () => null };
                        
                        return {
                            get: sinon.stub().resolves(retrievedMockDoc),
                            set: sinon.stub().resolves(),
                            update: sinon.stub().resolves(),
                            delete: sinon.stub().resolves()
                        };
                    })
                };
                return collectionMock;
            }),
            batch: sinon.stub().returns(this.createMockBatch()),
            runTransaction: sinon.stub().callsFake(async (updateFunction) => {
                const transaction = this.createMockTransaction();
                return await updateFunction(transaction);
            }),
            settings: sinon.stub()
        };
    }

    createMockBatch() {
        const batch = {
            set: sinon.stub(),
            update: sinon.stub(),
            delete: sinon.stub(),
            commit: sinon.stub().resolves()
        };
        
        batch.set.returns(batch);
        batch.update.returns(batch);
        batch.delete.returns(batch);
        
        return batch;
    }

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

    setupCollectionData(collectionPath, documents = [], idField = 'id') {
        const collectionRef = this.mockFirestore.collection.withArgs(collectionPath);
        
        const mockDocs = documents.map(doc => ({
            id: doc[idField] || doc.id,
            data: () => doc,
            exists: true,
            ref: { id: doc[idField] || doc.id }
        }));

        const mockQuerySnapshot = {
            empty: mockDocs.length === 0,
            size: mockDocs.length,
            docs: mockDocs,
            forEach: (callback) => mockDocs.forEach(callback)
        };

        collectionRef.returns({
            ...this.mockFirestoreCollection,
            get: sinon.stub().resolves(mockQuerySnapshot),
            where: sinon.stub().returns({
                ...this.mockFirestoreQuery,
                get: sinon.stub().resolves(mockQuerySnapshot)
            }),
            doc: (docId) => {
                const doc = mockDocs.find(d => d.id === docId);
                const mockDoc = doc ? {
                    exists: true,
                    data: () => doc.data(),
                    id: doc.id
                } : {
                    exists: false,
                    data: () => null
                };
                
                return {
                    ...this.mockFirestoreDoc,
                    get: sinon.stub().resolves(mockDoc),
                    set: sinon.stub().resolves(),
                    update: sinon.stub().resolves(),
                    delete: sinon.stub().resolves()
                };
            }
        });

        return collectionRef;
    }

    setupDocumentRetrieval(collectionPath, docId, data = null) {
        if (!this.testDocumentRegistry) {
            this.testDocumentRegistry = new Map();
        }
        
        if (!this.testDocumentRegistry.has(collectionPath)) {
            this.testDocumentRegistry.set(collectionPath, new Map());
        }
        
        const collectionRegistry = this.testDocumentRegistry.get(collectionPath);
        const mockDoc = data ? {
            exists: true,
            data: () => data,
            id: docId
        } : {
            exists: false,
            data: () => null
        };
        
        collectionRegistry.set(docId, mockDoc);
    }

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

    setupTransaction(transactionHandler) {
        this.mockFirestore.runTransaction.callsFake(async (updateFunction) => {
            const transaction = this.createMockTransaction();
            return await updateFunction(transaction);
        });
    }

    setupBatch(batchResults = { success: true, errorCount: 0 }) {
        const batch = this.createMockBatch();
        batch.commit.resolves(batchResults);
        this.mockFirestore.batch.returns(batch);
        return batch;
    }

    setupCount(collectionPath, count = 0) {
        const collectionRef = this.mockFirestore.collection.withArgs(collectionPath);
        collectionRef.returns({
            ...this.mockFirestoreCollection,
            count: sinon.stub().returns({
                get: sinon.stub().resolves({ data: () => ({ count }) })
            })
        });
    }

    getMocks() {
        return {
            firestore: this.mockFirestore,
            fieldValue: this.mockFieldValue,
            doc: this.mockFirestoreDoc,
            collection: this.mockFirestoreCollection,
            query: this.mockFirestoreQuery
        };
    }
}

module.exports = FirestoreMocks;