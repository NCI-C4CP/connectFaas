/**
 * Firestore mocks
 */
class FirestoreMocks {
    constructor() {
        this.reset();
    }

    reset() {
        this.mockDocs = new Map();
        this.mockCollections = new Map();
        this.mockQueries = new Map();
        this.testDocumentRegistry = new Map();
        this._collectionOverrides = new Map();

        this.createBaseMocks();
    }

    clearDocumentRegistry() {
        this.testDocumentRegistry = new Map();
    }

    createBaseMocks() {
        // Mock FieldValue
        this.mockFieldValue = {
            arrayUnion: vi.fn().mockReturnValue('arrayUnion'),
            arrayRemove: vi.fn().mockReturnValue('arrayRemove'),
            increment: vi.fn().mockReturnValue('increment'),
            serverTimestamp: vi.fn().mockReturnValue('serverTimestamp')
        };

        // Mock Document Reference
        this.mockFirestoreDoc = {
            get: vi.fn(),
            set: vi.fn(),
            update: vi.fn(),
            delete: vi.fn(),
            onSnapshot: vi.fn(),
            ref: null
        };

        // Mock Query
        this.mockFirestoreQuery = {
            where: vi.fn().mockReturnThis(),
            select: vi.fn().mockReturnThis(),
            limit: vi.fn().mockReturnThis(),
            orderBy: vi.fn().mockReturnThis(),
            offset: vi.fn().mockReturnThis(),
            startAfter: vi.fn().mockReturnThis(),
            endBefore: vi.fn().mockReturnThis(),
            get: vi.fn().mockResolvedValue({ empty: true, size: 0, docs: [] }),
            onSnapshot: vi.fn(),
            count: vi.fn().mockReturnValue({
                get: vi.fn().mockResolvedValue({ data: () => ({ count: 0 }) })
            })
        };

        // Mock Collection Reference
        this.mockFirestoreCollection = {
            doc: vi.fn().mockReturnValue(this.mockFirestoreDoc),
            where: vi.fn().mockReturnValue(this.mockFirestoreQuery),
            add: vi.fn(),
            get: vi.fn().mockResolvedValue({ empty: true, size: 0, docs: [] }),
            onSnapshot: vi.fn(),
            count: vi.fn().mockReturnValue({
                get: vi.fn().mockResolvedValue({ data: () => ({ count: 0 }) })
            })
        };

        const self = this;

        // Mock Firestore Database
        this.mockFirestore = {
            collection: vi.fn().mockImplementation((collectionPath) => {
                // Check for collection overrides first
                if (self._collectionOverrides.has(collectionPath)) {
                    return self._collectionOverrides.get(collectionPath);
                }

                const collectionMock = {
                    ...self.mockFirestoreCollection,
                    doc: vi.fn().mockImplementation((docId) => {
                        const currentCollectionRegistry = self.testDocumentRegistry.get(collectionPath);
                        const retrievedMockDoc = currentCollectionRegistry && currentCollectionRegistry.has(docId)
                            ? currentCollectionRegistry.get(docId)
                            : { exists: false, data: () => null };

                        return {
                            get: vi.fn().mockResolvedValue(retrievedMockDoc),
                            set: vi.fn().mockResolvedValue(undefined),
                            update: vi.fn().mockResolvedValue(undefined),
                            delete: vi.fn().mockResolvedValue(undefined)
                        };
                    })
                };
                return collectionMock;
            }),
            batch: vi.fn().mockImplementation(() => self.createMockBatch()),
            runTransaction: vi.fn().mockImplementation(async (updateFunction) => {
                const transaction = self.createMockTransaction();
                return await updateFunction(transaction);
            }),
            settings: vi.fn()
        };
    }

    createMockBatch() {
        const batch = {
            set: vi.fn(),
            update: vi.fn(),
            delete: vi.fn(),
            commit: vi.fn().mockResolvedValue(undefined)
        };

        batch.set.mockReturnValue(batch);
        batch.update.mockReturnValue(batch);
        batch.delete.mockReturnValue(batch);

        return batch;
    }

    createMockTransaction() {
        const transaction = {
            get: vi.fn(),
            set: vi.fn(),
            update: vi.fn(),
            delete: vi.fn(),
            where: vi.fn().mockReturnValue({
                select: vi.fn().mockReturnThis(),
                limit: vi.fn().mockReturnThis(),
                get: vi.fn().mockResolvedValue({ empty: true, size: 0, docs: [] })
            })
        };
        return transaction;
    }

    setupCollectionData(collectionPath, documents = [], idField = 'id') {
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

        const self = this;
        const collectionObj = {
            ...this.mockFirestoreCollection,
            get: vi.fn().mockResolvedValue(mockQuerySnapshot),
            where: vi.fn().mockReturnValue({
                ...this.mockFirestoreQuery,
                get: vi.fn().mockResolvedValue(mockQuerySnapshot)
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
                    ...self.mockFirestoreDoc,
                    get: vi.fn().mockResolvedValue(mockDoc),
                    set: vi.fn().mockResolvedValue(undefined),
                    update: vi.fn().mockResolvedValue(undefined),
                    delete: vi.fn().mockResolvedValue(undefined)
                };
            }
        };

        this._collectionOverrides.set(collectionPath, collectionObj);
        return collectionObj;
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

        const collectionObj = {
            ...this.mockFirestoreCollection,
            where: vi.fn().mockReturnValue({
                ...this.mockFirestoreQuery,
                select: vi.fn().mockReturnThis(),
                limit: vi.fn().mockReturnThis(),
                get: vi.fn().mockResolvedValue(mockQuerySnapshot)
            })
        };

        this._collectionOverrides.set(collectionPath, collectionObj);
        return mockQuerySnapshot;
    }

    setupTransaction(transactionHandler) {
        this.mockFirestore.runTransaction.mockImplementation(async (updateFunction) => {
            const transaction = this.createMockTransaction();
            return await updateFunction(transaction);
        });
    }

    setupBatch(batchResults = { success: true, errorCount: 0 }) {
        const batch = this.createMockBatch();
        batch.commit.mockResolvedValue(batchResults);
        this.mockFirestore.batch.mockReturnValue(batch);
        return batch;
    }

    setupCount(collectionPath, count = 0) {
        const collectionObj = {
            ...this.mockFirestoreCollection,
            count: vi.fn().mockReturnValue({
                get: vi.fn().mockResolvedValue({ data: () => ({ count }) })
            })
        };

        this._collectionOverrides.set(collectionPath, collectionObj);
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
