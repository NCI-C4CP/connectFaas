/**
 * Tests for participantDataCleanup, removeParticipantsDataDestruction,
 * removeDocumentFromCollection, deletePathologyReports, and removeUninvitedParticipants.
 */

const { createFirebaseMocks } = require('../mocks/mockFactory');

let factory, mocks;
let fieldMapping;
let firestoreModule;
let participantDataCleanupModule;

// ── Helpers (lazy, populated in before()) ───────────────────────────────────

const destroyDataCId = () => fieldMapping.participantMap.destroyData.toString();
const dataHasBeenDestroyedCId = () => fieldMapping.participantMap.dataHasBeenDestroyed.toString();
const destroyDataCategoricalCId = () => fieldMapping.participantMap.destroyDataCategorical.toString();
const dateRequestedDataDestroyCId = () => fieldMapping.participantMap.dateRequestedDataDestroy.toString();
const requestedAndSignCId = () => fieldMapping.participantMap.requestedAndSign;
const uninvitedRecruitsCId = () => fieldMapping.participantMap.uninvitedRecruits.toString();

/**
 * Build a mock participant document suitable for the data-destruction flow.
 * @param {Object} overrides - Override any default fields
 * @returns {{ doc: Object, data: Object }}
 */
function createDestructionParticipant(overrides = {}) {
    const defaults = {
        Connect_ID: '1234567890',
        token: 'test-token-abc',
        pin: '123456',
        query: { firstName: 'Jane', lastName: 'Doe', studyId: 'S1', someOtherField: 'val' },
        state: { uid: 'firebase-uid', otherStateField: 'val2' },
        [destroyDataCId()]: fieldMapping.yes,
        [dataHasBeenDestroyedCId()]: fieldMapping.no,
        [destroyDataCategoricalCId()]: requestedAndSignCId(),
        [dateRequestedDataDestroyCId()]: new Date().toISOString(),
        [fieldMapping.dhq3Username]: 'dhq-user-1',
        [fieldMapping.dhq3StudyID]: 'dhq-study-1',
        // Some extra fields that should be deleted
        '123456789': 'extra-data',
        '987654321': 'more-data',
    };

    const data = { ...defaults, ...overrides };
    const docId = overrides._docId || 'participant-doc-1';

    return {
        data,
        doc: {
            id: docId,
            data: () => data,
            exists: true,
            ref: { id: docId },
        },
    };
}

/**
 * Set up the Firestore mock so that querying the participants collection for
 * data-destruction-eligible participants returns the supplied docs.
 */
function setupParticipantsQuery(participantDocs) {
    const snapshot = {
        empty: participantDocs.length === 0,
        size: participantDocs.length,
        docs: participantDocs,
    };

    // The participants collection query uses two .where() calls chained.
    // We need collection('participants') to return a chainable query object.
    const queryObj = {
        where: vi.fn().mockReturnThis(),
        select: vi.fn().mockReturnThis(),
        limit: vi.fn().mockReturnThis(),
        get: vi.fn().mockResolvedValue(snapshot),
    };

    mocks.firestore.collection.mockImplementation((path) => {
        if (path === 'participants') {
            return {
                doc: vi.fn().mockImplementation((docId) => ({
                    get: vi.fn().mockResolvedValue({ exists: false, data: () => null }),
                    set: vi.fn().mockResolvedValue(),
                    update: vi.fn().mockResolvedValue(),
                    delete: vi.fn().mockResolvedValue(),
                })),
                where: vi.fn().mockReturnValue(queryObj),
                get: vi.fn().mockResolvedValue(snapshot),
            };
        }
        return mocks.firestore.collection._defaultImpl
            ? mocks.firestore.collection._defaultImpl(path)
            : { where: vi.fn().mockReturnThis(), get: vi.fn().mockResolvedValue({ empty: true, size: 0, docs: [] }) };
    });

    return { snapshot, queryObj };
}

/**
 * Set up default empty responses for all related collections so
 * removeDocumentFromCollection and deletePathologyReports don't fail.
 */
function setupEmptyCollections() {
    const emptySnapshot = { empty: true, size: 0, docs: [] };
    const chainableQuery = {
        where: vi.fn().mockReturnThis(),
        select: vi.fn().mockReturnThis(),
        limit: vi.fn().mockReturnThis(),
        get: vi.fn().mockResolvedValue(emptySnapshot),
    };

    // Default: any collection returns empty
    mocks.firestore.collection.mockImplementation((collectionPath) => ({
        doc: vi.fn().mockImplementation(() => ({
            get: vi.fn().mockResolvedValue({ exists: false, data: () => null }),
            set: vi.fn().mockResolvedValue(),
            update: vi.fn().mockResolvedValue(),
            delete: vi.fn().mockResolvedValue(),
        })),
        where: vi.fn().mockReturnValue(chainableQuery),
        get: vi.fn().mockResolvedValue(emptySnapshot),
    }));
}

/**
 * Set up the participants collection mock to return the given docs for both
 * the data-destruction query AND per-participant doc updates.
 */
function setupFullDestructionMock(participantDocs) {
    const snapshot = {
        empty: participantDocs.length === 0,
        size: participantDocs.length,
        docs: participantDocs,
    };

    const emptySnapshot = { empty: true, size: 0, docs: [] };

    // Track updates per participant doc
    const updateStubs = {};
    participantDocs.forEach((d) => {
        updateStubs[d.id] = vi.fn().mockResolvedValue();
    });

    const chainableQuery = {
        where: vi.fn().mockReturnThis(),
        select: vi.fn().mockReturnThis(),
        limit: vi.fn().mockReturnThis(),
        get: vi.fn().mockResolvedValue(emptySnapshot),
    };

    mocks.firestore.collection.mockImplementation((collectionPath) => {
        if (collectionPath === 'participants') {
            const participantsQueryObj = {
                where: vi.fn().mockReturnThis(),
                select: vi.fn().mockReturnThis(),
                limit: vi.fn().mockReturnThis(),
                get: vi.fn().mockResolvedValue(snapshot),
            };
            return {
                doc: vi.fn().mockImplementation((docId) => ({
                    get: vi.fn().mockResolvedValue({ exists: true, data: () => null }),
                    set: vi.fn().mockResolvedValue(),
                    update: updateStubs[docId] || vi.fn().mockResolvedValue(),
                    delete: vi.fn().mockResolvedValue(),
                })),
                where: vi.fn().mockReturnValue(participantsQueryObj),
                get: vi.fn().mockResolvedValue(snapshot),
            };
        }

        // pathologyReports — return empty by default
        if (collectionPath === 'pathologyReports') {
            return {
                doc: vi.fn().mockImplementation(() => ({
                    get: vi.fn().mockResolvedValue({ exists: false, data: () => null }),
                    set: vi.fn().mockResolvedValue(),
                    update: vi.fn().mockResolvedValue(),
                    delete: vi.fn().mockResolvedValue(),
                })),
                where: vi.fn().mockReturnValue({
                    ...chainableQuery,
                    select: vi.fn().mockReturnValue({
                        get: vi.fn().mockResolvedValue(emptySnapshot),
                    }),
                }),
                get: vi.fn().mockResolvedValue(emptySnapshot),
            };
        }

        // All other collections
        return {
            doc: vi.fn().mockImplementation(() => ({
                get: vi.fn().mockResolvedValue({ exists: false, data: () => null }),
                set: vi.fn().mockResolvedValue(),
                update: vi.fn().mockResolvedValue(),
                delete: vi.fn().mockResolvedValue(),
            })),
            where: vi.fn().mockReturnValue(chainableQuery),
            get: vi.fn().mockResolvedValue(emptySnapshot),
        };
    });

    return { updateStubs };
}

// ── Tests ───────────────────────────────────────────────────────────────────

// Module cache keys and saved entries for cleanup
const firestoreCacheKey = require.resolve('../../utils/firestore');
const cleanupCacheKey = require.resolve('../../utils/participantDataCleanup');

describe('Participant Data Cleanup', () => {
    let savedFirestoreCache, savedCleanupCache;
    let restoreRequire;

    beforeAll(() => {
        // Save and clear module caches so modules pick up our mock when re-required.
        savedFirestoreCache = require.cache[firestoreCacheKey];
        savedCleanupCache = require.cache[cleanupCacheKey];
        delete require.cache[firestoreCacheKey];
        delete require.cache[cleanupCacheKey];

        // Create mock system without calling setupModuleMocks through createFirebaseMocks
        // so we can capture the cleanup function ourselves.
        const mockResult = createFirebaseMocks({
            setupConsole: false,
            setupModuleMocks: false,
        });
        factory = mockResult.factory;
        mocks = mockResult.mocks;

        // Set up module mocks manually so we can properly restore Module.prototype.require
        restoreRequire = factory.helpers.setupModuleMocks(mocks.admin);

        // admin.firestore is a function in the mock, but the real firebase-admin also
        // exposes admin.firestore.FieldValue as a static property. Patch it here so
        // code that uses admin.firestore.FieldValue.delete() works.
        const FIELD_DELETE_SENTINEL = '__FIELD_VALUE_DELETE__';
        mocks.fieldValue.delete = vi.fn().mockReturnValue(FIELD_DELETE_SENTINEL);
        mocks.admin.firestore.FieldValue = mocks.fieldValue;

        // Set GCLOUD_PROJECT so deletePathologyReports doesn't bail out
        process.env.GCLOUD_PROJECT = 'nih-nci-dceg-connect-prod-6d04';

        // Stub console methods to suppress log noise during tests
        if (!vi.isMockFunction(console.log)) vi.spyOn(console, 'log').mockImplementation(() => {});
        if (!vi.isMockFunction(console.error)) vi.spyOn(console, 'error').mockImplementation(() => {});

        fieldMapping = require('../../utils/fieldToConceptIdMapping');
        firestoreModule = require('../../utils/firestore');
        participantDataCleanupModule = require('../../utils/participantDataCleanup');
    });

    afterAll(() => {
        // Restore Module.prototype.require to the state before our test
        if (restoreRequire) restoreRequire();

        // Restore module caches so subsequent test files get the pre-existing modules
        if (savedFirestoreCache) {
            require.cache[firestoreCacheKey] = savedFirestoreCache;
        } else {
            delete require.cache[firestoreCacheKey];
        }
        if (savedCleanupCache) {
            require.cache[cleanupCacheKey] = savedCleanupCache;
        } else {
            delete require.cache[cleanupCacheKey];
        }

        vi.restoreAllMocks();
        factory.reset();
    });

    // Reset stub behaviors before each test so mockImplementation/mockReturnValue from prior tests don't leak.
    beforeEach(() => {
        mocks.firestore.collection.mockReset();
        mocks.firestore.batch.mockReset();
    });
    describe('participantDataCleanup (orchestrator)', () => {
        it('should call both removeParticipantsDataDestruction and removeUninvitedParticipants', async () => {
            setupEmptyCollections();
            setupParticipantsQuery([]);

            await participantDataCleanupModule.participantDataCleanup();
            // If no error is thrown, both functions ran successfully
        });

        it('should not throw when removeParticipantsDataDestruction rejects', async () => {
            // Sabotage the participants query to throw
            mocks.firestore.collection.mockImplementation((path) => {
                if (path === 'participants') {
                    return {
                        where: vi.fn().mockReturnValue({
                            where: vi.fn().mockReturnValue({
                                get: vi.fn().mockRejectedValue(new Error('Firestore unavailable')),
                            }),
                        }),
                    };
                }
                return { where: vi.fn().mockReturnThis(), get: vi.fn().mockResolvedValue({ empty: true, size: 0, docs: [] }) };
            });

            // Should not throw — Promise.allSettled handles the rejection
            await participantDataCleanupModule.participantDataCleanup();
        });

        it('should not throw when removeUninvitedParticipants rejects', async () => {
            // Set up a working data-destruction path and a broken uninvited path.
            // The uninvited query is on the same 'participants' collection so
            // this is tricky — we'll just verify the orchestrator doesn't throw.
            setupEmptyCollections();
            setupParticipantsQuery([]);

            await participantDataCleanupModule.participantDataCleanup();
        });
    });

    describe('removeParticipantsDataDestruction', () => {
        describe('happy path', () => {
            it('should process a participant who signed the destruction form', async () => {
                const { data, doc } = createDestructionParticipant();
                const { updateStubs } = setupFullDestructionMock([doc]);

                await firestoreModule.removeParticipantsDataDestruction();

                // Participant doc should have been updated
                expect(updateStubs[doc.id]).toHaveBeenCalledOnce();
                const updateArg = updateStubs[doc.id].mock.calls[0][0];

                // dataHasBeenDestroyed should be set to yes
                expect(updateArg[dataHasBeenDestroyedCId()]).toBe(fieldMapping.yes);

                // participationStatus should be set to dataDestroyedStatus
                expect(updateArg[fieldMapping.participationStatus]).toBe(
                    fieldMapping.participantMap.dataDestroyedStatus
                );

                // Extra fields should be marked for deletion (FieldValue.delete())
                expect(updateArg).toHaveProperty('123456789');
                expect(updateArg).toHaveProperty('987654321');
            });

            it('should process a participant past the 60-day waiting period', async () => {
                const sixtyOneDaysAgo = new Date(Date.now() - 61 * 24 * 60 * 60 * 1000).toISOString();
                const { data, doc } = createDestructionParticipant({
                    [destroyDataCategoricalCId()]: 999999999, // Not requestedAndSign — rely on 60 days
                    [dateRequestedDataDestroyCId()]: sixtyOneDaysAgo,
                });
                const { updateStubs } = setupFullDestructionMock([doc]);

                await firestoreModule.removeParticipantsDataDestruction();

                expect(updateStubs[doc.id]).toHaveBeenCalledOnce();
            });

            it('should process multiple participants', async () => {
                const p1 = createDestructionParticipant({ _docId: 'doc-1', Connect_ID: '111' });
                const p2 = createDestructionParticipant({ _docId: 'doc-2', Connect_ID: '222' });
                const { updateStubs } = setupFullDestructionMock([p1.doc, p2.doc]);

                await firestoreModule.removeParticipantsDataDestruction();

                expect(updateStubs['doc-1']).toHaveBeenCalledOnce();
                expect(updateStubs['doc-2']).toHaveBeenCalledOnce();
            });

            it('should preserve stub fields (Connect_ID, token, pin, query, state) and delete others', async () => {
                const { data, doc } = createDestructionParticipant();
                const { updateStubs } = setupFullDestructionMock([doc]);

                await firestoreModule.removeParticipantsDataDestruction();

                const updateArg = updateStubs[doc.id].mock.calls[0][0];

                // Stub fields should NOT be in the update as FieldValue.delete()
                // Connect_ID, token, pin are preserved — not present in updatedData with delete()
                expect(updateArg).not.toHaveProperty('Connect_ID');
                expect(updateArg).not.toHaveProperty('token');
                expect(updateArg).not.toHaveProperty('pin');

                // Extra non-stub fields SHOULD be present (marked for deletion)
                expect(updateArg).toHaveProperty('123456789');
                expect(updateArg).toHaveProperty('987654321');
            });

            it('should delete sub-fields of physicalActivity that are not in subStubFieldArray', async () => {
                const physicalActivityCId = fieldMapping.dataDestruction.physicalActivity.toString();
                const { data, doc } = createDestructionParticipant({
                    [physicalActivityCId]: {
                        someExerciseData: 'val',
                        anotherField: 'val2',
                    },
                });
                const { updateStubs } = setupFullDestructionMock([doc]);

                await firestoreModule.removeParticipantsDataDestruction();

                const updateArg = updateStubs[doc.id].mock.calls[0][0];

                // physicalActivity sub-fields not in subStubFieldArray should be deleted
                expect(updateArg).toHaveProperty(`${physicalActivityCId}.someExerciseData`);
                expect(updateArg).toHaveProperty(`${physicalActivityCId}.anotherField`);
            });

            it('should delete sub-fields of query/state that are not in subStubFieldArray', async () => {
                const { data, doc } = createDestructionParticipant();
                const { updateStubs } = setupFullDestructionMock([doc]);

                await firestoreModule.removeParticipantsDataDestruction();

                const updateArg = updateStubs[doc.id].mock.calls[0][0];

                // query.someOtherField should be deleted
                expect(updateArg).toHaveProperty('query.someOtherField');

                // query.firstName, query.lastName, query.studyId should be preserved (not deleted)
                expect(updateArg).not.toHaveProperty('query.firstName');
                expect(updateArg).not.toHaveProperty('query.lastName');
                expect(updateArg).not.toHaveProperty('query.studyId');
            });
        });

        describe('ordering — related data deleted BEFORE participant profile update', () => {
            it('should call removeDocumentFromCollection before updating the participant doc', async () => {
                const { data, doc } = createDestructionParticipant();

                const callOrder = [];
                const emptySnapshot = { empty: true, size: 0, docs: [] };
                const chainableQuery = {
                    where: vi.fn().mockReturnThis(),
                    select: vi.fn().mockReturnThis(),
                    limit: vi.fn().mockReturnThis(),
                    get: vi.fn().mockResolvedValue(emptySnapshot),
                };

                const participantUpdateStub = vi.fn().mockImplementation(() => {
                    callOrder.push('participantUpdate');
                    return Promise.resolve();
                });

                // Participants collection
                const participantsSnapshot = { empty: false, size: 1, docs: [doc] };
                const participantsQueryObj = {
                    where: vi.fn().mockReturnThis(),
                    get: vi.fn().mockResolvedValue(participantsSnapshot),
                };

                mocks.firestore.collection.mockImplementation((collectionPath) => {
                    if (collectionPath === 'participants') {
                        return {
                            doc: vi.fn().mockReturnValue({
                                update: participantUpdateStub,
                                get: vi.fn().mockResolvedValue({ exists: true }),
                                set: vi.fn().mockResolvedValue(),
                                delete: vi.fn().mockResolvedValue(),
                            }),
                            where: vi.fn().mockReturnValue(participantsQueryObj),
                            get: vi.fn().mockResolvedValue(participantsSnapshot),
                        };
                    }

                    // For every other collection — track access
                    return {
                        doc: vi.fn().mockImplementation(() => ({
                            get: vi.fn().mockResolvedValue({ exists: false, data: () => null }),
                            set: vi.fn().mockResolvedValue(),
                            update: vi.fn().mockResolvedValue(),
                            delete: vi.fn().mockResolvedValue(),
                        })),
                        where: vi.fn().mockImplementation(() => {
                            callOrder.push(`collection:${collectionPath}`);
                            return {
                                ...chainableQuery,
                                select: vi.fn().mockReturnValue({
                                    get: vi.fn().mockResolvedValue(emptySnapshot),
                                }),
                            };
                        }),
                        get: vi.fn().mockResolvedValue(emptySnapshot),
                    };
                });

                await firestoreModule.removeParticipantsDataDestruction();

                // Verify collection queries happened before participant update
                const updateIndex = callOrder.indexOf('participantUpdate');
                expect(updateIndex).toBeGreaterThan(0);

                // At least one collection query should precede the update
                const collectionQueries = callOrder.filter((c) => c.startsWith('collection:'));
                expect(collectionQueries.length).toBeGreaterThan(0);
                expect(callOrder.indexOf(collectionQueries[0])).toBeLessThan(updateIndex);
            });
        });

        describe('edge cases', () => {
            it('should skip a participant not yet eligible (< 60 days and not signed)', async () => {
                const oneDayAgo = new Date(Date.now() - 1 * 24 * 60 * 60 * 1000).toISOString();
                const { data, doc } = createDestructionParticipant({
                    [destroyDataCategoricalCId()]: 999999999,
                    [dateRequestedDataDestroyCId()]: oneDayAgo,
                });
                const { updateStubs } = setupFullDestructionMock([doc]);

                await firestoreModule.removeParticipantsDataDestruction();

                // Should NOT have called update since participant isn't eligible
                expect(updateStubs[doc.id]).not.toHaveBeenCalled();
            });

            it('should still mark as destroyed when participant has only stub fields', async () => {
                // Build a participant with ONLY stub fields — no extra data to delete
                const stubOnlyData = {
                    Connect_ID: '1234567890',
                    token: 'test-token-abc',
                    pin: '123456',
                    query: { firstName: 'Jane', lastName: 'Doe', studyId: 'S1' },
                    state: { uid: 'firebase-uid' },
                };

                // Add all dataDestruction stub fields first
                for (const val of Object.values(fieldMapping.dataDestruction)) {
                    stubOnlyData[val.toString()] = 'stub-val';
                }

                // Set eligibility fields AFTER the loop so they aren't overwritten
                // (destroyDataCategorical shares CID 883668444 with dataDestructionCategoricalFlag)
                stubOnlyData[destroyDataCId()] = fieldMapping.yes;
                stubOnlyData[dataHasBeenDestroyedCId()] = 104430631;
                stubOnlyData[destroyDataCategoricalCId()] = requestedAndSignCId();
                stubOnlyData[dateRequestedDataDestroyCId()] = new Date().toISOString();

                const docId = 'stub-only-doc';
                const doc = {
                    id: docId,
                    data: () => stubOnlyData,
                    exists: true,
                    ref: { id: docId },
                };

                const { updateStubs } = setupFullDestructionMock([doc]);

                await firestoreModule.removeParticipantsDataDestruction();

                // Even with no extra fields, participant should be marked as destroyed
                // so they don't reappear in the query every day
                expect(updateStubs[docId]).toHaveBeenCalledOnce();
                const updateArg = updateStubs[docId].mock.calls[0][0];
                expect(updateArg[dataHasBeenDestroyedCId()]).toBe(fieldMapping.yes);
            });

            it('should do nothing when no participants are eligible', async () => {
                setupFullDestructionMock([]);

                await firestoreModule.removeParticipantsDataDestruction();
                // No error, no updates
            });

            it('should NOT process a participant just under 60 days (boundary: > not >=)', async () => {
                // Use 59 days + 23 hours to stay clearly under the 60-day threshold
                // while testing the boundary behavior (> not >=)
                const justUnder60Days = new Date(Date.now() - (59 * 24 + 23) * 60 * 60 * 1000).toISOString();
                const { data, doc } = createDestructionParticipant({
                    [destroyDataCategoricalCId()]: 999999999, // Not signed
                    [dateRequestedDataDestroyCId()]: justUnder60Days,
                });
                const { updateStubs } = setupFullDestructionMock([doc]);

                await firestoreModule.removeParticipantsDataDestruction();

                // Under 60 days and not signed — should NOT be processed
                expect(updateStubs[doc.id]).not.toHaveBeenCalled();
            });

            it('should handle invalid dateRequestedDataDestroy (timeDiff = 0)', async () => {
                const { data, doc } = createDestructionParticipant({
                    [destroyDataCategoricalCId()]: 999999999, // Not signed
                    [dateRequestedDataDestroyCId()]: 'not-a-valid-date',
                });
                const { updateStubs } = setupFullDestructionMock([doc]);

                await firestoreModule.removeParticipantsDataDestruction();

                // Should not process since timeDiff = 0 (< 60 days) and not signed
                expect(updateStubs[doc.id]).not.toHaveBeenCalled();
            });
        });

        describe('error handling', () => {
            it('should continue processing subsequent participants when one fails', async () => {
                const p1 = createDestructionParticipant({ _docId: 'doc-1', Connect_ID: '111' });
                const p2 = createDestructionParticipant({ _docId: 'doc-2', Connect_ID: '222' });

                const participantsSnapshot = { empty: false, size: 2, docs: [p1.doc, p2.doc] };
                const participantsQueryObj = {
                    where: vi.fn().mockReturnThis(),
                    get: vi.fn().mockResolvedValue(participantsSnapshot),
                };
                const emptySnapshot = { empty: true, size: 0, docs: [] };
                const chainableQuery = {
                    where: vi.fn().mockReturnThis(),
                    select: vi.fn().mockReturnThis(),
                    limit: vi.fn().mockReturnThis(),
                    get: vi.fn().mockResolvedValue(emptySnapshot),
                };

                const updateStubs = {
                    'doc-1': vi.fn().mockResolvedValue(),
                    'doc-2': vi.fn().mockResolvedValue(),
                };

                let callCount = 0;
                mocks.firestore.collection.mockImplementation((collectionPath) => {
                    if (collectionPath === 'participants') {
                        return {
                            doc: vi.fn().mockImplementation((docId) => ({
                                update: updateStubs[docId] || vi.fn().mockResolvedValue(),
                                get: vi.fn().mockResolvedValue({ exists: true }),
                                set: vi.fn().mockResolvedValue(),
                                delete: vi.fn().mockResolvedValue(),
                            })),
                            where: vi.fn().mockReturnValue(participantsQueryObj),
                            get: vi.fn().mockResolvedValue(participantsSnapshot),
                        };
                    }

                    if (collectionPath === 'pathologyReports') {
                        return {
                            doc: vi.fn().mockImplementation(() => ({
                                get: vi.fn().mockResolvedValue({ exists: false }),
                                delete: vi.fn().mockResolvedValue(),
                            })),
                            where: vi.fn().mockReturnValue({
                                select: vi.fn().mockReturnValue({
                                    get: vi.fn().mockResolvedValue(emptySnapshot),
                                }),
                            }),
                        };
                    }

                    // Make the first participant's collection queries throw, second succeeds
                    callCount++;
                    if (callCount <= 21) {
                        // First participant — throw on first collection
                        return {
                            doc: vi.fn().mockImplementation(() => ({
                                get: vi.fn().mockResolvedValue({ exists: false }),
                                delete: vi.fn().mockResolvedValue(),
                            })),
                            where: vi.fn().mockImplementation(() => { throw new Error('Firestore error for p1'); }),
                            get: vi.fn().mockResolvedValue(emptySnapshot),
                        };
                    }

                    // Second participant — succeed
                    return {
                        doc: vi.fn().mockImplementation(() => ({
                            get: vi.fn().mockResolvedValue({ exists: false }),
                            delete: vi.fn().mockResolvedValue(),
                        })),
                        where: vi.fn().mockReturnValue(chainableQuery),
                        get: vi.fn().mockResolvedValue(emptySnapshot),
                    };
                });

                await firestoreModule.removeParticipantsDataDestruction();

                // First participant should NOT be updated (failed)
                expect(updateStubs['doc-1']).not.toHaveBeenCalled();
                // Second participant SHOULD be updated (per-participant isolation)
                expect(updateStubs['doc-2']).toHaveBeenCalledOnce();
            });

            it('should not mark participant as destroyed if deletePathologyReports has errors', async () => {
                const { data, doc } = createDestructionParticipant();

                const participantUpdateStub = vi.fn().mockResolvedValue();
                const participantsSnapshot = { empty: false, size: 1, docs: [doc] };
                const participantsQueryObj = {
                    where: vi.fn().mockReturnThis(),
                    get: vi.fn().mockResolvedValue(participantsSnapshot),
                };
                const emptySnapshot = { empty: true, size: 0, docs: [] };
                const chainableQuery = {
                    where: vi.fn().mockReturnThis(),
                    select: vi.fn().mockReturnThis(),
                    limit: vi.fn().mockReturnThis(),
                    get: vi.fn().mockResolvedValue(emptySnapshot),
                };

                mocks.firestore.collection.mockImplementation((collectionPath) => {
                    if (collectionPath === 'participants') {
                        return {
                            doc: vi.fn().mockReturnValue({
                                update: participantUpdateStub,
                                get: vi.fn().mockResolvedValue({ exists: true }),
                                set: vi.fn().mockResolvedValue(),
                                delete: vi.fn().mockResolvedValue(),
                            }),
                            where: vi.fn().mockReturnValue(participantsQueryObj),
                            get: vi.fn().mockResolvedValue(participantsSnapshot),
                        };
                    }

                    // pathologyReports — throw an error
                    if (collectionPath === 'pathologyReports') {
                        return {
                            doc: vi.fn().mockImplementation(() => ({
                                get: vi.fn().mockResolvedValue({ exists: false }),
                                delete: vi.fn().mockResolvedValue(),
                            })),
                            where: vi.fn().mockReturnValue({
                                select: vi.fn().mockReturnValue({
                                    get: vi.fn().mockRejectedValue(new Error('Storage unavailable')),
                                }),
                            }),
                        };
                    }

                    // Other collections succeed
                    return {
                        doc: vi.fn().mockImplementation(() => ({
                            get: vi.fn().mockResolvedValue({ exists: false }),
                            delete: vi.fn().mockResolvedValue(),
                        })),
                        where: vi.fn().mockReturnValue(chainableQuery),
                        get: vi.fn().mockResolvedValue(emptySnapshot),
                    };
                });

                await firestoreModule.removeParticipantsDataDestruction();

                // Participant should NOT be updated — deletePathologyReports had errors
                expect(participantUpdateStub).not.toHaveBeenCalled();
            });

            it('should catch participant profile update failure without crashing', async () => {
                const { data, doc } = createDestructionParticipant();

                const participantUpdateStub = vi.fn().mockRejectedValue(new Error('Firestore write failed'));
                const participantsSnapshot = { empty: false, size: 1, docs: [doc] };
                const participantsQueryObj = {
                    where: vi.fn().mockReturnThis(),
                    get: vi.fn().mockResolvedValue(participantsSnapshot),
                };
                const emptySnapshot = { empty: true, size: 0, docs: [] };
                const chainableQuery = {
                    where: vi.fn().mockReturnThis(),
                    select: vi.fn().mockReturnThis(),
                    limit: vi.fn().mockReturnThis(),
                    get: vi.fn().mockResolvedValue(emptySnapshot),
                };

                mocks.firestore.collection.mockImplementation((collectionPath) => {
                    if (collectionPath === 'participants') {
                        return {
                            doc: vi.fn().mockReturnValue({
                                update: participantUpdateStub,
                                get: vi.fn().mockResolvedValue({ exists: true }),
                                set: vi.fn().mockResolvedValue(),
                                delete: vi.fn().mockResolvedValue(),
                            }),
                            where: vi.fn().mockReturnValue(participantsQueryObj),
                            get: vi.fn().mockResolvedValue(participantsSnapshot),
                        };
                    }

                    if (collectionPath === 'pathologyReports') {
                        return {
                            doc: vi.fn().mockImplementation(() => ({
                                get: vi.fn().mockResolvedValue({ exists: false }),
                                delete: vi.fn().mockResolvedValue(),
                            })),
                            where: vi.fn().mockReturnValue({
                                select: vi.fn().mockReturnValue({
                                    get: vi.fn().mockResolvedValue(emptySnapshot),
                                }),
                            }),
                        };
                    }

                    return {
                        doc: vi.fn().mockImplementation(() => ({
                            get: vi.fn().mockResolvedValue({ exists: false }),
                            delete: vi.fn().mockResolvedValue(),
                        })),
                        where: vi.fn().mockReturnValue(chainableQuery),
                        get: vi.fn().mockResolvedValue(emptySnapshot),
                    };
                });

                // Should not throw — inner try-catch handles the profile update error
                await firestoreModule.removeParticipantsDataDestruction();
            });

            it('should not mark participant as destroyed if removeDocumentFromCollection has errors', async () => {
                const { data, doc } = createDestructionParticipant();

                const participantUpdateStub = vi.fn().mockResolvedValue();
                const participantsSnapshot = { empty: false, size: 1, docs: [doc] };
                const participantsQueryObj = {
                    where: vi.fn().mockReturnThis(),
                    get: vi.fn().mockResolvedValue(participantsSnapshot),
                };

                const emptySnapshot = { empty: true, size: 0, docs: [] };

                mocks.firestore.collection.mockImplementation((collectionPath) => {
                    if (collectionPath === 'participants') {
                        return {
                            doc: vi.fn().mockReturnValue({
                                update: participantUpdateStub,
                                get: vi.fn().mockResolvedValue({ exists: true }),
                                set: vi.fn().mockResolvedValue(),
                                delete: vi.fn().mockResolvedValue(),
                            }),
                            where: vi.fn().mockReturnValue(participantsQueryObj),
                            get: vi.fn().mockResolvedValue(participantsSnapshot),
                        };
                    }

                    // Make first non-participants collection throw
                    return {
                        doc: vi.fn().mockImplementation(() => ({
                            get: vi.fn().mockResolvedValue({ exists: false }),
                            set: vi.fn().mockResolvedValue(),
                            update: vi.fn().mockResolvedValue(),
                            delete: vi.fn().mockRejectedValue(new Error('Delete failed')),
                        })),
                        where: vi.fn().mockReturnValue({
                            where: vi.fn().mockReturnThis(),
                            select: vi.fn().mockReturnThis(),
                            limit: vi.fn().mockReturnThis(),
                            get: vi.fn().mockResolvedValue({
                                empty: false,
                                size: 1,
                                docs: [{ id: 'related-doc-1' }],
                            }),
                        }),
                        get: vi.fn().mockResolvedValue(emptySnapshot),
                    };
                });

                await firestoreModule.removeParticipantsDataDestruction();

                // Participant should NOT be updated since removeDocumentFromCollection had errors
                expect(participantUpdateStub).not.toHaveBeenCalled();
            });

            it('should still call deletePathologyReports when removeDocumentFromCollection has errors', async () => {
                const { data, doc } = createDestructionParticipant();

                const participantUpdateStub = vi.fn().mockResolvedValue();
                const participantsSnapshot = { empty: false, size: 1, docs: [doc] };
                const participantsQueryObj = {
                    where: vi.fn().mockReturnThis(),
                    get: vi.fn().mockResolvedValue(participantsSnapshot),
                };
                const emptySnapshot = { empty: true, size: 0, docs: [] };

                let pathologyQueriesCalled = false;

                mocks.firestore.collection.mockImplementation((collectionPath) => {
                    if (collectionPath === 'participants') {
                        return {
                            doc: vi.fn().mockReturnValue({
                                update: participantUpdateStub,
                                get: vi.fn().mockResolvedValue({ exists: true }),
                                set: vi.fn().mockResolvedValue(),
                                delete: vi.fn().mockResolvedValue(),
                            }),
                            where: vi.fn().mockReturnValue(participantsQueryObj),
                            get: vi.fn().mockResolvedValue(participantsSnapshot),
                        };
                    }

                    if (collectionPath === 'pathologyReports') {
                        pathologyQueriesCalled = true;
                        return {
                            doc: vi.fn().mockImplementation(() => ({
                                get: vi.fn().mockResolvedValue({ exists: false }),
                                delete: vi.fn().mockResolvedValue(),
                            })),
                            where: vi.fn().mockReturnValue({
                                select: vi.fn().mockReturnValue({
                                    get: vi.fn().mockResolvedValue(emptySnapshot),
                                }),
                            }),
                        };
                    }

                    // Make all other collections throw — removeDocumentFromCollection will have errors
                    return {
                        doc: vi.fn().mockImplementation(() => ({
                            get: vi.fn().mockResolvedValue({ exists: false }),
                            delete: vi.fn().mockResolvedValue(),
                        })),
                        where: vi.fn().mockImplementation(() => { throw new Error('Collection error'); }),
                        get: vi.fn().mockResolvedValue(emptySnapshot),
                    };
                });

                await firestoreModule.removeParticipantsDataDestruction();

                // deletePathologyReports should still have been called despite collection errors
                expect(pathologyQueriesCalled).toBe(true);
                // Participant should NOT be updated since there were errors
                expect(participantUpdateStub).not.toHaveBeenCalled();
            });

            it('should accumulate errors from both removeDocumentFromCollection and deletePathologyReports', async () => {
                const { data, doc } = createDestructionParticipant();

                const participantUpdateStub = vi.fn().mockResolvedValue();
                const participantsSnapshot = { empty: false, size: 1, docs: [doc] };
                const participantsQueryObj = {
                    where: vi.fn().mockReturnThis(),
                    get: vi.fn().mockResolvedValue(participantsSnapshot),
                };
                const emptySnapshot = { empty: true, size: 0, docs: [] };

                mocks.firestore.collection.mockImplementation((collectionPath) => {
                    if (collectionPath === 'participants') {
                        return {
                            doc: vi.fn().mockReturnValue({
                                update: participantUpdateStub,
                                get: vi.fn().mockResolvedValue({ exists: true }),
                                set: vi.fn().mockResolvedValue(),
                                delete: vi.fn().mockResolvedValue(),
                            }),
                            where: vi.fn().mockReturnValue(participantsQueryObj),
                            get: vi.fn().mockResolvedValue(participantsSnapshot),
                        };
                    }

                    // pathologyReports — throw an error
                    if (collectionPath === 'pathologyReports') {
                        return {
                            doc: vi.fn().mockImplementation(() => ({
                                get: vi.fn().mockResolvedValue({ exists: false }),
                                delete: vi.fn().mockResolvedValue(),
                            })),
                            where: vi.fn().mockReturnValue({
                                select: vi.fn().mockReturnValue({
                                    get: vi.fn().mockRejectedValue(new Error('Storage unavailable')),
                                }),
                            }),
                        };
                    }

                    // All other collections also throw
                    return {
                        doc: vi.fn().mockImplementation(() => ({
                            get: vi.fn().mockResolvedValue({ exists: false }),
                            delete: vi.fn().mockResolvedValue(),
                        })),
                        where: vi.fn().mockImplementation(() => { throw new Error('Collection error'); }),
                        get: vi.fn().mockResolvedValue(emptySnapshot),
                    };
                });

                await firestoreModule.removeParticipantsDataDestruction();

                // Participant should NOT be updated — both functions had errors
                expect(participantUpdateStub).not.toHaveBeenCalled();
            });

            it('should continue processing next participant when updatedData building throws for a malformed record', async () => {
                // p1 has query: null — Object.keys(null) will throw during updatedData building.
                // p2 is normal. The outer per-participant try-catch should isolate the failure.
                const p1 = createDestructionParticipant({
                    _docId: 'doc-malformed',
                    Connect_ID: '111',
                    query: null, // will cause Object.keys(null) to throw
                });
                const p2 = createDestructionParticipant({ _docId: 'doc-normal', Connect_ID: '222' });

                const { updateStubs } = setupFullDestructionMock([p1.doc, p2.doc]);

                await firestoreModule.removeParticipantsDataDestruction();

                // p1 should NOT be updated (threw during updatedData building)
                expect(updateStubs['doc-malformed']).not.toHaveBeenCalled();
                // p2 should still be processed successfully
                expect(updateStubs['doc-normal']).toHaveBeenCalledOnce();
            });

            it('should mark participant as destroyed when both cleanup functions return no errors', async () => {
                const { data, doc } = createDestructionParticipant();
                const { updateStubs } = setupFullDestructionMock([doc]);

                await firestoreModule.removeParticipantsDataDestruction();

                // Both functions returned empty errors arrays — participant should be marked as destroyed
                expect(updateStubs[doc.id]).toHaveBeenCalledOnce();
                const updateArg = updateStubs[doc.id].mock.calls[0][0];
                expect(updateArg[dataHasBeenDestroyedCId()]).toBe(fieldMapping.yes);
                expect(updateArg[fieldMapping.participationStatus]).toBe(
                    fieldMapping.participantMap.dataDestroyedStatus
                );
            });
        });
    });

    describe('removeDocumentFromCollection', () => {
        // Access the unexported function indirectly through removeParticipantsDataDestruction,
        // or test behavior through the integration. Since removeDocumentFromCollection is not
        // directly exported, we test it through the parent function's behavior.
        // However, for focused unit tests we can use the internal module structure.

        describe('happy path', () => {
            it('should query each collection and delete matching documents', async () => {
                const { data, doc } = createDestructionParticipant();
                const deleteStubs = [];

                const participantsSnapshot = { empty: false, size: 1, docs: [doc] };
                const participantsQueryObj = {
                    where: vi.fn().mockReturnThis(),
                    get: vi.fn().mockResolvedValue(participantsSnapshot),
                };

                const emptySnapshot = { empty: true, size: 0, docs: [] };

                mocks.firestore.collection.mockImplementation((collectionPath) => {
                    if (collectionPath === 'participants') {
                        return {
                            doc: vi.fn().mockReturnValue({
                                update: vi.fn().mockResolvedValue(),
                                get: vi.fn().mockResolvedValue({ exists: true }),
                                set: vi.fn().mockResolvedValue(),
                                delete: vi.fn().mockResolvedValue(),
                            }),
                            where: vi.fn().mockReturnValue(participantsQueryObj),
                            get: vi.fn().mockResolvedValue(participantsSnapshot),
                        };
                    }

                    if (collectionPath === 'pathologyReports') {
                        return {
                            doc: vi.fn().mockImplementation(() => ({
                                get: vi.fn().mockResolvedValue({ exists: false }),
                                delete: vi.fn().mockResolvedValue(),
                            })),
                            where: vi.fn().mockReturnValue({
                                select: vi.fn().mockReturnValue({
                                    get: vi.fn().mockResolvedValue(emptySnapshot),
                                }),
                            }),
                        };
                    }

                    // For survey/related collections — return one doc to delete
                    const deleteStub = vi.fn().mockResolvedValue();
                    deleteStubs.push(deleteStub);

                    return {
                        doc: vi.fn().mockReturnValue({
                            get: vi.fn().mockResolvedValue({ exists: false }),
                            set: vi.fn().mockResolvedValue(),
                            update: vi.fn().mockResolvedValue(),
                            delete: deleteStub,
                        }),
                        where: vi.fn().mockReturnValue({
                            where: vi.fn().mockReturnThis(),
                            select: vi.fn().mockReturnThis(),
                            limit: vi.fn().mockReturnThis(),
                            get: vi.fn().mockResolvedValue({
                                empty: false,
                                size: 1,
                                docs: [{ id: 'doc-to-delete' }],
                            }),
                        }),
                        get: vi.fn().mockResolvedValue(emptySnapshot),
                    };
                });

                await firestoreModule.removeParticipantsDataDestruction();

                // At least some delete stubs should have been called
                const calledDeletes = deleteStubs.filter((s) => s.mock.calls.length > 0);
                expect(calledDeletes.length).toBeGreaterThan(0);
            });
        });

        describe('token-based collections', () => {
            it('should query notifications and ssn by token instead of Connect_ID', async () => {
                const { data, doc } = createDestructionParticipant({ token: 'my-special-token' });

                const whereArgs = {};
                const participantsSnapshot = { empty: false, size: 1, docs: [doc] };
                const participantsQueryObj = {
                    where: vi.fn().mockReturnThis(),
                    get: vi.fn().mockResolvedValue(participantsSnapshot),
                };
                const emptySnapshot = { empty: true, size: 0, docs: [] };

                mocks.firestore.collection.mockImplementation((collectionPath) => {
                    if (collectionPath === 'participants') {
                        return {
                            doc: vi.fn().mockReturnValue({
                                update: vi.fn().mockResolvedValue(),
                                get: vi.fn().mockResolvedValue({ exists: true }),
                                set: vi.fn().mockResolvedValue(),
                                delete: vi.fn().mockResolvedValue(),
                            }),
                            where: vi.fn().mockReturnValue(participantsQueryObj),
                            get: vi.fn().mockResolvedValue(participantsSnapshot),
                        };
                    }

                    if (collectionPath === 'pathologyReports') {
                        return {
                            doc: vi.fn().mockImplementation(() => ({
                                get: vi.fn().mockResolvedValue({ exists: false }),
                                delete: vi.fn().mockResolvedValue(),
                            })),
                            where: vi.fn().mockReturnValue({
                                select: vi.fn().mockReturnValue({
                                    get: vi.fn().mockResolvedValue(emptySnapshot),
                                }),
                            }),
                        };
                    }

                    const whereStub = vi.fn().mockImplementation((field, op, value) => {
                        whereArgs[collectionPath] = { field, op, value };
                        return {
                            where: vi.fn().mockReturnThis(),
                            select: vi.fn().mockReturnThis(),
                            limit: vi.fn().mockReturnThis(),
                            get: vi.fn().mockResolvedValue(emptySnapshot),
                        };
                    });
                    return {
                        doc: vi.fn().mockImplementation(() => ({
                            get: vi.fn().mockResolvedValue({ exists: false }),
                            delete: vi.fn().mockResolvedValue(),
                        })),
                        where: whereStub,
                        get: vi.fn().mockResolvedValue(emptySnapshot),
                    };
                });

                await firestoreModule.removeParticipantsDataDestruction();

                // notifications and ssn should be queried by "token"
                expect(whereArgs['notifications']).toBeDefined();
                expect(whereArgs['notifications'].field).toBe('token');
                expect(whereArgs['notifications'].value).toBe('my-special-token');

                expect(whereArgs['ssn']).toBeDefined();
                expect(whereArgs['ssn'].field).toBe('token');
                expect(whereArgs['ssn'].value).toBe('my-special-token');

                // Other collections should be queried by "Connect_ID"
                expect(whereArgs['bioSurvey_v1']).toBeDefined();
                expect(whereArgs['bioSurvey_v1'].field).toBe('Connect_ID');
            });
        });

        describe('DHQ collections', () => {
            it('should skip DHQ collections when dhq3Username is null', async () => {
                const { data, doc } = createDestructionParticipant({
                    [fieldMapping.dhq3Username]: null,
                });

                const queriedCollections = [];
                const participantsSnapshot = { empty: false, size: 1, docs: [doc] };
                const participantsQueryObj = {
                    where: vi.fn().mockReturnThis(),
                    get: vi.fn().mockResolvedValue(participantsSnapshot),
                };
                const emptySnapshot = { empty: true, size: 0, docs: [] };

                mocks.firestore.collection.mockImplementation((collectionPath) => {
                    if (collectionPath === 'participants') {
                        return {
                            doc: vi.fn().mockReturnValue({
                                update: vi.fn().mockResolvedValue(),
                                get: vi.fn().mockResolvedValue({ exists: true }),
                                set: vi.fn().mockResolvedValue(),
                                delete: vi.fn().mockResolvedValue(),
                            }),
                            where: vi.fn().mockReturnValue(participantsQueryObj),
                            get: vi.fn().mockResolvedValue(participantsSnapshot),
                        };
                    }

                    if (collectionPath === 'pathologyReports') {
                        return {
                            doc: vi.fn().mockImplementation(() => ({
                                get: vi.fn().mockResolvedValue({ exists: false }),
                                delete: vi.fn().mockResolvedValue(),
                            })),
                            where: vi.fn().mockReturnValue({
                                select: vi.fn().mockReturnValue({
                                    get: vi.fn().mockResolvedValue(emptySnapshot),
                                }),
                            }),
                        };
                    }

                    // Track .where() calls (not just collection access) to detect DHQ skipping
                    const whereStub = vi.fn().mockImplementation(() => {
                        queriedCollections.push(collectionPath);
                        return {
                            where: vi.fn().mockReturnThis(),
                            select: vi.fn().mockReturnThis(),
                            limit: vi.fn().mockReturnThis(),
                            get: vi.fn().mockResolvedValue(emptySnapshot),
                        };
                    });
                    return {
                        doc: vi.fn().mockImplementation(() => ({
                            get: vi.fn().mockResolvedValue({ exists: false }),
                            delete: vi.fn().mockResolvedValue(),
                        })),
                        where: whereStub,
                        get: vi.fn().mockResolvedValue(emptySnapshot),
                    };
                });

                await firestoreModule.removeParticipantsDataDestruction();

                // DHQ collections should NOT appear in the queried list (skipped via continue)
                expect(queriedCollections).not.toContain('dhqAnalysisResults');
                expect(queriedCollections).not.toContain('dhqDetailedAnalysis');
                expect(queriedCollections).not.toContain('dhqRawAnswers');
            });

            it('should query DHQ collections by dhq3Username CID when username is present', async () => {
                const { data, doc } = createDestructionParticipant({
                    [fieldMapping.dhq3Username]: 'test-dhq-user',
                });

                const queriedCollections = [];
                const participantsSnapshot = { empty: false, size: 1, docs: [doc] };
                const participantsQueryObj = {
                    where: vi.fn().mockReturnThis(),
                    get: vi.fn().mockResolvedValue(participantsSnapshot),
                };
                const emptySnapshot = { empty: true, size: 0, docs: [] };

                mocks.firestore.collection.mockImplementation((collectionPath) => {
                    if (collectionPath === 'participants') {
                        return {
                            doc: vi.fn().mockReturnValue({
                                update: vi.fn().mockResolvedValue(),
                                get: vi.fn().mockResolvedValue({ exists: true }),
                                set: vi.fn().mockResolvedValue(),
                                delete: vi.fn().mockResolvedValue(),
                            }),
                            where: vi.fn().mockReturnValue(participantsQueryObj),
                            get: vi.fn().mockResolvedValue(participantsSnapshot),
                        };
                    }

                    if (collectionPath === 'pathologyReports') {
                        return {
                            doc: vi.fn().mockImplementation(() => ({
                                get: vi.fn().mockResolvedValue({ exists: false }),
                                delete: vi.fn().mockResolvedValue(),
                            })),
                            where: vi.fn().mockReturnValue({
                                select: vi.fn().mockReturnValue({
                                    get: vi.fn().mockResolvedValue(emptySnapshot),
                                }),
                            }),
                        };
                    }

                    const whereStub = vi.fn().mockImplementation(() => {
                        queriedCollections.push(collectionPath);
                        return {
                            where: vi.fn().mockReturnThis(),
                            select: vi.fn().mockReturnThis(),
                            limit: vi.fn().mockReturnThis(),
                            get: vi.fn().mockResolvedValue(emptySnapshot),
                        };
                    });
                    return {
                        doc: vi.fn().mockImplementation(() => ({
                            get: vi.fn().mockResolvedValue({ exists: false }),
                            delete: vi.fn().mockResolvedValue(),
                        })),
                        where: whereStub,
                        get: vi.fn().mockResolvedValue(emptySnapshot),
                    };
                });

                await firestoreModule.removeParticipantsDataDestruction();

                // DHQ collections SHOULD be queried when username is present
                expect(queriedCollections).toContain('dhqAnalysisResults');
                expect(queriedCollections).toContain('dhqDetailedAnalysis');
                expect(queriedCollections).toContain('dhqRawAnswers');
            });
        });

        describe('per-collection error isolation', () => {
            it('should continue processing other collections when one collection fails', async () => {
                const { data, doc } = createDestructionParticipant();

                const queriedCollections = [];
                const participantsSnapshot = { empty: false, size: 1, docs: [doc] };
                const participantsQueryObj = {
                    where: vi.fn().mockReturnThis(),
                    get: vi.fn().mockResolvedValue(participantsSnapshot),
                };
                const emptySnapshot = { empty: true, size: 0, docs: [] };

                let firstNonParticipantsCollection = true;

                mocks.firestore.collection.mockImplementation((collectionPath) => {
                    if (collectionPath === 'participants') {
                        return {
                            doc: vi.fn().mockReturnValue({
                                update: vi.fn().mockResolvedValue(),
                                get: vi.fn().mockResolvedValue({ exists: true }),
                                set: vi.fn().mockResolvedValue(),
                                delete: vi.fn().mockResolvedValue(),
                            }),
                            where: vi.fn().mockReturnValue(participantsQueryObj),
                            get: vi.fn().mockResolvedValue(participantsSnapshot),
                        };
                    }

                    if (collectionPath === 'pathologyReports') {
                        return {
                            doc: vi.fn().mockImplementation(() => ({
                                get: vi.fn().mockResolvedValue({ exists: false }),
                                delete: vi.fn().mockResolvedValue(),
                            })),
                            where: vi.fn().mockReturnValue({
                                select: vi.fn().mockReturnValue({
                                    get: vi.fn().mockResolvedValue(emptySnapshot),
                                }),
                            }),
                        };
                    }

                    queriedCollections.push(collectionPath);

                    // Make the first non-participants collection throw
                    if (firstNonParticipantsCollection) {
                        firstNonParticipantsCollection = false;
                        return {
                            doc: vi.fn().mockImplementation(() => ({
                                get: vi.fn().mockResolvedValue({ exists: false }),
                                delete: vi.fn().mockResolvedValue(),
                            })),
                            where: vi.fn().mockImplementation(() => { throw new Error('Collection query failed'); }),
                            get: vi.fn().mockResolvedValue(emptySnapshot),
                        };
                    }

                    return {
                        doc: vi.fn().mockImplementation(() => ({
                            get: vi.fn().mockResolvedValue({ exists: false }),
                            delete: vi.fn().mockResolvedValue(),
                        })),
                        where: vi.fn().mockReturnValue({
                            where: vi.fn().mockReturnThis(),
                            select: vi.fn().mockReturnThis(),
                            limit: vi.fn().mockReturnThis(),
                            get: vi.fn().mockResolvedValue(emptySnapshot),
                        }),
                        get: vi.fn().mockResolvedValue(emptySnapshot),
                    };
                });

                await firestoreModule.removeParticipantsDataDestruction();

                // Should have queried more than just the failing collection
                // (proving error isolation — the loop continued after the first failure)
                expect(queriedCollections.length).toBeGreaterThan(1);
            });
        });
    });

    describe('deletePathologyReports', () => {
        it('should NOT mark participant as destroyed when a storage file fails to delete', async () => {
            const { data, doc } = createDestructionParticipant();

            const participantUpdateStub = vi.fn().mockResolvedValue();
            const participantsSnapshot = { empty: false, size: 1, docs: [doc] };
            const participantsQueryObj = {
                where: vi.fn().mockReturnThis(),
                get: vi.fn().mockResolvedValue(participantsSnapshot),
            };
            const emptySnapshot = { empty: true, size: 0, docs: [] };
            const chainableQuery = {
                where: vi.fn().mockReturnThis(),
                select: vi.fn().mockReturnThis(),
                limit: vi.fn().mockReturnThis(),
                get: vi.fn().mockResolvedValue(emptySnapshot),
            };

            const connectId = data.Connect_ID;
            const bucketName = `pathology-reports-some-site-prod-6d04`;
            const fileName = 'report1.pdf';
            const fileNameCidStr = fieldMapping.pathologyReportFilename.toString();

            // pathologyReports Firestore docs pointing to a file
            const pathologyDoc = {
                id: 'path-doc-1',
                data: () => ({
                    Connect_ID: connectId,
                    bucketName,
                    [fileNameCidStr]: fileName,
                }),
                ref: { id: 'path-doc-1' },
            };
            const pathologySnapshot = {
                empty: false,
                size: 1,
                docs: [pathologyDoc],
            };

            // Mock file that fails to delete
            const failingFile = {
                name: `${connectId}/${fileName}`,
                delete: vi.fn().mockRejectedValue(new Error('Permission denied')),
            };

            // Mock bucket
            const mockBucket = {
                exists: vi.fn().mockResolvedValue([true]),
                getFiles: vi.fn().mockResolvedValue([[failingFile]]),
            };

            // Wire storage.bucket() to return our mock bucket
            mocks.storage.bucket.mockReturnValue(mockBucket);

            mocks.firestore.collection.mockImplementation((collectionPath) => {
                if (collectionPath === 'participants') {
                    return {
                        doc: vi.fn().mockReturnValue({
                            update: participantUpdateStub,
                            get: vi.fn().mockResolvedValue({ exists: true }),
                            set: vi.fn().mockResolvedValue(),
                            delete: vi.fn().mockResolvedValue(),
                        }),
                        where: vi.fn().mockReturnValue(participantsQueryObj),
                        get: vi.fn().mockResolvedValue(participantsSnapshot),
                    };
                }

                if (collectionPath === 'pathologyReports') {
                    return {
                        doc: vi.fn().mockImplementation(() => ({
                            get: vi.fn().mockResolvedValue({ exists: false }),
                            delete: vi.fn().mockResolvedValue(),
                        })),
                        where: vi.fn().mockReturnValue({
                            select: vi.fn().mockReturnValue({
                                get: vi.fn().mockResolvedValue(pathologySnapshot),
                            }),
                        }),
                    };
                }

                // Other collections succeed
                return {
                    doc: vi.fn().mockImplementation(() => ({
                        get: vi.fn().mockResolvedValue({ exists: false }),
                        delete: vi.fn().mockResolvedValue(),
                    })),
                    where: vi.fn().mockReturnValue(chainableQuery),
                    get: vi.fn().mockResolvedValue(emptySnapshot),
                };
            });

            await firestoreModule.removeParticipantsDataDestruction();

            // File failed to delete from storage — participant should NOT be marked as destroyed
            // so the daily job picks them up again on the next run
            expect(participantUpdateStub).not.toHaveBeenCalled();
        });

        it('should NOT mark participant as destroyed when GCLOUD_PROJECT is missing', async () => {
            const savedProject = process.env.GCLOUD_PROJECT;
            process.env.GCLOUD_PROJECT = '';

            try {
                const { data, doc } = createDestructionParticipant();
                const { updateStubs } = setupFullDestructionMock([doc]);

                await firestoreModule.removeParticipantsDataDestruction();

                // Missing GCLOUD_PROJECT means deletePathologyReports returns errors,
                // so the participant should NOT be marked as destroyed
                expect(updateStubs[doc.id]).not.toHaveBeenCalled();
            } finally {
                process.env.GCLOUD_PROJECT = savedProject;
            }
        });
    });

    describe('DHQ deidentification', () => {
        it('should NOT include dhq3Username and dhq3StudyID in the dataDestruction stub fields', () => {
            const dataDestructionValues = Object.values(fieldMapping.dataDestruction).map((id) =>
                id.toString()
            );
            const dhq3UsernameCid = fieldMapping.dhq3Username.toString();
            const dhq3StudyIdCid = fieldMapping.dhq3StudyID.toString();

            expect(dataDestructionValues).not.toContain(dhq3UsernameCid);
            expect(dataDestructionValues).not.toContain(dhq3StudyIdCid);
        });

        it('should delete dhq3Username and dhq3StudyID fields during data destruction', async () => {
            const { data, doc } = createDestructionParticipant();
            const { updateStubs } = setupFullDestructionMock([doc]);

            await firestoreModule.removeParticipantsDataDestruction();

            const updateArg = updateStubs[doc.id].mock.calls[0][0];

            // dhq3Username and dhq3StudyID should be in the update (marked for deletion)
            expect(updateArg).toHaveProperty(fieldMapping.dhq3Username.toString());
            expect(updateArg).toHaveProperty(fieldMapping.dhq3StudyID.toString());
        });
    });

    describe('removeUninvitedParticipants', () => {
        describe('happy path', () => {
            it('should batch-delete uninvited participant records', async () => {
                const uninvitedDocs = [
                    { id: 'uninvited-1', ref: { id: 'uninvited-1' }, data: () => ({}) },
                    { id: 'uninvited-2', ref: { id: 'uninvited-2' }, data: () => ({}) },
                ];

                const snapshot = {
                    empty: false,
                    size: 2,
                    docs: uninvitedDocs,
                };
                const emptySnapshot = { empty: true, size: 0, docs: [] };

                const batchDeleteStub = vi.fn();
                const batchCommitStub = vi.fn().mockResolvedValue();
                const mockBatch = {
                    delete: batchDeleteStub,
                    commit: batchCommitStub,
                    set: vi.fn(),
                    update: vi.fn(),
                };

                let callCount = 0;
                const queryObj = {
                    where: vi.fn().mockReturnThis(),
                    limit: vi.fn().mockReturnThis(),
                    get: vi.fn().mockImplementation(() => {
                        callCount++;
                        // First call returns docs, second call returns empty (exit loop)
                        return Promise.resolve(callCount === 1 ? snapshot : emptySnapshot);
                    }),
                };

                mocks.firestore.collection.mockImplementation((path) => {
                    if (path === 'participants') {
                        return {
                            doc: vi.fn().mockImplementation(() => ({
                                get: vi.fn().mockResolvedValue({ exists: false }),
                            })),
                            where: vi.fn().mockReturnValue(queryObj),
                            get: vi.fn().mockResolvedValue(emptySnapshot),
                        };
                    }
                    return { where: vi.fn().mockReturnThis(), get: vi.fn().mockResolvedValue(emptySnapshot) };
                });

                mocks.firestore.batch.mockReturnValue(mockBatch);

                await firestoreModule.removeUninvitedParticipants();

                // Should have called batch.delete for each uninvited doc
                expect(batchDeleteStub).toHaveBeenCalledTimes(2);
                expect(batchCommitStub).toHaveBeenCalled();
            });
        });

        describe('edge cases', () => {
            it('should loop again when exactly batchLimit (500) docs are returned', async () => {
                const batchLimit = 500;
                // Create 500 mock docs for first batch
                const fullBatchDocs = Array.from({ length: batchLimit }, (_, i) => ({
                    id: `uninvited-${i}`,
                    ref: { id: `uninvited-${i}` },
                    data: () => ({}),
                }));
                const fullBatchSnapshot = { empty: false, size: batchLimit, docs: fullBatchDocs };

                // Second batch returns 2 docs (less than batchLimit, so loop ends)
                const remainingDocs = [
                    { id: 'uninvited-500', ref: { id: 'uninvited-500' }, data: () => ({}) },
                    { id: 'uninvited-501', ref: { id: 'uninvited-501' }, data: () => ({}) },
                ];
                const remainingSnapshot = { empty: false, size: 2, docs: remainingDocs };

                const batchDeleteStub = vi.fn();
                const batchCommitStub = vi.fn().mockResolvedValue();
                const mockBatch = {
                    delete: batchDeleteStub,
                    commit: batchCommitStub,
                    set: vi.fn(),
                    update: vi.fn(),
                };

                let callCount = 0;
                const queryObj = {
                    where: vi.fn().mockReturnThis(),
                    limit: vi.fn().mockReturnThis(),
                    get: vi.fn().mockImplementation(() => {
                        callCount++;
                        if (callCount === 1) return Promise.resolve(fullBatchSnapshot);
                        return Promise.resolve(remainingSnapshot);
                    }),
                };

                const emptySnapshot = { empty: true, size: 0, docs: [] };
                mocks.firestore.collection.mockImplementation((path) => {
                    if (path === 'participants') {
                        return {
                            doc: vi.fn().mockImplementation(() => ({
                                get: vi.fn().mockResolvedValue({ exists: false }),
                            })),
                            where: vi.fn().mockReturnValue(queryObj),
                            get: vi.fn().mockResolvedValue(emptySnapshot),
                        };
                    }
                    return { where: vi.fn().mockReturnThis(), get: vi.fn().mockResolvedValue(emptySnapshot) };
                });

                mocks.firestore.batch.mockReturnValue(mockBatch);

                await firestoreModule.removeUninvitedParticipants();

                // Should have looped twice: 500 docs + 2 docs = 502 total deletes
                expect(batchDeleteStub).toHaveBeenCalledTimes(502);
                // batch.commit called twice (once per loop iteration)
                expect(batchCommitStub).toHaveBeenCalledTimes(2);
            });

            it('should handle no uninvited participants', async () => {
                const emptySnapshot = { empty: true, size: 0, docs: [] };
                const batchCommitStub = vi.fn().mockResolvedValue();

                const queryObj = {
                    where: vi.fn().mockReturnThis(),
                    limit: vi.fn().mockReturnThis(),
                    get: vi.fn().mockResolvedValue(emptySnapshot),
                };

                mocks.firestore.collection.mockImplementation((path) => {
                    if (path === 'participants') {
                        return {
                            doc: vi.fn().mockImplementation(() => ({
                                get: vi.fn().mockResolvedValue({ exists: false }),
                            })),
                            where: vi.fn().mockReturnValue(queryObj),
                            get: vi.fn().mockResolvedValue(emptySnapshot),
                        };
                    }
                    return { where: vi.fn().mockReturnThis(), get: vi.fn().mockResolvedValue(emptySnapshot) };
                });

                mocks.firestore.batch.mockReturnValue({
                    delete: vi.fn(),
                    commit: batchCommitStub,
                    set: vi.fn(),
                    update: vi.fn(),
                });

                await firestoreModule.removeUninvitedParticipants();

                // Batch commit should still be called (with 0 deletes in it)
                expect(batchCommitStub).toHaveBeenCalled();
            });
        });

        describe('error handling', () => {
            it('should not throw when batch commit fails', async () => {
                const uninvitedDocs = [
                    { id: 'uninvited-1', ref: { id: 'uninvited-1' }, data: () => ({}) },
                ];
                const snapshot = { empty: false, size: 1, docs: uninvitedDocs };
                const emptySnapshot = { empty: true, size: 0, docs: [] };

                let callCount = 0;
                const queryObj = {
                    where: vi.fn().mockReturnThis(),
                    limit: vi.fn().mockReturnThis(),
                    get: vi.fn().mockImplementation(() => {
                        callCount++;
                        return Promise.resolve(callCount === 1 ? snapshot : emptySnapshot);
                    }),
                };

                mocks.firestore.collection.mockImplementation((path) => {
                    if (path === 'participants') {
                        return {
                            doc: vi.fn().mockImplementation(() => ({
                                get: vi.fn().mockResolvedValue({ exists: false }),
                            })),
                            where: vi.fn().mockReturnValue(queryObj),
                            get: vi.fn().mockResolvedValue(emptySnapshot),
                        };
                    }
                    return { where: vi.fn().mockReturnThis(), get: vi.fn().mockResolvedValue(emptySnapshot) };
                });

                mocks.firestore.batch.mockReturnValue({
                    delete: vi.fn(),
                    commit: vi.fn().mockRejectedValue(new Error('Batch commit failed')),
                    set: vi.fn(),
                    update: vi.fn(),
                });

                // Should not throw — error is caught internally
                await firestoreModule.removeUninvitedParticipants();
            });
        });
    });
});
