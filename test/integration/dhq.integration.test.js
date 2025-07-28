const { expect } = require('chai');
const sinon = require('sinon');
const { setupTestSuite, assertResult } = require('../shared/testHelpers');
const TestUtils = require('../testUtils');
const TEST_CONSTANTS = require('../constants');

// Set up test environment, mocks, and cleanup
const { factory, mocks } = setupTestSuite({
    setupConsole: false,
    setupModuleMocks: true
});

const fieldMapping = require('../../utils/fieldToConceptIdMapping');

const { 
    createResponseDocID, 
    getDynamicChunkSize, 
    prepareDocumentsForFirestore,
    getProcessedRespondentIds
} = require('../../utils/dhq');

describe('DHQ Integration Tests', () => {

    // INTEGRATION TESTING
    describe('DHQ Integration Testing', () => {
        it('should demonstrate end-to-end CSV processing', async () => {
            
            // Set up mock data for processing and collection data for tracking.
            factory.setupCollectionData(`dhq3SurveyCredentials/${TEST_CONSTANTS.STUDY_IDS.DEFAULT}/responseTracking`, [
                {
                    id: TEST_CONSTANTS.DOCS.DHQ_ANALYSIS_RESULTS,
                    [fieldMapping.dhq3StudyID]: TEST_CONSTANTS.STUDY_IDS.DEFAULT,
                    [fieldMapping.dhq3ProcessedRespondentArray]: [TEST_CONSTANTS.PARTICIPANT_IDS.DEFAULT, TEST_CONSTANTS.PARTICIPANT_IDS.SECOND]
                }
            ]);

            // Set up count for available credentials.
            factory.setupCount(`dhq3SurveyCredentials/${TEST_CONSTANTS.STUDY_IDS.DEFAULT}/availableCredentials`, 500);

            // Test the processing function.
            const testData = [
                { 'Respondent ID': TEST_CONSTANTS.PARTICIPANT_IDS.DEFAULT, 'Energy': '2000', 'Protein': '50' },
                { 'Respondent ID': TEST_CONSTANTS.PARTICIPANT_IDS.SECOND, 'Energy': '1800', 'Protein': '45' },
                { 'Respondent ID': TEST_CONSTANTS.PARTICIPANT_IDS.THIRD, 'Energy': '2200', 'Protein': '60' }
            ];
            
            const result = prepareDocumentsForFirestore(testData, TEST_CONSTANTS.STUDY_IDS.DEFAULT, TEST_CONSTANTS.DOCS.ANALYSIS_RESULTS);
            assertResult(result, {
                documentCount: 3,
                expectedIds: [TEST_CONSTANTS.PARTICIPANT_IDS.DEFAULT, TEST_CONSTANTS.PARTICIPANT_IDS.SECOND, TEST_CONSTANTS.PARTICIPANT_IDS.THIRD]
            });
        });

        it('should query for participants', async () => {
            // Set up participants with different statuses
            const participants = [
                TestUtils.createMockParticipantData().createNotStartedDHQParticipant(TEST_CONSTANTS.PARTICIPANT_IDS.DEFAULT),
                TestUtils.createMockParticipantData().createStartedDHQParticipant(TEST_CONSTANTS.PARTICIPANT_IDS.SECOND),
                TestUtils.createMockParticipantData().createCompletedDHQParticipant(TEST_CONSTANTS.PARTICIPANT_IDS.THIRD)
            ];

            // Filter to only started participants
            const startedParticipants = participants.filter(p => p[fieldMapping.dhq3SurveyStatus] === fieldMapping.started);

            const mockQuerySnapshot = {
                empty: false,
                size: startedParticipants.length,
                docs: startedParticipants.map(p => ({
                    id: p.state.uid,
                    data: () => p,
                    exists: true,
                    ref: { id: p.state.uid }
                })),
                forEach: (callback) => startedParticipants.forEach(callback)
            };

            // Set up the mock to return the filtered results
            const collectionRef = mocks.firestore.collection('participants');
            collectionRef.where.returns({
                select: sinon.stub().returnsThis(),
                limit: sinon.stub().returnsThis(),
                get: sinon.stub().resolves(mockQuerySnapshot)
            });

            // Test querying for started participants
            const startedQuery = mocks.firestore.collection('participants')
                .where(fieldMapping.dhq3SurveyStatus, '==', fieldMapping.started)
                .select('state', fieldMapping.dhq3StudyID, fieldMapping.dhq3participantname)
                .get();

            const result = await startedQuery;
            expect(result.size).to.equal(1);
        });

        it('should execute a transaction', async () => {
            // Set up initial participant data
            const participantData = TestUtils.createMockParticipantData().createNotStartedDHQParticipant(TEST_CONSTANTS.PARTICIPANT_IDS.DEFAULT);
            factory.setupDocumentRetrieval(TEST_CONSTANTS.COLLECTIONS.PARTICIPANTS, TEST_CONSTANTS.PARTICIPANT_IDS.DEFAULT, participantData);

            // Set up transaction behavior
            factory.setupTransaction(async (transaction) => {
                const docRef = transaction.get(`${TEST_CONSTANTS.COLLECTIONS.PARTICIPANTS}/${TEST_CONSTANTS.PARTICIPANT_IDS.DEFAULT}`);
                const doc = await docRef;
                
                if (doc && doc.exists) {
                    transaction.update(doc.ref, {
                        [fieldMapping.dhq3SurveyStatus]: fieldMapping.submitted,
                        [fieldMapping.dhq3SurveyCompletionTime]: new Date().toISOString()
                    });
                }
                
                return { success: true };
            });

            // Test transaction
            const result = await mocks.firestore.runTransaction(async (transaction) => {
                const docRef = transaction.get(`${TEST_CONSTANTS.COLLECTIONS.PARTICIPANTS}/${TEST_CONSTANTS.PARTICIPANT_IDS.DEFAULT}`);
                const doc = await docRef;
                
                if (doc && doc.exists) {
                    transaction.update(doc.ref, {
                        [fieldMapping.dhq3SurveyStatus]: fieldMapping.submitted,
                        [fieldMapping.dhq3SurveyCompletionTime]: new Date().toISOString()
                    });
                }
                
                return { success: true };
            });

            expect(result).to.deep.equal({ success: true });
        });

        it('should handle field name sanitization in end-to-end processing', () => {
            // Test data with already sanitized field names (as would come from processAnalysisResultsCSV)
            const testData = [
                { 
                    'Respondent ID': TEST_CONSTANTS.PARTICIPANT_IDS.DEFAULT, 
                    'Energy_kcal': '2000',
                    'Protein_total_g': '75.4',
                    'star_Vitamin_A': '800',
                    'field_123_field': '15.2',
                    'calcium_mg': '1200'
                }
            ];
            
            const result = prepareDocumentsForFirestore(testData, TEST_CONSTANTS.STUDY_IDS.DEFAULT, 'analysisResults');
            
            // Verify document structure preserves sanitized field names
            expect(result.documents).to.have.length(1);
            expect(result.documents[0].id).to.equal(TEST_CONSTANTS.PARTICIPANT_IDS.DEFAULT);
            
            const data = result.documents[0].data;
            expect(data).to.have.property('Energy_kcal', '2000');
            expect(data).to.have.property('Protein_total_g', '75.4');
            expect(data).to.have.property('star_Vitamin_A', '800');
            expect(data).to.have.property('field_123_field', '15.2');
            expect(data).to.have.property('calcium_mg', '1200');
        });
    });

    describe('getProcessedRespondentIds', () => {
        it('should be a function that returns a Set', () => {
            expect(getProcessedRespondentIds).to.be.a('function');
            expect(getProcessedRespondentIds.length).to.equal(2);
        });

        it('should handle database errors', async () => {
            // Mock the collection to throw an error
            const originalCollection = mocks.firestore.collection;
            mocks.firestore.collection = sinon.stub().throws(new Error('Database error'));

            const result = await getProcessedRespondentIds('study_test', 'dhqAnalysisResults');
            expect(result).to.be.instanceof(Set);
            expect(result.size).to.equal(0);

            // Restore original mock
            mocks.firestore.collection = originalCollection;
        });

        it('should return an empty Set if the tracking doc does not exist', async () => {
            // Use setupDocumentRetrieval for non-existent document
            const collectionPath = 'dhq3SurveyCredentials/study_test/responseTracking';
            const docId = 'dhqAnalysisResults';
            
            factory.setupDocumentRetrieval(collectionPath, docId, null);

            const result = await getProcessedRespondentIds('study_test', 'dhqAnalysisResults');
            expect(result).to.be.instanceof(Set);
            expect(result.size).to.equal(0);
        });

        it('should return a Set of processed respondent IDs if present', async () => {
            // Use setupDocumentRetrieval for document with data
            const collectionPath = 'dhq3SurveyCredentials/study_test/responseTracking';
            const docId = 'dhqAnalysisResults';
            const mockData = { 
                [fieldMapping.dhq3ProcessedRespondentArray.toString()]: ['participant1', 'participant2'] 
            };
            
            factory.setupDocumentRetrieval(collectionPath, docId, mockData);

            const result = await getProcessedRespondentIds('study_test', 'dhqAnalysisResults');
            expect(result).to.be.instanceof(Set);
            expect(result.size).to.equal(2);
            expect(result.has('participant1')).to.be.true;
            expect(result.has('participant2')).to.be.true;
        });
    });

    // PERFORMANCE AND LOAD TESTING
    describe('Performance and Load Testing', () => {
        it('should handle large datasets efficiently', () => {
            const largeDataset = Array.from({ length: 10000 }, (_, i) => ({
                'Respondent ID': `participant${i}`,
                'Energy': (2000 + Math.random() * 1000).toFixed(2),
                'Protein': (50 + Math.random() * 50).toFixed(2)
            }));

            const startTime = Date.now();
            const result = prepareDocumentsForFirestore(largeDataset, 'study_large', 'analysisResults');
            const endTime = Date.now();
            const processingTime = endTime - startTime;

            assertResult(result, {
                documentCount: 10000,
                respondentCount: 10000,
                skippedCount: 0
            });
            expect(processingTime).to.be.lessThan(5000);
        });
    });
});
