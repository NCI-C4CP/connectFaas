const { expect } = require('chai');
const sinon = require('sinon');
const { setupTestSuite, assertResult } = require('../shared/testHelpers');
const TestUtils = require('../testUtils');

// Set up test environment, mocks, and cleanup
const { factory, mocks } = setupTestSuite({
    setupConsole: false,
    setupModuleMocks: true
});

const fieldMapping = require('../../utils/fieldToConceptIdMapping');
const { 
    createResponseDocID, 
    getDynamicChunkSize, 
    prepareDocumentsForFirestore
} = require('../../utils/dhq');

describe('DHQ Integration Tests', () => {

    // INTEGRATION TESTING
    describe('DHQ Integration Testing', () => {
        it('should demonstrate end-to-end CSV processing', async () => {
            // Set up mock data for processing and collection data for tracking.
            factory.setupCollectionData('dhq3SurveyCredentials/study_123/responseTracking', [
                {
                    id: 'dhqAnalysisResults',
                    [fieldMapping.dhq3StudyID]: 'study_123',
                    [fieldMapping.dhq3ProcessedRespondentArray]: ['participant1', 'participant2']
                }
            ]);

            // Set up count for available credentials.
            factory.setupCount('dhq3SurveyCredentials/study_123/availableCredentials', 500);

            // Test the processing function.
            const testData = [
                { 'Respondent ID': 'participant1', 'Energy': '2000', 'Protein': '50' },
                { 'Respondent ID': 'participant2', 'Energy': '1800', 'Protein': '45' },
                { 'Respondent ID': 'participant3', 'Energy': '2200', 'Protein': '60' }
            ];
            
            const result = prepareDocumentsForFirestore(testData, 'study_123', 'analysisResults');
            assertResult(result, {
                documentCount: 3,
                expectedIds: ['participant1', 'participant2', 'participant3']
            });
        });

        it('should query for participants', async () => {
            // Set up participants with different statuses
            const participants = [
                TestUtils.createMockParticipantData().createNotStartedDHQParticipant('participant1'),
                TestUtils.createMockParticipantData().createStartedDHQParticipant('participant2'),
                TestUtils.createMockParticipantData().createCompletedDHQParticipant('participant3')
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
            const participantData = TestUtils.createMockParticipantData().createNotStartedDHQParticipant('participant1');
            factory.setupDocumentData('participants', 'participant1', participantData);

            // Set up transaction behavior
            factory.setupTransaction(async (transaction) => {
                const docRef = transaction.get('participants/participant1');
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
                const docRef = transaction.get('participants/participant1');
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
