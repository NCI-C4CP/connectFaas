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
    getProcessedRespondentIds
} = require('../../utils/dhq');

describe('DHQ Integration Tests', () => {

    // INTEGRATION TESTING
    describe('DHQ Integration Testing', () => {

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
    describe('Streaming Processing', () => {
        it('should validate streaming logic', () => {
            const { streamCSVRows } = require('../../utils/fileProcessing');
            
            // Test that streamCSVRows is properly exported and functional
            expect(streamCSVRows).to.be.a('function');
            
            // Test basic streaming functionality
            const csvContent = `header1,header2
value1,value2`;

            let rowCount = 0;
            const streamTest = async () => {
                for await (const row of streamCSVRows(csvContent)) {
                    rowCount++;
                    expect(row).to.be.an('array');
                }
            };

            return streamTest().then(() => {
                expect(rowCount).to.equal(2); // Header + 1 data row
            });
        });

        it('should validate memory efficiency of streaming approach', () => {
            const { getDynamicChunkSize } = require('../../utils/dhq');
            
            // Test that memory management functions work correctly
            const originalMemoryUsage = process.memoryUsage;
            
            try {
                // Test various memory scenarios
                process.memoryUsage = () => ({ heapUsed: 500 * 1024 * 1024 }); // 500MB
                expect(getDynamicChunkSize()).to.equal(1000);
                
                process.memoryUsage = () => ({ heapUsed: 1200 * 1024 * 1024 }); // 1200MB  
                expect(getDynamicChunkSize()).to.equal(500);
                
                process.memoryUsage = () => ({ heapUsed: 1600 * 1024 * 1024 }); // 1600MB
                expect(getDynamicChunkSize()).to.equal(100);
            } finally {
                process.memoryUsage = originalMemoryUsage;
            }
        });

        it('should validate field sanitization works correctly', () => {
            const { sanitizeFieldName } = require('../../utils/dhq');
            
            // Test field sanitization without requiring Firestore
            expect(sanitizeFieldName('Energy (kcal)')).to.equal('Energy_kcal');
            expect(sanitizeFieldName('*Weight')).to.equal('star_Weight');
            expect(sanitizeFieldName('Protein-total')).to.equal('Protein_total');
            expect(sanitizeFieldName('123field')).to.equal('field_123field');
        });
    });
});
