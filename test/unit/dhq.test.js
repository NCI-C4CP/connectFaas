const { expect } = require('chai');
const sinon = require('sinon');
const { setupTestSuite, assertResult } = require('../shared/testHelpers');
const TestUtils = require('../testUtils');
const ErrorScenarios = require('../shared/errorScenarios');

let factory, mocks;
let fieldMapping, dhqModule;
const errorScenarios = new ErrorScenarios();

before(() => {
    const mockSystem = setupTestSuite({
        setupConsole: true,
        setupModuleMocks: true
    });
    factory = mockSystem.factory;
    mocks = mockSystem.mocks;
    
    // Load modules after mocking is set up
    fieldMapping = require('../../utils/fieldToConceptIdMapping');
    dhqModule = require('../../utils/dhq');
});

describe('DHQ Unit Tests', () => {

    describe('Function Exports', () => {
        it('should export core utility functions', () => {
            expect(dhqModule.createResponseDocID).to.be.a('function');
            expect(dhqModule.getDynamicChunkSize).to.be.a('function');
            expect(dhqModule.prepareDocumentsForFirestore).to.be.a('function');
        });
    });

    describe('Core Utility Functions - Basic Implementation', () => {
        describe('dhqModule.createResponseDocID', () => {
            it('should create valid document ID from respondent ID', () => {
                const result = dhqModule.createResponseDocID('participant123');
                expect(result).to.equal('participant123');
            });

            it('should sanitize invalid Firestore characters', () => {
                const result = dhqModule.createResponseDocID('participant/123\\test.doc#id$[0]');
                expect(result).to.equal('participant_123_test_doc_id__0_');
            });

            it('should handle character combinations', () => {
                const result = dhqModule.createResponseDocID('participant.name#123$array[0]/path\\file');
                expect(result).to.equal('participant_name_123_array_0__path_file');
            });

            it('should handle null and undefined input', () => {
                expect(dhqModule.createResponseDocID(null)).to.be.null;
                expect(dhqModule.createResponseDocID(undefined)).to.be.null;
                expect(dhqModule.createResponseDocID('')).to.be.null;
            });

            it('should handle numeric input', () => {
                const result = dhqModule.createResponseDocID(12345);
                expect(result).to.equal('12345');
            });
        });

        describe('dhqModule.prepareDocumentsForFirestore', () => {
            it('should prepare detailed analysis documents correctly', () => {
                const testData = [
                    ['participant1', {
                        'Q001_FOOD001': { 'Question ID': 'Q001', 'Food ID': 'FOOD001', 'Answer': '15' },
                        'Q002_FOOD002': { 'Question ID': 'Q002', 'Food ID': 'FOOD002', 'Answer': '2' }
                    }],
                    ['participant2', {
                        'Q001_FOOD001': { 'Question ID': 'Q001', 'Food ID': 'FOOD001', 'Answer': '2' }
                    }]
                ];
                
                const result = dhqModule.prepareDocumentsForFirestore(testData, 'study_123', 'detailedAnalysis');
                
                assertResult(result, {
                    documentCount: 2,
                    expectedIds: ['participant1', 'participant2']
                });
                expect(result.documents[0].id).to.equal('participant1');
                expect(result.documents[0].data).to.have.property('Q001_FOOD001');
            });

            it('should prepare raw answers documents correctly', () => {
                const testData = [
                    ['participant1', {
                        'Q001': 'Yes',
                        'Q002': '2 cups',
                        'dhq3StudyID': 'study_123'
                    }]
                ];
                
                const result = dhqModule.prepareDocumentsForFirestore(testData, 'study_123', 'rawAnswers');
                
                expect(result.documents).to.have.length(1);
                expect(result.documents[0].id).to.equal('participant1');
                expect(result.documents[0].data).to.have.property('Q001', 'Yes');
                expect(result.documents[0].data).to.have.property('Q002', '2 cups');
            });

            it('should throw error for invalid data type', () => {
                const testData = [{ 'Respondent ID': 'participant1' }];
                
                expect(() => {
                    dhqModule.prepareDocumentsForFirestore(testData, 'study_123', 'invalidType');
                }).to.throw('Invalid data type in prepareDocumentsForFirestore(): invalidType');
            });
        });
    });

    describe('Error Handling and Edge Cases', () => {
        it('should handle malformed data gracefully', () => {
            const malformedInputs = [
                { input: null, expected: null },
                { input: undefined, expected: null },
                { input: '', expected: null },
                { input: 0, expected: null },
                { input: false, expected: null },
                { input: {}, expected: '_object Object_' },
                { input: [], expected: '' },
                { input: 'normal', expected: 'normal' }
            ];

            malformedInputs.forEach(testCase => {
                const result = dhqModule.createResponseDocID(testCase.input);
                expect(result).to.equal(testCase.expected, 
                    `Failed for input: ${testCase.input}`);
            });
        });

        it('should handle invalid data types in document preparation', () => {
            const invalidData = [
                { 'Respondent ID': 'participant1' }, // Missing required fields
                { 'Energy': '2000' }, // Missing respondent ID
                null, // Null entry
                undefined // Undefined entry
            ];

            expect(() => {
                dhqModule.prepareDocumentsForFirestore(invalidData, 'study_123', 'invalidType');
            }).to.throw('Invalid data type');
        });

        it('should handle memory pressure scenarios', () => {
            const originalMemoryUsage = process.memoryUsage;
            
            // Mock high memory usage
            process.memoryUsage = () => ({
                heapUsed: 1600 * 1024 * 1024, // 1.6GB
                heapTotal: 2048 * 1024 * 1024,
                external: 0,
                arrayBuffers: 0
            });

            const chunkSize = dhqModule.getDynamicChunkSize([]);
            expect(chunkSize).to.equal(100); // Should use minimum chunk size

            // Restore original function
            process.memoryUsage = originalMemoryUsage;
        });

        it('should handle extreme data size scenarios', () => {
            // Test with extremely large array
            const largeArray = new Array(10000).fill({ data: 'test' });
            const chunkSize = dhqModule.getDynamicChunkSize(largeArray);
            expect(chunkSize).to.be.a('number');
            expect(chunkSize).to.be.above(0);
        });

        it('should handle concurrent chunk size calculations', () => {
            const testData = [{ test: 'data' }];
            const results = [];
            
            // Simulate concurrent calls
            for (let i = 0; i < 10; i++) {
                results.push(dhqModule.getDynamicChunkSize(testData));
            }
            
            // All results should be consistent
            expect(results.every(size => size === results[0])).to.be.true;
        });

        it('should handle corrupted input data', () => {
            const corruptedData = [
                { 'Respondent ID': null },
                { 'Respondent ID': undefined },
                { 'Respondent ID': '' },
                { 'Respondent ID': 0 },
                { 'Respondent ID': false },
                { 'Respondent ID': {} },
                { 'Respondent ID': [] }
            ];

            corruptedData.forEach(data => {
                const result = dhqModule.createResponseDocID(data['Respondent ID']);
                expect(result).to.satisfy(val => val === null || typeof val === 'string');
            });
        });
    });


    describe('dhqModule.getDynamicChunkSize Functionality', () => {
        it('should scale chunk sizes appropriately based on data size', () => {
            const testScenarios = [
                { dataSize: 100, minChunks: 1 },
                { dataSize: 1500, minChunks: 2 },
                { dataSize: 5000, minChunks: 3 },
                { dataSize: 10000, minChunks: 5 }
            ];

            testScenarios.forEach(scenario => {
                const data = Array.from({ length: scenario.dataSize }, (_, i) => ({ id: `item_${i}` }));
                const chunkSize = dhqModule.getDynamicChunkSize();
                
                const chunks = [];
                for (let i = 0; i < data.length; i += chunkSize) {
                    chunks.push(data.slice(i, i + chunkSize));
                }
                
                // We get at least the minimum expected chunks
                expect(chunks.length).to.be.at.least(scenario.minChunks);
                // Each chunk doesn't exceed the calculated chunk size
                expect(chunks[0].length).to.be.at.most(chunkSize);
                // All data is included
                expect(chunks.flat().length).to.equal(scenario.dataSize);
            });
        });

        it('should handle memory pressure and usage monitoring', () => {
            const perfUtils = TestUtils.createMockPerformanceData();
            const originalMemoryUsage = process.memoryUsage;
            
            // Test different memory scenarios
            const memoryScenarios = [
                { memory: 800 * 1024 * 1024, expectedChunkSize: 1000 },
                { memory: 1100 * 1024 * 1024, expectedChunkSize: 500 },
                { memory: 1400 * 1024 * 1024, expectedChunkSize: 250 },
                { memory: 1700 * 1024 * 1024, expectedChunkSize: 100 }
            ];

            memoryScenarios.forEach(scenario => {
                // Test with direct memory usage mocking
                process.memoryUsage = () => ({ heapUsed: scenario.memory });
                let chunkSize = dhqModule.getDynamicChunkSize();
                expect(chunkSize).to.equal(scenario.expectedChunkSize);

                // Test with utility function
                const memoryUsage = perfUtils.createMemoryUsage(scenario.memory / (1024 * 1024));
                process.memoryUsage = () => memoryUsage;
                chunkSize = dhqModule.getDynamicChunkSize();
                expect(chunkSize).to.equal(scenario.expectedChunkSize);
            });

            // Restore original memory usage
            process.memoryUsage = originalMemoryUsage;
        });
    });

    describe('Test Overrides', () => {
        it('should preserve uid when overriding state in createNotStartedDHQParticipant', () => {
            const participantUtils = TestUtils.createMockParticipantData();
            const participant = participantUtils.createNotStartedDHQParticipant('participant123', {
                state: { query: 'test-query', site: 'test-site' }
            });
            
            // Verify uid is preserved, overrides are applied, and other fields are still present
            expect(participant.state.uid).to.equal('participant123');
            expect(participant.state.query).to.equal('test-query');
            expect(participant.state.site).to.equal('test-site');
            expect(participant[fieldMapping.dhq3SurveyStatus]).to.equal(fieldMapping.notStarted);
        });

        it('should preserve uid when overriding state in createStartedDHQParticipant', () => {
            const participantUtils = TestUtils.createMockParticipantData();
            const participant = participantUtils.createStartedDHQParticipant('participant456', {
                state: { sessionId: 'session-789', device: 'mobile' }
            });
            
            // Verify uid is preserved, overrides are applied, and other fields are still present
            expect(participant.state.uid).to.equal('participant456');
            expect(participant.state.sessionId).to.equal('session-789');
            expect(participant.state.device).to.equal('mobile');
            expect(participant[fieldMapping.dhq3SurveyStatus]).to.equal(fieldMapping.started);
        });

        it('should preserve uid when overriding state in createCompletedDHQParticipant', () => {
            const participantUtils = TestUtils.createMockParticipantData();
            const participant = participantUtils.createCompletedDHQParticipant('participant789', {
                state: { completionReason: 'normal', finalScore: 95 }
            });
            
            // Verify uid is preserved, overrides are applied, and other fields are still present
            expect(participant.state.uid).to.equal('participant789');
            expect(participant.state.completionReason).to.equal('normal');
            expect(participant.state.finalScore).to.equal(95);
            expect(participant[fieldMapping.dhq3SurveyStatus]).to.equal(fieldMapping.submitted);
        });

        it('should handle non-state overrides correctly', () => {
            const participantUtils = TestUtils.createMockParticipantData();
            const participant = participantUtils.createNotStartedDHQParticipant('participant123', {
                customField: 'custom-value',
                [fieldMapping.dhq3StudyID]: 'override-study-id'
            });
            
            // Verify uid is preserved, overrides are applied, and other fields are still present
            expect(participant.state.uid).to.equal('participant123');
            expect(participant.customField).to.equal('custom-value');
            expect(participant[fieldMapping.dhq3StudyID]).to.equal('override-study-id');
        });

        it('should handle empty overrides correctly', () => {
            const participantUtils = TestUtils.createMockParticipantData();
            const participant = participantUtils.createNotStartedDHQParticipant('participant123', {});
            
            // Verify uid is preserved, overrides are applied, and other fields are still present
            expect(participant.state.uid).to.equal('participant123');
            expect(Object.keys(participant.state)).to.deep.equal(['uid']);
        });

        it('should handle undefined overrides correctly', () => {
            const participantUtils = TestUtils.createMockParticipantData();
            const participant = participantUtils.createNotStartedDHQParticipant('participant123');
            
            // Verify uid is preserved, overrides are applied, and other fields are still present
            expect(participant.state.uid).to.equal('participant123');
            expect(Object.keys(participant.state)).to.deep.equal(['uid']);
        });
    });
});
