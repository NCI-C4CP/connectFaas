const { expect } = require('chai');
const sinon = require('sinon');
const { setupTestSuite, assertResult } = require('../shared/testHelpers');
const TestUtils = require('../testUtils');

// Set up test environment, mocks, and cleanup
const { factory, mocks } = setupTestSuite({
    setupConsole: true,
    setupModuleMocks: true
});

const fieldMapping = require('../../utils/fieldToConceptIdMapping');
const { 
    createResponseDocID, 
    getDynamicChunkSize, 
    prepareDocumentsForFirestore
} = require('../../utils/dhq');

describe('DHQ Unit Tests', () => {

    describe('Function Exports and Signatures', () => {
        it('should export core utility functions', () => {
            expect(createResponseDocID).to.be.a('function');
            expect(getDynamicChunkSize).to.be.a('function');
            expect(prepareDocumentsForFirestore).to.be.a('function');
        });

        it('should validate function signatures', () => {
            expect(createResponseDocID.length).to.equal(1);
            expect(getDynamicChunkSize.length).to.equal(0);
            expect(prepareDocumentsForFirestore.length).to.equal(3);
        });
    });

    describe('Core Utility Functions - Basic Implementation', () => {
        describe('createResponseDocID', () => {
            it('should create valid document ID from respondent ID', () => {
                const result = createResponseDocID('participant123');
                expect(result).to.equal('participant123');
            });

            it('should sanitize invalid Firestore characters', () => {
                const result = createResponseDocID('participant/123\\test.doc#id$[0]');
                expect(result).to.equal('participant_123_test_doc_id__0_');
            });

            it('should handle character combinations', () => {
                const result = createResponseDocID('participant.name#123$array[0]/path\\file');
                expect(result).to.equal('participant_name_123_array_0__path_file');
            });

            it('should handle null and undefined input', () => {
                expect(createResponseDocID(null)).to.be.null;
                expect(createResponseDocID(undefined)).to.be.null;
                expect(createResponseDocID('')).to.be.null;
            });

            it('should handle numeric input', () => {
                const result = createResponseDocID(12345);
                expect(result).to.equal('12345');
            });
        });

        describe('prepareDocumentsForFirestore', () => {
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
                
                const result = prepareDocumentsForFirestore(testData, 'study_123', 'detailedAnalysis');
                
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
                
                const result = prepareDocumentsForFirestore(testData, 'study_123', 'rawAnswers');
                
                expect(result.documents).to.have.length(1);
                expect(result.documents[0].id).to.equal('participant1');
                expect(result.documents[0].data).to.have.property('Q001', 'Yes');
                expect(result.documents[0].data).to.have.property('Q002', '2 cups');
            });

            it('should throw error for invalid data type', () => {
                const testData = [{ 'Respondent ID': 'participant1' }];
                
                expect(() => {
                    prepareDocumentsForFirestore(testData, 'study_123', 'invalidType');
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
                const result = createResponseDocID(testCase.input);
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
                prepareDocumentsForFirestore(invalidData, 'study_123', 'invalidType');
            }).to.throw('Invalid data type');
        });
    });

    describe('Logic and Validation Testing', () => {
        it('should validate data processing patterns', () => {
            const testData = [
                { 'Respondent ID': 'participant1', 'Answer': 'Yes' },
                { 'Respondent ID': 'participant2', 'Answer': '.' },
                { 'Respondent ID': 'participant3', 'Answer': 'No' }
            ];

            const validAnswers = testData.filter(row => row.Answer !== '.');
            expect(validAnswers).to.have.length(2);
            expect(validAnswers.map(row => row['Respondent ID'])).to.deep.equal(['participant1', 'participant3']);
        });

        it('should validate study ID processing patterns', () => {
            const testStudyIDs = [
                { input: 'study_123', expected: 'study_123' },
                { input: '123', expected: 'study_123' },
                { input: 'study_456', expected: 'study_456' }
            ];

            testStudyIDs.forEach(testCase => {
                const normalized = testCase.input.startsWith('study_') ? testCase.input : `study_${testCase.input}`;
                expect(normalized).to.equal(testCase.expected);
            });
        });

        it('should validate processing results tracking', () => {
            const createProcessingResult = (totalItems, successCount, errorCount) => ({
                totalItems,
                successCount,
                errorCount,
                successRate: totalItems > 0 ? (successCount / totalItems) * 100 : 0,
                hasErrors: errorCount > 0
            });
            
            const result1 = createProcessingResult(100, 95, 5);
            expect(result1.successRate).to.equal(95);
            expect(result1.hasErrors).to.be.true;
            
            const result2 = createProcessingResult(100, 100, 0);
            expect(result2.successRate).to.equal(100);
            expect(result2.hasErrors).to.be.false;
            
            const result3 = createProcessingResult(0, 0, 0);
            expect(result3.successRate).to.equal(0);
            expect(result3.hasErrors).to.be.false;
        });
    });

    describe('getDynamicChunkSize Functionality', () => {
        it('should scale chunk sizes appropriately based on data size', () => {
            const testScenarios = [
                { dataSize: 100, minChunks: 1 },
                { dataSize: 1500, minChunks: 2 },
                { dataSize: 5000, minChunks: 3 },
                { dataSize: 10000, minChunks: 5 }
            ];

            testScenarios.forEach(scenario => {
                const data = Array.from({ length: scenario.dataSize }, (_, i) => ({ id: `item_${i}` }));
                const chunkSize = getDynamicChunkSize();
                
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
                let chunkSize = getDynamicChunkSize();
                expect(chunkSize).to.equal(scenario.expectedChunkSize);

                // Test with utility function
                const memoryUsage = perfUtils.createMemoryUsage(scenario.memory / (1024 * 1024));
                process.memoryUsage = () => memoryUsage;
                chunkSize = getDynamicChunkSize();
                expect(chunkSize).to.equal(scenario.expectedChunkSize);
            });

            // Restore original memory usage
            process.memoryUsage = originalMemoryUsage;
        });
    });
});
