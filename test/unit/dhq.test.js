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
        describe('dhqModule.sanitizeFieldName', () => {
            it('should handle basic field names without changes', () => {
                expect(dhqModule.sanitizeFieldName('Energy')).to.equal('Energy');
                expect(dhqModule.sanitizeFieldName('protein_g')).to.equal('protein_g');
                expect(dhqModule.sanitizeFieldName('vitamin_A123')).to.equal('vitamin_A123');
            });

            it('should replace special characters with underscores', () => {
                expect(dhqModule.sanitizeFieldName('Energy (kcal)')).to.equal('Energy_kcal');
                expect(dhqModule.sanitizeFieldName('Protein-total')).to.equal('Protein_total');
                expect(dhqModule.sanitizeFieldName('vitamin A.mg')).to.equal('vitamin_A_mg');
                expect(dhqModule.sanitizeFieldName('field name with spaces')).to.equal('field_name_with_spaces');
            });

            it('should handle leading asterisk by adding star_ prefix', () => {
                expect(dhqModule.sanitizeFieldName('*Energy')).to.equal('star_Energy');
                expect(dhqModule.sanitizeFieldName('*total_calories')).to.equal('star_total_calories');
                expect(dhqModule.sanitizeFieldName('***')).to.equal('star');
            });

            it('should add field_ prefix when starting with numbers', () => {
                expect(dhqModule.sanitizeFieldName('123calories')).to.equal('field_123calories');
                expect(dhqModule.sanitizeFieldName('2024_data')).to.equal('field_2024_data');
            });

            it('should throw errors for invalid inputs', () => {
                expect(() => dhqModule.sanitizeFieldName(null)).to.throw('Invalid field name: null (must be a non-empty string)');
                expect(() => dhqModule.sanitizeFieldName(undefined)).to.throw('Invalid field name: undefined (must be a non-empty string)');
                expect(() => dhqModule.sanitizeFieldName('')).to.throw('Invalid field name:  (must be a non-empty string)');
                expect(() => dhqModule.sanitizeFieldName('___')).to.throw('empty after sanitization');
            });
        });

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
            it('should prepare detailed analysis documents correctly: one document per question', () => {
                // Test basic detailed analysis documents with {id, data} structure
                const basicTestData = [
                    {
                        id: 'participant1_Q001_FOOD001',
                        data: {
                        [fieldMapping.dhq3Username]: 'participant1',
                        [fieldMapping.dhq3StudyID]: 'study_123',
                        Answer: '15',
                        Energy: '50',
                        },
                    },
                    {
                        id: 'participant1_Q002_FOOD002',
                        data: {
                        [fieldMapping.dhq3Username]: 'participant1',
                        [fieldMapping.dhq3StudyID]: 'study_123',
                        Answer: '2',
                        Energy: '25',
                        },
                    },
                ];

                const basicResult = dhqModule.prepareDocumentsForFirestore(basicTestData, 'study_123', 'detailedAnalysis');

                // Should return the same documents structure
                expect(basicResult.documents).to.have.length(2);
                expect(basicResult.documents[0].id).to.equal('participant1_Q001_FOOD001');
                expect(basicResult.documents[1].id).to.equal('participant1_Q002_FOOD002');
                expect(basicResult.documents[0].data).to.have.property('Answer', '15');
                expect(basicResult.documents[0].data).to.have.property('Energy', '50');

                // Test detailed analysis documents with sanitized field names
                const sanitizedTestData = [
                    {
                        id: 'participant1_Q001_FOOD001',
                        data: {
                        [fieldMapping.dhq3Username]: 'participant1',
                        [fieldMapping.dhq3StudyID]: 'study_123',
                        Energy_kcal: '2000',
                        Protein_g: '50',
                        star_Special_Field: '10',
                        field_123numeric: '25',
                        },
                    },
                ];

                const sanitizedResult = dhqModule.prepareDocumentsForFirestore(sanitizedTestData, 'study_123', 'detailedAnalysis');

                expect(sanitizedResult.documents).to.have.length(1);
                expect(sanitizedResult.documents[0].id).to.equal('participant1_Q001_FOOD001');

                const data = sanitizedResult.documents[0].data;
                expect(data).to.have.property('Energy_kcal', '2000');
                expect(data).to.have.property('Protein_g', '50');
                expect(data).to.have.property('star_Special_Field', '10');
                expect(data).to.have.property('field_123numeric', '25');
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
                expect(result).to.equal(testCase.expected, `Failed for input: ${testCase.input}`);
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
                arrayBuffers: 0,
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
