const { setupTestSuite, assertResult } = require('../shared/testHelpers');
const TestUtils = require('../testUtils');
const ErrorScenarios = require('../shared/errorScenarios');

let factory, mocks;
let fieldMapping, dhqModule;
const errorScenarios = new ErrorScenarios();

beforeAll(() => {
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
            expect(dhqModule.createResponseDocID).toBeTypeOf('function');
            expect(dhqModule.getDynamicChunkSize).toBeTypeOf('function');
            expect(dhqModule.sanitizeFieldName).toBeTypeOf('function');
            expect(dhqModule.processAnalysisResultsCSV).toBeTypeOf('function');
            expect(dhqModule.processDetailedAnalysisCSV).toBeTypeOf('function');
            expect(dhqModule.processRawAnswersCSV).toBeTypeOf('function');
        });
    });

    describe('Core Utility Functions - Basic Implementation', () => {
        describe('dhqModule.sanitizeFieldName', () => {
            it('should handle basic field names without changes', () => {
                expect(dhqModule.sanitizeFieldName('Energy')).toBe('Energy');
                expect(dhqModule.sanitizeFieldName('protein_g')).toBe('protein_g');
                expect(dhqModule.sanitizeFieldName('vitamin_A123')).toBe('vitamin_A123');
            });

            it('should replace special characters with underscores', () => {
                expect(dhqModule.sanitizeFieldName('Energy (kcal)')).toBe('Energy_kcal');
                expect(dhqModule.sanitizeFieldName('Protein-total')).toBe('Protein_total');
                expect(dhqModule.sanitizeFieldName('vitamin A.mg')).toBe('vitamin_A_mg');
                expect(dhqModule.sanitizeFieldName('field name with spaces')).toBe('field_name_with_spaces');
            });

            it('should handle leading asterisk by adding star_ prefix', () => {
                expect(dhqModule.sanitizeFieldName('*Energy')).toBe('star_Energy');
                expect(dhqModule.sanitizeFieldName('*total_calories')).toBe('star_total_calories');
                expect(dhqModule.sanitizeFieldName('***')).toBe('star');
            });

            it('should add field_ prefix when starting with numbers', () => {
                expect(dhqModule.sanitizeFieldName('123calories')).toBe('field_123calories');
                expect(dhqModule.sanitizeFieldName('2024_data')).toBe('field_2024_data');
            });

            it('should throw errors for invalid inputs', () => {
                expect(() => dhqModule.sanitizeFieldName(null)).toThrow('Invalid field name: null (must be a non-empty string)');
                expect(() => dhqModule.sanitizeFieldName(undefined)).toThrow('Invalid field name: undefined (must be a non-empty string)');
                expect(() => dhqModule.sanitizeFieldName('')).toThrow('Invalid field name:  (must be a non-empty string)');
                expect(() => dhqModule.sanitizeFieldName('___')).toThrow('empty after sanitization');
            });
        });

        describe('dhqModule.createResponseDocID', () => {
            it('should create valid document ID from respondent ID', () => {
                const result = dhqModule.createResponseDocID('participant123');
                expect(result).toBe('participant123');
            });

            it('should sanitize invalid Firestore characters', () => {
                const result = dhqModule.createResponseDocID('participant/123\\test.doc#id$[0]');
                expect(result).toBe('participant_123_test_doc_id__0_');
            });

            it('should handle character combinations', () => {
                const result = dhqModule.createResponseDocID('participant.name#123$array[0]/path\\file');
                expect(result).toBe('participant_name_123_array_0__path_file');
            });

            it('should handle null and undefined input', () => {
                expect(dhqModule.createResponseDocID(null)).toBeNull();
                expect(dhqModule.createResponseDocID(undefined)).toBeNull();
                expect(dhqModule.createResponseDocID('')).toBeNull();
            });

            it('should handle numeric input', () => {
                const result = dhqModule.createResponseDocID(12345);
                expect(result).toBe('12345');
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
                expect(result).toBe(testCase.expected, `Failed for input: ${testCase.input}`);
            });
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
            expect(chunkSize).toBe(100); // Should use minimum chunk size

            // Restore original function
            process.memoryUsage = originalMemoryUsage;
        });

        it('should handle extreme data size scenarios', () => {
            // Test with extremely large array
            const largeArray = new Array(10000).fill({ data: 'test' });
            const chunkSize = dhqModule.getDynamicChunkSize(largeArray);
            expect(chunkSize).toBeTypeOf('number');
            expect(chunkSize).toBeGreaterThan(0);
        });

        it('should handle concurrent chunk size calculations', () => {
            const testData = [{ test: 'data' }];
            const results = [];

            // Simulate concurrent calls
            for (let i = 0; i < 10; i++) {
                results.push(dhqModule.getDynamicChunkSize(testData));
            }

            // All results should be consistent
            expect(results.every(size => size === results[0])).toBe(true);
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
                expect(result).toSatisfy(val => val === null || typeof val === 'string');
            });
        });
    });

    describe('dhqModule.getDynamicChunkSize Functionality', () => {
        it('should return a valid chunk size based on current memory usage', () => {
            const chunkSize = dhqModule.getDynamicChunkSize();

            expect(chunkSize).toBeGreaterThan(0);
            expect(Number.isInteger(chunkSize)).toBe(true);

            // Chunk size can be used to partition datasets of various sizes
            const testDataSizes = [100, 1500, 5000, 10000];
            testDataSizes.forEach(dataSize => {
                const data = Array.from({ length: dataSize }, (_, i) => ({ id: `item_${i}` }));
                const chunks = [];
                for (let i = 0; i < data.length; i += chunkSize) {
                    chunks.push(data.slice(i, i + chunkSize));
                }

                // Each chunk doesn't exceed the calculated chunk size
                expect(chunks[0].length).toBeLessThanOrEqual(chunkSize);
                // All data is included
                expect(chunks.flat().length).toBe(dataSize);
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
                expect(chunkSize).toBe(scenario.expectedChunkSize);

                // Test with utility function
                const memoryUsage = perfUtils.createMemoryUsage(scenario.memory / (1024 * 1024));
                process.memoryUsage = () => memoryUsage;
                chunkSize = dhqModule.getDynamicChunkSize();
                expect(chunkSize).toBe(scenario.expectedChunkSize);
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
            expect(participant.state.uid).toBe('participant123');
            expect(participant.state.query).toBe('test-query');
            expect(participant.state.site).toBe('test-site');
            expect(participant[fieldMapping.dhq3SurveyStatus]).toBe(fieldMapping.notStarted);
        });

        it('should preserve uid when overriding state in createStartedDHQParticipant', () => {
            const participantUtils = TestUtils.createMockParticipantData();
            const participant = participantUtils.createStartedDHQParticipant('participant456', {
                state: { sessionId: 'session-789', device: 'mobile' }
            });

            // Verify uid is preserved, overrides are applied, and other fields are still present
            expect(participant.state.uid).toBe('participant456');
            expect(participant.state.sessionId).toBe('session-789');
            expect(participant.state.device).toBe('mobile');
            expect(participant[fieldMapping.dhq3SurveyStatus]).toBe(fieldMapping.started);
        });

        it('should preserve uid when overriding state in createCompletedDHQParticipant', () => {
            const participantUtils = TestUtils.createMockParticipantData();
            const participant = participantUtils.createCompletedDHQParticipant('participant789', {
                state: { completionReason: 'normal', finalScore: 95 }
            });

            // Verify uid is preserved, overrides are applied, and other fields are still present
            expect(participant.state.uid).toBe('participant789');
            expect(participant.state.completionReason).toBe('normal');
            expect(participant.state.finalScore).toBe(95);
            expect(participant[fieldMapping.dhq3SurveyStatus]).toBe(fieldMapping.submitted);
        });

        it('should handle non-state overrides correctly', () => {
            const participantUtils = TestUtils.createMockParticipantData();
            const participant = participantUtils.createNotStartedDHQParticipant('participant123', {
                customField: 'custom-value',
                [fieldMapping.dhq3StudyID]: 'override-study-id'
            });

            // Verify uid is preserved, overrides are applied, and other fields are still present
            expect(participant.state.uid).toBe('participant123');
            expect(participant.customField).toBe('custom-value');
            expect(participant[fieldMapping.dhq3StudyID]).toBe('override-study-id');
        });

        it('should handle empty overrides correctly', () => {
            const participantUtils = TestUtils.createMockParticipantData();
            const participant = participantUtils.createNotStartedDHQParticipant('participant123', {});

            // Verify uid is preserved, overrides are applied, and other fields are still present
            expect(participant.state.uid).toBe('participant123');
            expect(Object.keys(participant.state)).toEqual(['uid']);
        });

        it('should handle undefined overrides correctly', () => {
            const participantUtils = TestUtils.createMockParticipantData();
            const participant = participantUtils.createNotStartedDHQParticipant('participant123');

            // Verify uid is preserved, overrides are applied, and other fields are still present
            expect(participant.state.uid).toBe('participant123');
            expect(Object.keys(participant.state)).toEqual(['uid']);
        });
    });

    describe('CSV Processing', () => {
        describe('Error Handling', () => {
            it('should handle missing required columns gracefully', async () => {
                const csvContent = `Energy,Protein
2000,50`;

                try {
                    const { processAnalysisResultsCSV } = require('../../utils/dhq');
                    await processAnalysisResultsCSV(csvContent, 'study_test');
                    expect.fail('Should have thrown an error for missing Respondent ID column');
                } catch (error) {
                    expect(error.message).toContain('Respondent ID column not found');
                }
            });

            it('should handle missing required columns in detailed analysis', async () => {
                const csvContent = `Energy,Protein
2000,50`;

                try {
                    const { processDetailedAnalysisCSV } = require('../../utils/dhq');
                    await processDetailedAnalysisCSV(csvContent, 'study_test');
                    expect.fail('Should have thrown an error for missing required columns');
                } catch (error) {
                    expect(error.message).toContain('Required columns missing');
                }
            });

            it('should handle missing required columns in raw answers', async () => {
                const csvContent = `Energy,Protein
2000,50`;

                try {
                    const { processRawAnswersCSV } = require('../../utils/dhq');
                    await processRawAnswersCSV(csvContent, 'study_test');
                    expect.fail('Should have thrown an error for missing required columns');
                } catch (error) {
                    expect(error.message).toContain('Required columns missing');
                }
            });
        });

        describe('Memory Management Integration', () => {
            it('should adapt chunk sizes based on memory pressure', () => {
                const originalMemoryUsage = process.memoryUsage;

                const memoryScenarios = [
                    { heapUsed: 800 * 1024 * 1024, expectedSize: 1000 },  // Low memory
                    { heapUsed: 1200 * 1024 * 1024, expectedSize: 500 },  // Medium memory
                    { heapUsed: 1400 * 1024 * 1024, expectedSize: 250 },  // High memory
                    { heapUsed: 1600 * 1024 * 1024, expectedSize: 100 }   // Critical memory
                ];

                try {
                    memoryScenarios.forEach(scenario => {
                        process.memoryUsage = () => ({ heapUsed: scenario.heapUsed });
                        const chunkSize = dhqModule.getDynamicChunkSize();
                        expect(chunkSize).toBe(scenario.expectedSize);
                    });
                } finally {
                    process.memoryUsage = originalMemoryUsage;
                }
            });
        });
    });
});
