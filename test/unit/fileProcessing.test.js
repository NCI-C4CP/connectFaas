const { expect } = require('chai');
const sinon = require('sinon');
const { setupTestSuite, createConsoleSafeStub } = require('../shared/testHelpers');
const zlib = require('zlib');

const fileProcessing = require('../../utils/fileProcessing.js');

describe('File Processing (Unzipping, CSV Parsing, Validation) Test Suite', () => {
    
    let factory, mocks;

    before(() => {
        const mockSystem = setupTestSuite({
            setupConsole: false,
            setupModuleMocks: false
        });
        factory = mockSystem.factory;
        mocks = mockSystem.mocks;
    });

    describe('cleanCSVContent', () => {
        it('should remove comment lines starting with default character (*)', () => {
            const csvContent = `* This is a comment line
header1,header2,header3
value1,value2,value3
* Another comment line
value4,value5,value6`;

            const result = fileProcessing.cleanCSVContent(csvContent);
            
            expect(result).to.equal(`header1,header2,header3
value1,value2,value3
value4,value5,value6`);
        });

        it('should remove empty lines', () => {
            const csvContent = `header1,header2,header3

value1,value2,value3

value4,value5,value6
`;

            const result = fileProcessing.cleanCSVContent(csvContent);
            
            expect(result).to.equal(`header1,header2,header3
value1,value2,value3
value4,value5,value6`);
        });

        it('should handle empty string input', () => {
            const result = fileProcessing.cleanCSVContent('');
            expect(result).to.equal('');
        });
    });

    describe('parseCSV', () => {
        let consoleWarnStub;

        beforeEach(() => {
            consoleWarnStub = createConsoleSafeStub('warn');
        });

        afterEach(() => {
            if (consoleWarnStub && consoleWarnStub.restore) {
                consoleWarnStub.restore();
                consoleWarnStub = null;
            }
        });
        it('should parse basic CSV with headers to an array of objects', () => {
            const csvContent = `header1,header2,header3
value1,value2,value3
value4,value5,value6
value7,value8,value9`;

            const result = fileProcessing.parseCSV(csvContent);

            expect(result).to.have.lengthOf(3);
            expect(result[0]).to.deep.equal({
                header1: 'value1',
                header2: 'value2',
                header3: 'value3'
            });
            expect(result[1]).to.deep.equal({
                header1: 'value4',
                header2: 'value5',
                header3: 'value6'
            });
            expect(result[2]).to.deep.equal({
                header1: 'value7',
                header2: 'value8',
                header3: 'value9'
            });
        });

        it('should correctly parse CSV with quoted fields', () => {
            const csvContent = `name,description,age
"John Doe","A field with, commas",50
"Jane Smith","Another ""quoted"" field",30`;

            const result = fileProcessing.parseCSV(csvContent);

            expect(result).to.have.lengthOf(2);
            expect(result[0]).to.deep.equal({
                name: 'John Doe',
                description: 'A field with, commas',
                age: '50'
            });
            expect(result[1]).to.deep.equal({
                name: 'Jane Smith',
                description: 'Another "quoted" field',
                age: '30'
            });
        });

        it('should handle CSV with CRLF line endings', () => {
            const csvContent = `header1,header2\r\nvalue1,value2\r\nvalue3,value4`;

            const result = fileProcessing.parseCSV(csvContent);

            expect(result).to.have.lengthOf(2);
            expect(result[0]).to.deep.equal({
                header1: 'value1',
                header2: 'value2'
            });
        });

        it('should convert numbers when convertNumbers option is true', () => {
            const csvContent = `id,score,name
1,85.5,John
2,92.0,Jane
3,78,Bob`;

            const result = fileProcessing.parseCSV(csvContent, { convertNumbers: true });

            expect(result).to.have.lengthOf(3);
            expect(result[0]).to.deep.equal({
                id: 1,
                score: 85.5,
                name: 'John'
            });
            expect(result[1].score).to.equal(92.0);
            expect(result[2].id).to.equal(3);
        });

        it('should handle CSV with custom comment character', () => {
            const csvContent = `# This is a comment
header1,header2
value1,value2
# Another comment
value3,value4`;

            const result = fileProcessing.parseCSV(csvContent, { commentChar: '#' });

            expect(result).to.have.lengthOf(2);
            expect(result[0]).to.deep.equal({
                header1: 'value1',
                header2: 'value2'
            });
        });

        it('should return empty array for empty CSV', () => {
            const result = fileProcessing.parseCSV('');
            expect(result).to.be.an('array').that.is.empty;
        });

        it('should return empty array for CSV with only headers', () => {
            const csvContent = `header1,header2,header3`;
            const result = fileProcessing.parseCSV(csvContent);
            expect(result).to.be.an('array').that.is.empty;
        });

        it('should skip rows with mismatched column counts', () => {
            // Create a local stub for this test
            let localConsoleWarnStub = createConsoleSafeStub('warn');
            
            const csvContent = `header1,header2,header3
value1,value2,value3
value4,value5
value6,value7,value8`;

            const result = fileProcessing.parseCSV(csvContent);

            expect(result).to.have.lengthOf(2);
            expect(result[0]).to.deep.equal({
                header1: 'value1',
                header2: 'value2',
                header3: 'value3'
            });
            expect(result[1]).to.deep.equal({
                header1: 'value6',
                header2: 'value7',
                header3: 'value8'
            });
            
            // Check console.warn was called
            expect(localConsoleWarnStub.calledOnce).to.be.true;
            expect(localConsoleWarnStub.firstCall.args[0]).to.include('Row 3 has 2 columns');
            
            // Restore the local stub
            if (localConsoleWarnStub.restore) {
                localConsoleWarnStub.restore();
            }
        });

        it('should handle trailing newlines and empty rows', () => {
            const csvContent = `header1,header2
value1,value2

value3,value4
`;

            const result = fileProcessing.parseCSV(csvContent);

            expect(result).to.have.lengthOf(2);
            expect(result[0]).to.deep.equal({
                header1: 'value1',
                header2: 'value2'
            });
            expect(result[1]).to.deep.equal({
                header1: 'value3',
                header2: 'value4'
            });
        });

        it('should handle empty fields', () => {
            const csvContent = `name,middle,last
John,,Doe
Jane,Marie,Smith
Bob,"",`;

            const result = fileProcessing.parseCSV(csvContent);

            expect(result).to.have.lengthOf(3);
            expect(result[0]).to.deep.equal({
                name: 'John',
                middle: '',
                last: 'Doe'
            });
            expect(result[2]).to.deep.equal({
                name: 'Bob',
                middle: '',
                last: ''
            });
        });

        it('should not convert dots and empty strings to numbers', () => {
            const csvContent = `id,score,comment
1,.,good
2,,excellent
3,85.5,average`;

            const result = fileProcessing.parseCSV(csvContent, { convertNumbers: true });

            expect(result[0]).to.deep.equal({
                id: 1,
                score: '.',
                comment: 'good'
            });
            expect(result[1]).to.deep.equal({
                id: 2,
                score: '',
                comment: 'excellent'
            });
            expect(result[2]).to.deep.equal({
                id: 3,
                score: 85.5,
                comment: 'average'
            });
        });

        it('should handle malformed CSV gracefully', () => {
            const csvContent = `header1,header2
"unclosed quote,value2`;

            const result = fileProcessing.parseCSV(csvContent);

            // Parser skips malformed rows with unclosed quotes
            expect(result).to.be.an('array').that.is.empty;
        });
    });

    describe('validateCSVRow', () => {
        it('should validate rows with various field scenarios', () => {
            const requiredFields = ['id', 'name', 'email'];
            
            const testCases = [
                {
                    row: { id: '1', name: 'John', email: 'john@example.com' },
                    expected: { isValid: true, missingFields: [] }
                },
                {
                    row: { id: '1', name: 'John' }, // missing email
                    expected: { isValid: false, missingFields: ['email'] }
                },
                {
                    row: { id: '1', name: '', email: 'john@example.com' }, // empty name
                    expected: { isValid: false, missingFields: ['name'] }
                },
                {
                    row: { id: '1', name: null, email: 'john@example.com' }, // null name
                    expected: { isValid: false, missingFields: ['name'] }
                },
                {
                    row: { id: '1', email: 'john@example.com' }, // undefined name
                    expected: { isValid: false, missingFields: ['name'] }
                },
                {
                    row: { id: '1' }, // multiple missing fields
                    expected: { isValid: false, missingFields: ['name', 'email'] }
                }
            ];

            testCases.forEach(testCase => {
                const result = fileProcessing.validateCSVRow(testCase.row, requiredFields);
                expect(result).to.deep.equal(testCase.expected);
            });
        });
    });

    describe('extractZipFiles', () => {
        let mockZlib;
        let zlibStub;

        beforeEach(() => {
            mockZlib = {
                inflateRawSync: sinon.stub()
            };
            // Only stub zlib.inflateRawSync if not already stubbed
            const zlib = require('zlib');
            if (!zlib.inflateRawSync.isSinonProxy) {
                zlibStub = sinon.stub(zlib, 'inflateRawSync').callsFake(mockZlib.inflateRawSync);
            }
        });

        afterEach(() => {
            if (zlibStub && zlibStub.restore) {
                zlibStub.restore();
                zlibStub = null;
            }
        });

        

        it('should extract files from a valid ZIP buffer', async () => {
            const filename = 'test.txt';
            const content = Buffer.from('Hello, World!\nThis is a test file.');
            
            const zipBuffer = createMockZipBuffer(filename, content);
            const base64Data = zipBuffer.toString('base64');

            mockZlib.inflateRawSync.returns(content);

            const result = await fileProcessing.extractZipFiles(base64Data);

            expect(result).to.be.an('array');
            expect(result).to.have.lengthOf(1);
            expect(result[0]).to.have.property('filename', filename);
            expect(result[0]).to.have.property('content');
            expect(result[0]).to.have.property('size');
            expect(result[0]).to.have.property('compressedSize');
        });

        it('should handle uncompressed files (compression method 0)', async () => {
            const filename = 'uncompressed.txt';
            const content = Buffer.from('Hello, World!\nThis is a test uncompressed file.');
            
            const zipBuffer = createMockZipBuffer(filename, content, 0);
            const base64Data = zipBuffer.toString('base64');

            const result = await fileProcessing.extractZipFiles(base64Data);

            expect(result).to.have.lengthOf(1);
            expect(result[0].content.toString()).to.equal('Hello, World!\nThis is a test uncompressed file.');
        });

        it('should handle DEFLATE compressed files (compression method 8)', async () => {
            const filename = 'compressed.txt';
            const originalContent = Buffer.from('Hello, World!\nThis is a test file to compress.');
            const compressedContent = Buffer.from('compressed-data');
            
            const zipBuffer = createMockZipBuffer(filename, compressedContent, 8);
            const base64Data = zipBuffer.toString('base64');

            mockZlib.inflateRawSync.returns(originalContent);

            const result = await fileProcessing.extractZipFiles(base64Data);

            expect(result).to.have.lengthOf(1);
            expect(mockZlib.inflateRawSync.calledOnce).to.be.true;
            expect(result[0].content).to.equal(originalContent);
        });

        it('should handle decompression errors gracefully', async () => {
            const filename = 'corrupt.txt';
            const content = Buffer.from('Corrupt compressed data');
            
            const zipBuffer = createMockZipBuffer(filename, content, 8);
            const base64Data = zipBuffer.toString('base64');

            mockZlib.inflateRawSync.throws(new Error('Decompression failed'));

            const result = await fileProcessing.extractZipFiles(base64Data);

            expect(result).to.have.lengthOf(1);
            expect(Buffer.compare(result[0].content, Buffer.from('compressed-data'))).to.equal(0);
        });

        it('should skip unsupported compression methods', async () => {
            const filename = 'unsupported.txt';
            const content = Buffer.from('Content with unsupported compression');
            
            const zipBuffer = createMockZipBuffer(filename, content, 9);
            const base64Data = zipBuffer.toString('base64');

            const result = await fileProcessing.extractZipFiles(base64Data);

            expect(result).to.be.an('array').that.is.empty;
        });

        it('should skip directory entries', async () => {
            const dirName = 'test-directory/';
            const content = Buffer.alloc(0);
            
            const zipBuffer = createMockZipBuffer(dirName, content);
            const base64Data = zipBuffer.toString('base64');

            const result = await fileProcessing.extractZipFiles(base64Data);

            expect(result).to.be.an('array').that.is.empty;
        });

        it('should throw error for invalid ZIP data', async () => {
            const invalidBase64 = 'not-a-valid-zip-file';

            try {
                await fileProcessing.extractZipFiles(invalidBase64);
                expect.fail('Should have thrown an error');
            } catch (error) {
                expect(error.message).to.include('Failed to extract ZIP file');
            }
        });

        it('should throw error for ZIP file without EOCD', async () => {
            const incompleteZip = Buffer.from('This is not a complete ZIP file');
            const base64Data = incompleteZip.toString('base64');

            try {
                await fileProcessing.extractZipFiles(base64Data);
                expect.fail('Should have thrown an error');
            } catch (error) {
                expect(error.message).to.include('End of Central Directory record not found');
            }
        });
    });

    describe('Error Scenarios', () => {
        const ErrorScenarios = require('../shared/errorScenarios');
        const errorScenarios = new ErrorScenarios();

        it('should handle network-related file download errors', async () => {
            const networkError = errorScenarios.createNetworkError('File download failed');
            
            // Mock a scenario where file extraction fails due to network issues
            const corruptedZip = Buffer.from('corrupted-network-response');
            const base64Data = corruptedZip.toString('base64');

            try {
                await fileProcessing.extractZipFiles(base64Data);
                expect.fail('Should have thrown an error');
            } catch (error) {
                expect(error.message).to.include('Failed to extract ZIP file');
            }
        });

        it('should handle various ZIP corruption scenarios', async () => {
            const corruptionScenarios = [
                { name: 'truncated header', data: Buffer.from('PK\x03\x04') },
                { name: 'missing signature', data: Buffer.from('INVALID_ZIP_DATA') },
                { name: 'empty buffer', data: Buffer.alloc(0) },
                { name: 'partial central directory', data: Buffer.from('PK\x01\x02incomplete') }
            ];

            for (const scenario of corruptionScenarios) {
                const base64Data = scenario.data.toString('base64');
                
                try {
                    await fileProcessing.extractZipFiles(base64Data);
                    expect.fail(`Should have thrown an error for ${scenario.name}`);
                } catch (error) {
                    expect(error.message).to.include('Failed to extract ZIP file');
                }
            }
        });

        it('should handle malformed CSV data gracefully', () => {
            const malformedCSVs = [
                { name: 'unbalanced quotes', content: 'id,name\n1,"unclosed quote\n2,valid' },
                { name: 'mixed line endings', content: 'id,name\r\n1,test\n2,test2\r3,test3' },
                { name: 'extra commas', content: 'id,name,\n1,test,\n2,test2,extra,' },
                { name: 'unicode characters', content: 'id,name\n1,café\n2,naïve\n3,résumé' },
                { name: 'very long lines', content: `id,description\n1,"${'x'.repeat(10000)}"` }
            ];

            malformedCSVs.forEach(scenario => {
                const result = fileProcessing.parseCSV(scenario.content);
                expect(result).to.be.an('array');
                // Should not throw, but might return empty or partial results
            });
        });

        it('should handle extreme memory conditions', async () => {
            const originalMemoryUsage = process.memoryUsage;
            
            // Mock extremely high memory usage
            process.memoryUsage = () => ({
                heapUsed: 1800 * 1024 * 1024, // 1.8GB
                heapTotal: 2048 * 1024 * 1024,
                external: 100 * 1024 * 1024,
                arrayBuffers: 50 * 1024 * 1024
            });

            // Create a large CSV content
            const largeCSVContent = 'id,data\n' + 
                Array.from({length: 1000}, (_, i) => `${i},"${'x'.repeat(1000)}"`).join('\n');

            const result = fileProcessing.parseCSV(largeCSVContent);
            expect(result).to.be.an('array');
            expect(result.length).to.equal(1000);

            // Restore original function
            process.memoryUsage = originalMemoryUsage;
        });

        it('should handle concurrent file processing', async () => {
            const csvContent = 'id,name\n1,test\n2,test2';
            const zipBuffer = createMockZipBuffer('test.csv', Buffer.from(csvContent));
            const base64Data = zipBuffer.toString('base64');

            // Process multiple files concurrently
            const promises = Array.from({length: 10}, () => 
                fileProcessing.extractZipFiles(base64Data)
            );

            const results = await Promise.all(promises);
            
            results.forEach(result => {
                expect(result).to.have.lengthOf(1);
                expect(result[0].filename).to.equal('test.csv');
            });
        });

        it('should handle various character encodings', () => {
            const encodingTests = [
                { name: 'UTF-8 BOM', content: '\ufeffid,name\n1,test' },
                { name: 'special characters', content: 'id,name\n1,café\n2,naïve\n3,résumé' },
                { name: 'emojis', content: 'id,name\n1,😀\n2,🚀\n3,💾' },
                { name: 'mixed languages', content: 'id,name\n1,English\n2,日本語\n3,العربية' }
            ];

            encodingTests.forEach(test => {
                const result = fileProcessing.parseCSV(test.content);
                expect(result).to.be.an('array');
                expect(result.length).to.be.greaterThan(0);
            });
        });

        it('should handle validation edge cases', () => {
            const edgeCases = [
                { row: {}, fields: ['id'], expected: false },
                { row: { id: null }, fields: ['id'], expected: false },
                { row: { id: undefined }, fields: ['id'], expected: false },
                { row: { id: '' }, fields: ['id'], expected: false },
                { row: { id: 0 }, fields: ['id'], expected: true },
                { row: { id: false }, fields: ['id'], expected: true },
                { row: { id: 'valid' }, fields: ['id'], expected: true }
            ];

            edgeCases.forEach(testCase => {
                const result = fileProcessing.validateCSVRow(testCase.row, testCase.fields);
                expect(result.isValid).to.equal(testCase.expected);
            });
        });

        it('should handle CSV parsing limitations gracefully', () => {
            // Test that the parser handles semicolon data as single field (expected behavior)
            const csvContent = 'id;name;email\n1;John;john@example.com\n2;Jane;jane@example.com';
            const result = fileProcessing.parseCSV(csvContent);
            
            expect(result).to.have.lengthOf(2);
            expect(result[0]).to.have.property('id;name;email');
            expect(result[0]['id;name;email']).to.equal('1;John;john@example.com');
        });

        it('should handle resource cleanup after errors', async () => {
            const invalidZip = Buffer.from('definitely-not-a-zip');
            const base64Data = invalidZip.toString('base64');

            // Multiple failed attempts should not leak resources
            for (let i = 0; i < 5; i++) {
                try {
                    await fileProcessing.extractZipFiles(base64Data);
                    expect.fail('Should have thrown an error');
                } catch (error) {
                    expect(error.message).to.include('Failed to extract ZIP file');
                }
            }
            
            // Memory usage should remain stable
            const memoryAfter = process.memoryUsage();
            expect(memoryAfter.heapUsed).to.be.lessThan(100 * 1024 * 1024); // Less than 100MB
        });
    });

    describe('Integration Tests', () => {
        it('should handle CSV with comments and mixed data types', () => {
            const csvContent = `* This CSV contains survey data
* Generated on 2024-01-01
Respondent ID,Age,Score,Comments
1,25,85.5,"Good performance"
2,30,92.0,"Excellent work"
* End of data
3,28,78.2,"Needs improvement"`;

            const result = fileProcessing.parseCSV(csvContent, { convertNumbers: true });

            expect(result).to.have.lengthOf(3);
            expect(result[0]).to.deep.equal({
                'Respondent ID': 1,
                'Age': 25,
                'Score': 85.5,
                'Comments': 'Good performance'
            });
            expect(result[2]['Respondent ID']).to.equal(3);
        });

        it('should validate processed CSV data', () => {
            const csvContent = `id,name,email
1,John,john@example.com
2,,jane@example.com
3,Bob,`;

            const data = fileProcessing.parseCSV(csvContent);
            const requiredFields = ['id', 'name', 'email'];

            const validationResults = data.map(row => 
                fileProcessing.validateCSVRow(row, requiredFields)
            );

            expect(validationResults[0].isValid).to.be.true;
            expect(validationResults[1].isValid).to.be.false;
            expect(validationResults[1].missingFields).to.include('name');
            expect(validationResults[2].isValid).to.be.false;
            expect(validationResults[2].missingFields).to.include('email');
        });

        it('should handle complete workflow: ZIP extraction → CSV parsing → validation', async () => {
            const csvContent = `* DHQ Survey Results Export
* Generated: 2024-01-01
* Total Participants: 3
participant_id,name,email,age,survey_score
1,John Doe,john@example.com,25,85.5
2,Jane Smith,,30,92.0
3,,bob@example.com,28,78.2
* End of export`;

            const zipBuffer = createMockZipBuffer('survey_data.csv', Buffer.from(csvContent));
            const base64Data = zipBuffer.toString('base64');

            // Step 1: Extract files from ZIP
            const extractedFiles = await fileProcessing.extractZipFiles(base64Data);
            
            expect(extractedFiles).to.have.lengthOf(1);
            expect(extractedFiles[0].filename).to.equal('survey_data.csv');

            // Step 2: Parse the CSV content
            const csvData = fileProcessing.parseCSV(extractedFiles[0].content.toString(), { 
                convertNumbers: true 
            });
            
            expect(csvData).to.have.lengthOf(3);
            expect(csvData[0]).to.deep.equal({
                participant_id: 1,
                name: 'John Doe',
                email: 'john@example.com',
                age: 25,
                survey_score: 85.5
            });

            // Step 3: Validate the parsed data
            const requiredFields = ['participant_id', 'name', 'email'];
            const validationResults = csvData.map(row => 
                fileProcessing.validateCSVRow(row, requiredFields)
            );

            expect(validationResults[0].isValid).to.be.true;
            expect(validationResults[1].isValid).to.be.false;
            expect(validationResults[1].missingFields).to.deep.equal(['email']);
            expect(validationResults[2].isValid).to.be.false;
            expect(validationResults[2].missingFields).to.deep.equal(['name']);

            const validRows = validationResults.filter(r => r.isValid).length;
            const invalidRows = validationResults.filter(r => !r.isValid).length;
            
            expect(validRows).to.equal(1);
            expect(invalidRows).to.equal(2);
        });

        it('should handle ZIP with multiple CSV files and process each', async () => {
            const csvContent1 = `id,name,score
1,Alice,95
2,Bob,88`;

            const csvContent2 = `id,department,budget
1,Engineering,50000
2,Marketing,30000`;

            const zipBuffer = createMockZipBuffer([
                { filename: 'participants.csv', content: Buffer.from(csvContent1) },
                { filename: 'departments.csv', content: Buffer.from(csvContent2) }
            ]);
            const base64Data = zipBuffer.toString('base64');

            const extractedFiles = await fileProcessing.extractZipFiles(base64Data);
            
            expect(extractedFiles).to.have.lengthOf(2);

            const processedData = {};
            for (const file of extractedFiles) {
                const parsedData = fileProcessing.parseCSV(file.content.toString(), { 
                    convertNumbers: true 
                });
                processedData[file.filename] = parsedData;
            }

            expect(processedData['participants.csv']).to.have.lengthOf(2);
            expect(processedData['participants.csv'][0]).to.deep.equal({
                id: 1,
                name: 'Alice',
                score: 95
            });

            expect(processedData['departments.csv']).to.have.lengthOf(2);
            expect(processedData['departments.csv'][0]).to.deep.equal({
                id: 1,
                department: 'Engineering',
                budget: 50000
            });
        });
    });

    // Create a mock ZIP buffer (supports single file or multiple files)
    function createMockZipBuffer(input, content = null, compressionMethod = 0) {
        const EOCD_SIGNATURE = 0x06054b50;
        const CD_SIGNATURE = 0x02014b50;
        const LFH_SIGNATURE = 0x04034b50;

        // Handle both single file and multiple files
        const files = Array.isArray(input) ? input : [{ filename: input, content, compressionMethod }];
        
        // Ensure each file has a compressionMethod property
        files.forEach(file => {
            if (!file.hasOwnProperty('compressionMethod')) {
                file.compressionMethod = 0;
            }
        });
        
        let localHeaderOffset = 0;
        const fileEntries = [];
        const centralDirEntries = [];

        for (const file of files) {
            const filenameBuffer = Buffer.from(file.filename, 'utf8');
            const actualCompressionMethod = file.compressionMethod || 0;
            const fileData = (actualCompressionMethod === 0) ? file.content : Buffer.from('compressed-data');

            // Local File Header
            const lfh = Buffer.alloc(30 + filenameBuffer.length);
            lfh.writeUInt32LE(LFH_SIGNATURE, 0);
            lfh.writeUInt16LE(20, 4);
            lfh.writeUInt16LE(0, 6);
            lfh.writeUInt16LE(actualCompressionMethod, 8);
            lfh.writeUInt32LE(fileData.length, 18);
            lfh.writeUInt32LE(file.content.length, 22);
            lfh.writeUInt16LE(filenameBuffer.length, 26);
            lfh.writeUInt16LE(0, 28);
            filenameBuffer.copy(lfh, 30);

            // Central Directory Header
            const cdh = Buffer.alloc(46 + filenameBuffer.length);
            cdh.writeUInt32LE(CD_SIGNATURE, 0);
            cdh.writeUInt16LE(actualCompressionMethod, 10);
            cdh.writeUInt32LE(fileData.length, 20);
            cdh.writeUInt32LE(file.content.length, 24);
            cdh.writeUInt16LE(filenameBuffer.length, 28);
            cdh.writeUInt16LE(0, 30);
            cdh.writeUInt16LE(0, 32);
            cdh.writeUInt32LE(localHeaderOffset, 42);
            filenameBuffer.copy(cdh, 46);

            fileEntries.push(Buffer.concat([lfh, fileData]));
            centralDirEntries.push(cdh);
            localHeaderOffset += lfh.length + fileData.length;
        }

        const centralDirSize = centralDirEntries.reduce((sum, entry) => sum + entry.length, 0);

        // End of Central Directory
        const eocd = Buffer.alloc(22);
        eocd.writeUInt32LE(EOCD_SIGNATURE, 0);
        eocd.writeUInt16LE(files.length, 10);
        eocd.writeUInt32LE(centralDirSize, 12);
        eocd.writeUInt32LE(localHeaderOffset, 16);

        return Buffer.concat([
            ...fileEntries,
            ...centralDirEntries,
            eocd
        ]);
    }
});
