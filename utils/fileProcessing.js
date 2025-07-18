/**
 * Extracts files from a ZIP archive. Returns an array of file objects.
 * @param {string} base64Data - Base64 encoded ZIP file data.
 * @returns {Promise<Array>} - Array of file objects with { filename, content, size, compressedSize }
 * 
 * FILE SIZE WARNING (Cloud):
 *   Cloud Function will crash if the zip file or its contents exceed the memory allocation.
 */
const extractZipFiles = async (base64Data) => {
    const EOCD_SIGNATURE = 0x06054b50;  // End of Central Directory
    const CD_SIGNATURE = 0x02014b50;    // Central Directory

    try {
        const buffer = Buffer.from(base64Data, 'base64');
        
        // Find the End of Central Directory record.
        let eocdOffset = -1;
        const maxScanLen = Math.min(buffer.length, 65535 + 22);
        for (let i = buffer.length - 22; i >= buffer.length - maxScanLen; i--) {
            if (buffer.readUInt32LE(i) === EOCD_SIGNATURE) {
                eocdOffset = i;
                break;
            }
        }

        if (eocdOffset === -1) {
            throw new Error('Invalid ZIP file: End of Central Directory record not found.');
        }

        // Get the total # of entries in the Central Directory and get the offset.
        const totalEntries = buffer.readUInt16LE(eocdOffset + 10);
        const centralDirectoryOffset = buffer.readUInt32LE(eocdOffset + 16);

        const files = [];
        let currentCdOffset = centralDirectoryOffset;

        for (let i = 0; i < totalEntries; i++) {
            if (buffer.readUInt32LE(currentCdOffset) !== CD_SIGNATURE) {
                throw new Error(`Invalid Central Directory entry at offset ${currentCdOffset}`);
            }

            const compressionMethod = buffer.readUInt16LE(currentCdOffset + 10);
            const compressedSize = buffer.readUInt32LE(currentCdOffset + 20);
            const uncompressedSize = buffer.readUInt32LE(currentCdOffset + 24);
            const filenameLength = buffer.readUInt16LE(currentCdOffset + 28);
            const extraFieldLength = buffer.readUInt16LE(currentCdOffset + 30);
            const fileCommentLength = buffer.readUInt16LE(currentCdOffset + 32);
            const localHeaderOffset = buffer.readUInt32LE(currentCdOffset + 42);
            const filename = buffer.toString('utf8', currentCdOffset + 46, currentCdOffset + 46 + filenameLength);
            
            const nextEntryOffset = currentCdOffset + 46 + filenameLength + extraFieldLength + fileCommentLength;

            if (filename.endsWith('/')) {
                currentCdOffset = nextEntryOffset;
                continue;
            }

            const lfhFilenameLength = buffer.readUInt16LE(localHeaderOffset + 26);
            const lfhExtraFieldLength = buffer.readUInt16LE(localHeaderOffset + 28);
            const dataStart = localHeaderOffset + 30 + lfhFilenameLength + lfhExtraFieldLength;
            const compressedData = buffer.slice(dataStart, dataStart + compressedSize);

            let content;
            if (compressionMethod === 0) {
                content = compressedData;

            } else if (compressionMethod === 8) {
                try {
                    const zlib = require('zlib');
                    content = zlib.inflateRawSync(compressedData);
                } catch (error) {
                    console.warn(`Failed to decompress ${filename} with DEFLATE:`, error.message);
                    content = compressedData;
                }

            } else {
                console.warn(`Unsupported compression method ${compressionMethod} for file ${filename}.`);
                currentCdOffset = nextEntryOffset;
                continue;
            }

            if (content.length !== uncompressedSize) {
                console.warn(`Uncompressed size mismatch for ${filename}. Expected ${uncompressedSize}, got ${content.length}.`);
            }

            files.push({
                filename,
                content,
                size: uncompressedSize,
                compressedSize: compressedSize,
            });

            currentCdOffset = nextEntryOffset;
        }

        return files;

    } catch (error) {
        console.error('Error extracting ZIP file:', error);
        throw new Error(`Failed to extract ZIP file: ${error.message}`);
    }
};

/**
 * Remove comment lines and prepare for parsing
 * @param {string} csvContent - The raw CSV content as a string
 * @param {string} commentChar - Character that indicates comment lines (default: '*')
 * @returns {string} - Cleaned CSV content
 */
const cleanCSVContent = (csvContent, commentChar = '*') => {
    const lines = csvContent.split('\n');
    
    // Remove lines that start with the comment character
    const cleanedLines = lines.filter(line => {
        const trimmed = line.trim();
        return trimmed !== '' && !trimmed.startsWith(commentChar);
    });
    
    return cleanedLines.join('\n');
};

/**
 * Parse CSV content into an array of objects with headers as keys.
 * @param {string} csvContent - The CSV content as a string.
 * @param {Object} options - Parsing options obj with commentChar (default: '*') and convertNumbers (default: false)
 * @returns {Array<Object>} - Array of objects representing CSV rows.
 */
const parseCSV = (csvContent, options = {}) => {
    const { commentChar = '*', convertNumbers = false } = options;
    
    try {
        const cleanedContent = cleanCSVContent(csvContent, commentChar);
        if (!cleanedContent || cleanedContent.trim() === '') {
            return [];
        }

        const rows = [];
        let fields = [];
        let currentField = '';
        let isInQuotes = false;

        for (let i = 0; i < cleanedContent.length; i++) {
            const char = cleanedContent[i];

            if (isInQuotes) {
                if (char === '"') {
                    // Check for an escaped quote ("").
                    if (i + 1 < cleanedContent.length && cleanedContent[i + 1] === '"') {
                        currentField += '"';
                        i++; // Skip the next char (it's part of the escape sequence).
                    } else {
                        // Closing quote.
                        isInQuotes = false;
                    }
                } else {
                    currentField += char;
                }

            } else {
                if (char === ',') {
                    fields.push(currentField);
                    currentField = '';
                } else if (char === '"') {
                    // Opening quote.
                    isInQuotes = true;
                } else if (char === '\n' || char === '\r') {
                    fields.push(currentField);
                    rows.push(fields);
                    fields = [];
                    currentField = '';
                    // Handle CRLF (\r\n): skip the next char if it's \n.
                    if (char === '\r' && i + 1 < cleanedContent.length && cleanedContent[i + 1] === '\n') {
                        i++;
                    }
                } else {
                    currentField += char;
                }
            }
        }

        // Add the last field and row if the file doesn't end with a newline.
        if (currentField || fields.length > 0) {
            fields.push(currentField);
            rows.push(fields);
        }

        // Filter out empty rows.
        const nonEmptyRows = rows.filter(row => row.length > 1 || (row.length === 1 && row[0] !== ''));

        // Handle empty or header-only CSVs.
        if (nonEmptyRows.length < 2) {
            return [];
        }

        const headers = nonEmptyRows[0].map(h => h.trim());
        const data = [];

        for (let i = 1; i < nonEmptyRows.length; i++) {
            const values = nonEmptyRows[i];

            // A trailing newline can create a row with a single empty field.
            if (values.length === 1 && values[0] === '') continue;

            if (values.length !== headers.length) {
                console.warn(`Row ${i + 1} has ${values.length} columns, but header has ${headers.length}. Skipping row.`);
                continue;
            }

            const rowObject = {};

            headers.forEach((header, index) => {
                const value = values[index] || '';
                // Convert to number if convertNumbers is true and the value is numeric
                rowObject[header] = convertNumbers && !isNaN(value) && value.trim() !== '' && value !== '.' 
                    ? Number(value) 
                    : value;
            });
            data.push(rowObject);
        }

        return data;

    } catch (error) {
        console.error('Error parsing CSV:', error);
        throw new Error(`Failed to parse CSV: ${error.message}`);
    }
};

/**
 * Validate if a CSV row has the required fields.
 * @param {Object} row - The CSV row object
 * @param {Array<string>} requiredFields - Array of required field names
 * @returns {Object} - Validation result with isValid boolean and missing fields array
 */
const validateCSVRow = (row, requiredFields) => {
    const missingFields = [];
    
    for (const field of requiredFields) {
        if (row[field] === undefined || row[field] === null || row[field] === '') {
            missingFields.push(field);
        }
    }
    
    return {
        isValid: missingFields.length === 0,
        missingFields: missingFields
    };
};

module.exports = {
    extractZipFiles,
    cleanCSVContent,
    parseCSV,
    validateCSVRow,
};
