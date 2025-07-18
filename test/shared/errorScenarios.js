/**
 * Error Scenario Testing Utilities
 * Provides comprehensive error testing patterns and scenarios
 */

const sinon = require('sinon');
const { expect } = require('chai');

class ErrorScenarios {
    constructor() {
        this.errorTypes = {
            NETWORK: 'NetworkError',
            TIMEOUT: 'TimeoutError',
            VALIDATION: 'ValidationError',
            PERMISSION: 'PermissionError',
            NOT_FOUND: 'NotFoundError',
            RATE_LIMIT: 'RateLimitError',
            FIRESTORE: 'FirestoreError',
            STORAGE: 'StorageError',
            AUTH: 'AuthError'
        };
    }

    /**
     * Create standard error objects with consistent structure
     */
    createError(type, message, details = {}) {
        const error = new Error(message);
        error.name = type;
        error.code = details.code || 'UNKNOWN';
        error.statusCode = details.statusCode || 500;
        error.details = details;
        error.timestamp = new Date().toISOString();
        return error;
    }

    /**
     * Network-related errors
     */
    createNetworkError(message = 'Network request failed', details = {}) {
        return this.createError(this.errorTypes.NETWORK, message, {
            code: 'NETWORK_ERROR',
            statusCode: 503,
            ...details
        });
    }

    /**
     * Timeout errors
     */
    createTimeoutError(message = 'Request timeout', details = {}) {
        return this.createError(this.errorTypes.TIMEOUT, message, {
            code: 'TIMEOUT',
            statusCode: 408,
            timeout: details.timeout || 5000,
            ...details
        });
    }

    /**
     * Validation errors
     */
    createValidationError(field, message, details = {}) {
        return this.createError(this.errorTypes.VALIDATION, message, {
            code: 'VALIDATION_ERROR',
            statusCode: 400,
            field,
            ...details
        });
    }

    /**
     * Permission/Authorization errors
     */
    createPermissionError(message = 'Permission denied', details = {}) {
        return this.createError(this.errorTypes.PERMISSION, message, {
            code: 'PERMISSION_DENIED',
            statusCode: 403,
            ...details
        });
    }

    /**
     * Resource not found errors
     */
    createNotFoundError(resource, message = 'Resource not found', details = {}) {
        return this.createError(this.errorTypes.NOT_FOUND, message, {
            code: 'NOT_FOUND',
            statusCode: 404,
            resource,
            ...details
        });
    }

    /**
     * Rate limiting errors
     */
    createRateLimitError(message = 'Rate limit exceeded', details = {}) {
        return this.createError(this.errorTypes.RATE_LIMIT, message, {
            code: 'RATE_LIMIT_EXCEEDED',
            statusCode: 429,
            retryAfter: details.retryAfter || 60,
            ...details
        });
    }

    /**
     * Firestore-specific errors
     */
    createFirestoreError(message = 'Firestore operation failed', details = {}) {
        return this.createError(this.errorTypes.FIRESTORE, message, {
            code: details.code || 'FIRESTORE_ERROR',
            statusCode: details.statusCode || 500,
            ...details
        });
    }

    /**
     * Storage-specific errors
     */
    createStorageError(message = 'Storage operation failed', details = {}) {
        return this.createError(this.errorTypes.STORAGE, message, {
            code: details.code || 'STORAGE_ERROR',
            statusCode: details.statusCode || 500,
            ...details
        });
    }

    /**
     * Authentication errors
     */
    createAuthError(message = 'Authentication failed', details = {}) {
        return this.createError(this.errorTypes.AUTH, message, {
            code: 'AUTH_ERROR',
            statusCode: 401,
            ...details
        });
    }

    /**
     * DHQ API specific errors
     */
    createDHQAPIError(statusCode, message, details = {}) {
        return this.createError('DHQAPIError', message, {
            code: 'DHQ_API_ERROR',
            statusCode,
            ...details
        });
    }

    /**
     * Test helper to verify error properties
     */
    verifyError(error, expectedType, expectedMessage, expectedDetails = {}) {
        expect(error).to.be.an.instanceof(Error);
        expect(error.name).to.equal(expectedType);
        expect(error.message).to.equal(expectedMessage);
        
        if (expectedDetails.code) {
            expect(error.code).to.equal(expectedDetails.code);
        }
        
        if (expectedDetails.statusCode) {
            expect(error.statusCode).to.equal(expectedDetails.statusCode);
        }
        
        expect(error.timestamp).to.be.a('string');
    }

    /**
     * Test helper for async error scenarios
     */
    async testAsyncError(asyncFunction, expectedError, ...args) {
        try {
            await asyncFunction(...args);
            expect.fail('Function should have thrown an error');
        } catch (error) {
            this.verifyError(error, expectedError.name, expectedError.message, expectedError.details);
        }
    }

    /**
     * Test helper for synchronous error scenarios
     */
    testSyncError(syncFunction, expectedError, ...args) {
        try {
            syncFunction(...args);
            expect.fail('Function should have thrown an error');
        } catch (error) {
            this.verifyError(error, expectedError.name, expectedError.message, expectedError.details);
        }
    }

    /**
     * Create error scenarios for Firebase operations
     */
    createFirebaseErrorScenarios() {
        return {
            // Firestore scenarios
            firestorePermissionDenied: this.createFirestoreError('Permission denied', {
                code: 'permission-denied',
                statusCode: 403
            }),
            firestoreNotFound: this.createFirestoreError('Document not found', {
                code: 'not-found',
                statusCode: 404
            }),
            firestoreUnavailable: this.createFirestoreError('Service unavailable', {
                code: 'unavailable',
                statusCode: 503
            }),
            firestoreDeadlineExceeded: this.createFirestoreError('Deadline exceeded', {
                code: 'deadline-exceeded',
                statusCode: 504
            }),

            // Auth scenarios
            authInvalidToken: this.createAuthError('Invalid token', {
                code: 'invalid-token',
                statusCode: 401
            }),
            authTokenExpired: this.createAuthError('Token expired', {
                code: 'token-expired',
                statusCode: 401
            }),
            authUserNotFound: this.createAuthError('User not found', {
                code: 'user-not-found',
                statusCode: 404
            }),

            // Storage scenarios
            storageNotFound: this.createStorageError('File not found', {
                code: 'not-found',
                statusCode: 404
            }),
            storagePermissionDenied: this.createStorageError('Permission denied', {
                code: 'permission-denied',
                statusCode: 403
            }),
            storageQuotaExceeded: this.createStorageError('Quota exceeded', {
                code: 'quota-exceeded',
                statusCode: 413
            })
        };
    }

    /**
     * Create error scenarios for DHQ operations
     */
    createDHQErrorScenarios() {
        return {
            invalidCredentials: this.createDHQAPIError(401, 'Invalid credentials'),
            studyNotFound: this.createDHQAPIError(404, 'Study not found'),
            participantNotFound: this.createDHQAPIError(404, 'Participant not found'),
            dataValidationError: this.createValidationError('surveyData', 'Invalid survey data format'),
            rateLimitExceeded: this.createRateLimitError('DHQ API rate limit exceeded'),
            serverError: this.createDHQAPIError(500, 'Internal server error'),
            badRequest: this.createDHQAPIError(400, 'Bad request format'),
            serviceUnavailable: this.createDHQAPIError(503, 'DHQ service unavailable')
        };
    }

    /**
     * Create error scenarios for file processing
     */
    createFileProcessingErrorScenarios() {
        return {
            invalidZipFile: this.createError('ZipError', 'Invalid ZIP file format'),
            corruptedData: this.createError('CorruptionError', 'Data corruption detected'),
            unsupportedFormat: this.createError('FormatError', 'Unsupported file format'),
            fileTooLarge: this.createError('SizeError', 'File size exceeds limit'),
            memoryError: this.createError('MemoryError', 'Insufficient memory for processing'),
            csvParseError: this.createError('CSVError', 'CSV parsing failed'),
            encodingError: this.createError('EncodingError', 'Character encoding not supported')
        };
    }

    /**
     * Setup error mocking for Firebase operations
     */
    setupFirebaseErrorMocking(mocks) {
        const scenarios = this.createFirebaseErrorScenarios();
        
        return {
            mockFirestoreError: (operation, errorType) => {
                const error = scenarios[errorType];
                if (!error) {
                    throw new Error(`Unknown error type: ${errorType}`);
                }
                
                switch (operation) {
                    case 'get':
                        mocks.firestore.collection().doc().get.rejects(error);
                        break;
                    case 'set':
                        mocks.firestore.collection().doc().set.rejects(error);
                        break;
                    case 'update':
                        mocks.firestore.collection().doc().update.rejects(error);
                        break;
                    case 'delete':
                        mocks.firestore.collection().doc().delete.rejects(error);
                        break;
                    default:
                        throw new Error(`Unknown operation: ${operation}`);
                }
            },
            
            mockAuthError: (operation, errorType) => {
                const error = scenarios[errorType];
                if (!error) {
                    throw new Error(`Unknown error type: ${errorType}`);
                }
                
                mocks.auth[operation].rejects(error);
            },
            
            mockStorageError: (operation, errorType) => {
                const error = scenarios[errorType];
                if (!error) {
                    throw new Error(`Unknown error type: ${errorType}`);
                }
                
                switch (operation) {
                    case 'download':
                        mocks.storage.bucket().file().download.rejects(error);
                        break;
                    case 'upload':
                        mocks.storage.bucket().upload.rejects(error);
                        break;
                    default:
                        throw new Error(`Unknown operation: ${operation}`);
                }
            }
        };
    }

    /**
     * Generate comprehensive error test suite
     */
    generateErrorTestSuite(moduleName, testFunction, scenarios) {
        return () => {
            describe(`${moduleName} - Error Scenarios`, () => {
                scenarios.forEach(scenario => {
                    it(`should handle ${scenario.name}`, async () => {
                        if (scenario.async) {
                            await this.testAsyncError(testFunction, scenario.error, ...scenario.args);
                        } else {
                            this.testSyncError(testFunction, scenario.error, ...scenario.args);
                        }
                    });
                });
            });
        };
    }
}

module.exports = ErrorScenarios;