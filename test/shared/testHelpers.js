/**
 * Test Setup Helpers: Reusable test setup functions and patterns
 */

const sinon = require('sinon');
const { createFirebaseMocks } = require('../mocks/mockFactory.js');
const TEST_CONSTANTS = require('../constants');

/**
 * Sets up the entire test suite: environment, mocks, and cleanup.
 * @param {Object} options - Configuration options for mocks
 * @returns {Object} Mock system (factory, mocks, helper, restore)
 */
function setupTestSuite(options = {}) {
    // Set up test environment variables
    process.env.NODE_ENV = TEST_CONSTANTS.ENV.NODE_ENV;
    process.env.DHQ_TOKEN = TEST_CONSTANTS.ENV.TEST_TOKEN;
    global.fetch = sinon.stub();

    // Set up Firebase mocks with per-test-file isolation
    const mockSystem = createFirebaseMocks({
        setupConsole: true,
        setupModuleMocks: true,
        isolatePerTestFile: true,
        ...options
    });

    // Clean up after each test
    afterEach(() => {
        sinon.restore();
        mockSystem.factory.reset();
        // Clear document registry for fresh state
        if (mockSystem.factory.clearDocumentRegistry) {
            mockSystem.factory.clearDocumentRegistry();
        }
        // Clear any global fetch stub
        if (global.fetch && global.fetch.restore) {
            global.fetch.restore();
        }
        // Reset process.env if needed
        if (process.env.NODE_ENV === TEST_CONSTANTS.ENV.NODE_ENV) {
            // Keep test environment but clear any test-specific vars
            delete process.env.TEST_SPECIFIC_VAR;
        }
    });

    // Clean up after all tests
    after(() => {
        mockSystem.restore();
    });

    return mockSystem;
}

/**
 * Helper to create consistent test data
 * @param {number} count - Number of items to create
 * @param {Function} factory - Factory function to create each item
 * @returns {Array} Array of test data
 */
function createTestData(count, factory) {
    return Array.from({ length: count }, (_, i) => factory(i));
}

/**
 * Assertion helper for collection/processing results
 * @param {Object} result - Result to assert
 * @param {Object} expected - Expected values (documentCount, respondentCount, skippedCount, expectedIds)
 */
function assertResult(result, expected) {
    const { expect } = require('chai');
    if (expected.documentCount !== undefined) {
        expect(result.documents).to.have.length(expected.documentCount);
    }
    if (expected.respondentCount !== undefined) {
        expect(result.respondentIds).to.have.length(expected.respondentCount);
    }
    if (expected.skippedCount !== undefined) {
        expect(result.skippedCount).to.equal(expected.skippedCount);
    }
    if (expected.expectedIds && expected.expectedIds.length > 0) {
        expect(result.respondentIds).to.deep.equal(expected.expectedIds);
    }
}


/**
 * Helper to run performance benchmarks
 * @param {Function} testFunction - Function to benchmark
 * @param {string} testName - Name of the test
 * @returns {Object} Performance results
 */
function benchmarkFunction(testFunction, testName = 'Test') {
    const startTime = Date.now();
    const result = testFunction();
    const endTime = Date.now();
    const processingTime = endTime - startTime;
    
    return {
        result,
        processingTime,
        testName
    };
}

/**
 * Creates a safe console stub that won't conflict with existing stubs
 * @param {string} method - Console method name ('log', 'warn', 'error')
 * @returns {Object} Stub or existing proxy
 */
function createConsoleSafeStub(method) {
    if (!console[method].isSinonProxy) {
        return sinon.stub(console, method);
    }
    return console[method];
}

/**
 * Validates test environment state for isolation
 * @returns {Object} State validation results
 */
function validateTestState() {
    const issues = [];
    
    // Check for lingering stubs
    if (console.log.isSinonProxy) issues.push('console.log still stubbed');
    if (console.warn.isSinonProxy) issues.push('console.warn still stubbed');
    if (console.error.isSinonProxy) issues.push('console.error still stubbed');
    
    // Check for global pollution
    if (global.fetch && typeof global.fetch.restore === 'function') {
        issues.push('global.fetch not properly restored');
    }
    
    // Check environment variables
    if (process.env.NODE_ENV !== TEST_CONSTANTS.ENV.NODE_ENV) {
        issues.push(`NODE_ENV is ${process.env.NODE_ENV}, expected ${TEST_CONSTANTS.ENV.NODE_ENV}`);
    }
    
    return {
        clean: issues.length === 0,
        issues
    };
}

module.exports = {
    setupTestSuite,
    createTestData,
    assertResult,
    benchmarkFunction,
    createConsoleSafeStub,
    validateTestState
};
