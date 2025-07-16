/**
 * Test Setup Helpers: Reusable test setup functions and patterns
 */

const sinon = require('sinon');
const { createFirebaseMocks } = require('../mocks/firebaseMocks');

/**
 * Sets up the entire test suite: environment, mocks, and cleanup.
 * @param {Object} options - Configuration options for mocks
 * @returns {Object} Mock system (factory, mocks, helper, restore)
 */
function setupTestSuite(options = {}) {
    // Set up test environment variables
    process.env.NODE_ENV = 'test';
    process.env.DHQ_TOKEN = 'test-token';
    global.fetch = sinon.stub();

    // Set up Firebase mocks
    const mockSystem = createFirebaseMocks({
        setupConsole: true,
        setupModuleMocks: true,
        ...options
    });

    // Clean up after each test
    afterEach(() => {
        sinon.restore();
        mockSystem.factory.reset();
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

module.exports = {
    setupTestSuite,
    createTestData,
    assertResult,
    benchmarkFunction
};
