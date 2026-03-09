/**
 * Test Setup Helpers: Reusable test setup functions and patterns
 */

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
    global.fetch = vi.fn();

    // Set up Firebase mocks with per-test-file isolation
    const mockSystem = createFirebaseMocks({
        setupConsole: true,
        setupModuleMocks: true,
        isolatePerTestFile: true,
        ...options
    });

    // Clean up after each test
    afterEach(() => {
        vi.restoreAllMocks();
        mockSystem.factory.reset();
        // Clear document registry for fresh state
        if (mockSystem.factory.clearDocumentRegistry) {
            mockSystem.factory.clearDocumentRegistry();
        }
        // Clear any global fetch mock
        if (global.fetch && vi.isMockFunction(global.fetch)) {
            global.fetch.mockReset();
        }
        // Reset process.env if needed
        if (process.env.NODE_ENV === TEST_CONSTANTS.ENV.NODE_ENV) {
            // Keep test environment but clear any test-specific vars
            delete process.env.TEST_SPECIFIC_VAR;
        }
    });

    // Clean up after all tests
    afterAll(() => {
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
    if (expected.documentCount !== undefined) {
        expect(result.documents).toHaveLength(expected.documentCount);
    }
    if (expected.respondentCount !== undefined) {
        expect(result.respondentIds).toHaveLength(expected.respondentCount);
    }
    if (expected.skippedCount !== undefined) {
        expect(result.skippedCount).toBe(expected.skippedCount);
    }
    if (expected.expectedIds && expected.expectedIds.length > 0) {
        expect(result.respondentIds).toEqual(expected.expectedIds);
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
 * @returns {Object} Spy or existing mock
 */
function createConsoleSafeStub(method) {
    if (!vi.isMockFunction(console[method])) {
        return vi.spyOn(console, method).mockImplementation(() => {});
    }
    return console[method];
}

/**
 * Validates test environment state for isolation
 * @returns {Object} State validation results
 */
function validateTestState() {
    const issues = [];

    // Check for lingering mocks
    if (vi.isMockFunction(console.log)) issues.push('console.log still mocked');
    if (vi.isMockFunction(console.warn)) issues.push('console.warn still mocked');
    if (vi.isMockFunction(console.error)) issues.push('console.error still mocked');

    // Check for global pollution
    if (global.fetch && vi.isMockFunction(global.fetch)) {
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
