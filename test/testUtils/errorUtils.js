/**
 * Error Scenario Generation Utilities
 * Utilities for creating different types of errors for testing
 */

function createMockErrorScenarios() {
    return {
        /**
         * Mock network error
         * @param {string} message - Error message
         * @returns {Error} Mock network error
         */
        createNetworkError: (message = 'Network timeout') => new Error(message),

        /**
         * Mock DHQ API error
         * @param {number} status - HTTP status code
         * @param {string} message - Error message
         * @returns {Error} Mock DHQ API error
         */
        createDHQAPIError: (status = 500, message = 'DHQ API Error') => {
            const error = new Error(`DHQ API Error ${status}: ${message}`);
            error.status = status;
            return error;
        },

        /**
         * Mock validation error
         * @param {string} field - Field name
         * @param {string} message - Error message
         * @returns {Error} Mock validation error
         */
        createValidationError: (field, message) => {
            const error = new Error(`Validation error for ${field}: ${message}`);
            error.field = field;
            return error;
        }
    };
}


module.exports = {
    createMockErrorScenarios
};
