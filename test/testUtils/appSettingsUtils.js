/**
 * App Settings Generation Utility
 * Provides utility for creating mock app settings data
 */

function createMockAppSettings() {
    return {
        /**
         * Create app settings config
         * @param {Object} options - Configuration options
         * @param {Array} options.dhqStudyIDs - Array of study IDs (default: ['study_123', 'study_456'])
         * @param {Array} options.dhqDepletedCredentials - Array of depleted study IDs (default: [])
         * @param {number} options.lookbackDays - Lookback days (default: 30)
         * @param {number} options.lowCredentialWarningThreshold - Warning threshold (default: 1000)
         * @param {Object} overrides - Additional settings to override defaults
         * @returns {Object} Mock app settings
         */
        createAppSettings: (options = {}, overrides = {}) => {
            const {
                dhqStudyIDs = ['study_123', 'study_456'],
                dhqDepletedCredentials = [],
                lookbackDays = 30,
                lowCredentialWarningThreshold = 1000
            } = options;

            return {
                appName: 'connectApp',
                dhq: {
                    dhqStudyIDs,
                    lookbackDays,
                    lowCredentialWarningThreshold,
                    dhqDepletedCredentials,
                    ...overrides.dhq
                },
                ...overrides
            };
        }
    };
}

module.exports = {
    createMockAppSettings
};
