/**
 * Test Constants
 * Centralized constants for current hardcoded values in tests
 */

const TEST_CONSTANTS = {
    // Common values
    STUDY_IDS: {
        DEFAULT: 'study_123',
        SECONDARY: 'study_456'
    },

    PARTICIPANT_IDS: {
        DEFAULT: 'participant1',
        SECOND: 'participant2', 
        THIRD: 'participant3',
        FOURTH: 'participant4',
        NUMBERED: (n) => `participant${n}`
    },

    // Collection paths
    COLLECTIONS: {
        PARTICIPANTS: 'participants',
        DHQ_CREDENTIALS: 'dhq3SurveyCredentials',
        RESPONSE_TRACKING: 'responseTracking',
        AVAILABLE_CREDENTIALS: 'availableCredentials'
    },

    // Common document IDs (not Firestore auto-generated)
    DOCS: {
        ANALYSIS_RESULTS: 'analysisResults',
        DHQ_ANALYSIS_RESULTS: 'dhqAnalysisResults',
        CONNECT_APP: 'connectApp'
    },

    // Test environment values
    ENV: {
        TEST_TOKEN: 'test-token',
        NODE_ENV: 'test'
    },

    // Common file names in tests
    FILES: {
        SURVEY_DATA_CSV: 'survey_data.csv',
        TEST_TXT: 'test.txt'
    }
};

module.exports = TEST_CONSTANTS;