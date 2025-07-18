/**
 * DHQ API Response Generation Utilities
 * Utilities for creating mock DHQ API responses
 */

function createMockDHQResponses() {
    return {
        /**
         * Mock respondent info response
         * @param {Object} overrides - Additional data to override defaults
         * @returns {Object} Mock DHQ API response
         */
        createRespondentInfo: (overrides = {}) => ({
            username: 'CCC00002',
            active_status: 1,
            questionnaire_status: 2, // In progress
            status_date: '2024-04-11T16:29:53.601000Z',
            device_used: 'PC',
            browser_used: 'Safari',
            total_time_logged_in: '00:05:38.280738',
            number_of_times_logged_in: 2,
            login_durations: ['00:00:46.688120', '00:04:51.592618'],
            language: 'en',
            viewed_rnr_report: false,
            downloaded_rnr_report: false,
            viewed_hei_report: false,
            downloaded_hei_report: false,
            ...overrides
        }),

        /**
         * Mock completed respondent info
         * @param {Object} overrides - Additional data to override defaults
         * @returns {Object} Mock DHQ API response
         */
        createCompletedRespondentInfo: (overrides = {}) => ({
            username: 'CCC00002',
            active_status: 1,
            questionnaire_status: 3, // Completed
            status_date: '2024-04-11T16:29:53.601000Z',
            device_used: 'PC',
            browser_used: 'Safari',
            total_time_logged_in: '00:15:38.280738',
            number_of_times_logged_in: 3,
            login_durations: ['00:00:46.688120', '00:04:51.592618', '00:10:00.000000'],
            language: 'en',
            viewed_rnr_report: true,
            downloaded_rnr_report: false,
            viewed_hei_report: true,
            downloaded_hei_report: false,
            ...overrides
        })
    };
}

module.exports = {
    createMockDHQResponses
};
