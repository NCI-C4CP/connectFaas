const fieldMapping = require('../../utils/fieldToConceptIdMapping');

/**
 * Participant data generation utility for creating mock participant data
 */

function createMockParticipantData() {
    return {
      /**
       * Mock a 'not started' DHQ participant
       * @param {string} uid - User ID
       * @param {Object} overrides - Additional data to override defaults
       * @returns {Object} Mock participant data
       */
      createNotStartedDHQParticipant: (uid, overrides = {}) => ({
        state: { uid },
        [fieldMapping.dhq3StudyID]: 'study_123',
        [fieldMapping.dhq3Username]: `user_${uid}`,
        [fieldMapping.dhq3UUID]: `uuid_${uid}`,
        [fieldMapping.dhq3SurveyStatus]: fieldMapping.notStarted,
        [fieldMapping.dhq3SurveyStatusExternal]: fieldMapping.dhq3NotYetBegun,
        ...overrides,
      }),

      /**
       * Mock a 'started' DHQ participant
       * @param {string} uid - User ID
       * @param {Object} overrides - Additional data to override defaults
       * @returns {Object} Mock participant data
       */
      createStartedDHQParticipant: (uid, overrides = {}) => ({
        state: { uid },
        [fieldMapping.dhq3StudyID]: 'study_123',
        [fieldMapping.dhq3Username]: `user_${uid}`,
        [fieldMapping.dhq3UUID]: `uuid_${uid}`,
        [fieldMapping.dhq3SurveyStatus]: fieldMapping.started,
        [fieldMapping.dhq3SurveyStatusExternal]: fieldMapping.dhq3InProgress,
        [fieldMapping.dhq3SurveyStartTime]: new Date().toISOString(),
        ...overrides,
      }),

      /**
       * Mock a 'completed' DHQ participant
       * @param {string} uid - User ID
       * @param {Object} overrides - Additional data to override defaults
       * @returns {Object} Mock participant data
       */
      createCompletedDHQParticipant: (uid, overrides = {}) => ({
        state: { uid },
        [fieldMapping.dhq3StudyID]: 'study_123',
        [fieldMapping.dhq3Username]: `user_${uid}`,
        [fieldMapping.dhq3SurveyStatus]: fieldMapping.submitted,
        [fieldMapping.dhq3SurveyStatusExternal]: fieldMapping.dhq3Completed,
        [fieldMapping.dhq3SurveyStartTime]: new Date(Date.now() - 86400000).toISOString(),
        [fieldMapping.dhq3SurveyCompletionTime]: new Date().toISOString(),
        [fieldMapping.dhq3Language]: fieldMapping.english,
        [fieldMapping.dhq3HEIReportStatusInternal]: fieldMapping.reportStatus.unread,
        [fieldMapping.dhq3HEIReportStatusExternal]: fieldMapping.reportStatus.unread,
        ...overrides,
      }),
    };
}


module.exports = {
    createMockParticipantData
};
