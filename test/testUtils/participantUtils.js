const fieldMapping = require('../../utils/fieldToConceptIdMapping');
const TEST_CONSTANTS = require('../constants');

/**
 * Participant data generation utility for creating mock participant data
 */

function createMockParticipantData() {
    return {
      /**
       * Mock a 'not started' DHQ participant
       * @param {string} uid - User ID
       * @param {Object} overrides - Additional data to override defaults
       * @param {Object} overrides.state - Additional state properties (will be merged with { uid })
       * @returns {Object} Mock participant data
       */
      createNotStartedDHQParticipant: (uid, overrides = {}) => {
        const { state: stateOverrides, ...otherOverrides } = overrides;
        return {
          state: { uid, ...stateOverrides },
          [fieldMapping.dhq3StudyID]: TEST_CONSTANTS.STUDY_IDS.DEFAULT,
          [fieldMapping.dhq3Username]: `user_${uid}`,
          [fieldMapping.dhq3UUID]: `uuid_${uid}`,
          [fieldMapping.dhq3SurveyStatus]: fieldMapping.notStarted,
          [fieldMapping.dhq3SurveyStatusExternal]: fieldMapping.dhq3NotYetBegun,
          ...otherOverrides,
        };
      },

      /**
       * Mock a 'started' DHQ participant
       * @param {string} uid - User ID
       * @param {Object} overrides - Additional data to override defaults
       * @param {Object} overrides.state - Additional state properties (will be merged with { uid })
       * @returns {Object} Mock participant data
       */
      createStartedDHQParticipant: (uid, overrides = {}) => {
        const { state: stateOverrides, ...otherOverrides } = overrides;
        return {
          state: { uid, ...stateOverrides },
          [fieldMapping.dhq3StudyID]: TEST_CONSTANTS.STUDY_IDS.DEFAULT,
          [fieldMapping.dhq3Username]: `user_${uid}`,
          [fieldMapping.dhq3UUID]: `uuid_${uid}`,
          [fieldMapping.dhq3SurveyStatus]: fieldMapping.started,
          [fieldMapping.dhq3SurveyStatusExternal]: fieldMapping.dhq3InProgress,
          [fieldMapping.dhq3SurveyStartTime]: new Date().toISOString(),
          ...otherOverrides,
        };
      },

      /**
       * Mock a 'completed' DHQ participant
       * @param {string} uid - User ID
       * @param {Object} overrides - Additional data to override defaults
       * @param {Object} overrides.state - Additional state properties (will be merged with { uid })
       * @returns {Object} Mock participant data
       */
      createCompletedDHQParticipant: (uid, overrides = {}) => {
        const { state: stateOverrides, ...otherOverrides } = overrides;
        return {
          state: { uid, ...stateOverrides },
          [fieldMapping.dhq3StudyID]: TEST_CONSTANTS.STUDY_IDS.DEFAULT,
          [fieldMapping.dhq3Username]: `user_${uid}`,
          [fieldMapping.dhq3SurveyStatus]: fieldMapping.submitted,
          [fieldMapping.dhq3SurveyStatusExternal]: fieldMapping.dhq3Completed,
          [fieldMapping.dhq3SurveyStartTime]: new Date(Date.now() - 86400000).toISOString(),
          [fieldMapping.dhq3SurveyCompletionTime]: new Date().toISOString(),
          [fieldMapping.dhq3Language]: fieldMapping.english,
          [fieldMapping.dhq3HEIReportStatusInternal]: fieldMapping.reportStatus.unread,
          [fieldMapping.dhq3HEIReportStatusExternal]: fieldMapping.reportStatus.unread,
          ...otherOverrides,
        };
      },
    };
}


module.exports = {
    createMockParticipantData
};
