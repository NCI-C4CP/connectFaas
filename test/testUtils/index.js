/**
 * TestUtils Export: Centralizes testing utilities from modular files
 */

const { createMockCSVData } = require('./csvUtils');
const { createMockParticipantData } = require('./participantUtils');
const { createMockDHQResponses } = require('./dhqApiUtils');
const { createMockErrorScenarios } = require('./errorUtils');
const { createMockPerformanceData } = require('./performanceUtils');
const { createMockAppSettings } = require('./appSettingsUtils');

class TestUtils {
    /**
     * Create mock CSV data for different DHQ report types
     */
    static createMockCSVData() {
        return createMockCSVData();
    }

    /**
     * Mock participant data
     */
    static createMockParticipantData() {
        return createMockParticipantData();
    }


    /**
     * Mock app settings data
     */
    static createMockAppSettings() {
        return createMockAppSettings();
    }

    /**
     * Mock DHQ API responses
     */
    static createMockDHQResponses() {
        return createMockDHQResponses();
    }

    /**
     * Mock error scenarios
     */
    static createMockErrorScenarios() {
        return createMockErrorScenarios();
    }

    /**
     * Mock performance data
     */
    static createMockPerformanceData() {
        return createMockPerformanceData();
    }
}

module.exports = TestUtils;
