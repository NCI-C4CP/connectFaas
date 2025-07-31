/**
 * CSV Data Generation Utilities
 * Provides utilities for creating mock CSV data for different DHQ report types
 */

function createMockCSVData() {
    return {
        /**
         * Mock Analysis Results CSV
         * @param {number} count - Number of participants
         * @returns {string} CSV content
         */
        createAnalysisResultsCSV: (count = 3) => {
            const header = 'Respondent ID,Energy,Protein,Carbs,Fat';
            const rows = Array.from({length: count}, (_, i) => 
                `user${i + 1},${2000 + i * 100},${50 + i * 5},${250 + i * 10},${70 + i * 3}`
            );
            return [header, ...rows].join('\n');
        },

        /**
         * Mock Detailed Analysis CSV
         * @param {number} count - Number of participants
         * @returns {string} CSV content
         */
        createDetailedAnalysisCSV: (count = 3) => {
            const header = 'Respondent ID,Question ID,Food ID,Answer';
            const rows = [];
            for (let i = 1; i <= count; i++) {
                rows.push(`user${i},Q001,FOOD001,Yes`);
                rows.push(`user${i},Q002,FOOD002,2 cups`);
                rows.push(`user${i},Q003,FOOD003,.`); // Missing answer
            }
            return [header, ...rows].join('\n');
        },

        /**
         * Mock Raw Answers CSV
         * @param {number} count - Number of participants
         * @returns {string} CSV content
         */
        createRawAnswersCSV: (count = 3) => {
            const header = 'Respondent Login ID,Question ID,Answer';
            const rows = [];
            for (let i = 1; i <= count; i++) {
                rows.push(`user${i},Q001,Yes`);
                rows.push(`user${i},Q002,2 cups`);
                rows.push(`user${i},Q003,.`); // Missing answer
            }
            return [header, ...rows].join('\n');
        }
    };
}

module.exports = {
    createMockCSVData
};
