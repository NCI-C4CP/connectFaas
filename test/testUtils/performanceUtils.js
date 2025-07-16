/**
 * Performance Data Generation Utilities
 * Provides utilities for creating mock performance data and metrics
 */

function createMockPerformanceData() {
    return {
        /**
         * Mock memory usage data
         * @param {number} heapUsedMB - Heap used in MB
         * @returns {Object} Mock memory usage
         */
        createMemoryUsage: (heapUsedMB = 500) => ({
            heapUsed: heapUsedMB * 1024 * 1024,
            heapTotal: 2048 * 1024 * 1024,
            external: 0,
            rss: 2048 * 1024 * 1024
        }),

        /**
         * Mock processing metrics
         * @param {number} totalItems - Total items processed
         * @param {number} successCount - Successful items
         * @param {number} errorCount - Error count
         * @param {number} processingTimeMs - Processing time in milliseconds
         * @returns {Object} Mock processing metrics
         */
        createProcessingMetrics: (totalItems, successCount, errorCount, processingTimeMs) => ({
            totalItems,
            successCount,
            errorCount,
            processingTimeMs,
            itemsPerSecond: (totalItems / processingTimeMs) * 1000,
            successRate: totalItems > 0 ? (successCount / totalItems) * 100 : 0,
            errorRate: totalItems > 0 ? (errorCount / totalItems) * 100 : 0,
            hasErrors: errorCount > 0
        })
    };
}


module.exports = {
    createMockPerformanceData
};
