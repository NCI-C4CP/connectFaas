#!/usr/bin/env node

/**
 * Test Metrics and Monitoring Script
 * Provides basic test suite metrics for tracking test health
 */

const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

class TestMetrics {
    constructor() {
        this.results = {
            timestamp: new Date().toISOString(),
            totalTests: 0,
            passing: 0,
            failing: 0,
            duration: 0,
            categories: {},
            coverage: null,
            slowTests: []
        };
    }

    /**
     * Run specific test category and collect metrics
     */
    async runTestCategory(category) {
        return new Promise((resolve, reject) => {
            const startTime = Date.now();
            const testProcess = spawn('npm', ['run', `test:${category}`], {
                stdio: 'pipe',
                shell: true
            });

            let stdout = '';
            let stderr = '';

            testProcess.stdout.on('data', (data) => {
                stdout += data.toString();
            });

            testProcess.stderr.on('data', (data) => {
                stderr += data.toString();
            });

            testProcess.on('close', (code) => {
                const duration = Date.now() - startTime;
                const metrics = this.parseTestOutput(stdout, stderr);
                
                resolve({
                    category,
                    duration,
                    exitCode: code,
                    ...metrics
                });
            });

            testProcess.on('error', reject);
        });
    }

    /**
     * Parse Mocha test output to extract metrics
     */
    parseTestOutput(stdout, stderr) {
        const lines = stdout.split('\n');
        let passing = 0;
        let failing = 0;
        let total = 0;
        const slowTests = [];

        // Parse test results
        lines.forEach(line => {
            if (line.includes('✔')) {
                passing++;
            } else if (line.includes('✖') || line.includes('failing')) {
                failing++;
            }
            
            // Detect slow tests (> 100ms)
            const slowMatch = line.match(/✔.*\((\d+)ms\)/);
            if (slowMatch && parseInt(slowMatch[1]) > 100) {
                slowTests.push({
                    name: line.trim(),
                    duration: parseInt(slowMatch[1])
                });
            }
        });

        total = passing + failing;

        return {
            passing,
            failing,
            total,
            slowTests
        };
    }

    /**
     * Run all test categories and collect comprehensive metrics
     */
    async runAllMetrics() {
        const categories = ['unit', 'integration', 'mocks', 'dhq', 'processing'];
        const categoryResults = {};

        console.log('📊 Collecting test metrics...\n');

        for (const category of categories) {
            try {
                console.log(`Running ${category} tests...`);
                const result = await this.runTestCategory(category);
                categoryResults[category] = result;
                
                console.log(`✅ ${category}: ${result.passing} passing, ${result.failing} failing (${result.duration}ms)`);
            } catch (error) {
                console.error(`❌ Error running ${category} tests:`, error.message);
                categoryResults[category] = {
                    category,
                    error: error.message,
                    passing: 0,
                    failing: 0,
                    total: 0,
                    duration: 0
                };
            }
        }

        // Aggregate results
        this.results.categories = categoryResults;
        this.results.totalTests = Object.values(categoryResults).reduce((sum, cat) => sum + cat.total, 0);
        this.results.passing = Object.values(categoryResults).reduce((sum, cat) => sum + cat.passing, 0);
        this.results.failing = Object.values(categoryResults).reduce((sum, cat) => sum + cat.failing, 0);
        this.results.duration = Object.values(categoryResults).reduce((sum, cat) => sum + cat.duration, 0);

        // Collect slow tests
        this.results.slowTests = Object.values(categoryResults)
            .flatMap(cat => cat.slowTests || [])
            .sort((a, b) => b.duration - a.duration)
            .slice(0, 10); // Top 10 slowest

        return this.results;
    }

    /**
     * Generate test metrics report
     */
    generateReport() {
        const report = `
# Test Metrics Report
Generated: ${this.results.timestamp}

## Summary
- **Total Tests**: ${this.results.totalTests}
- **Passing**: ${this.results.passing} (${((this.results.passing / this.results.totalTests) * 100).toFixed(1)}%)
- **Failing**: ${this.results.failing} (${((this.results.failing / this.results.totalTests) * 100).toFixed(1)}%)
- **Total Duration**: ${this.results.duration}ms

## Category Breakdown
${Object.entries(this.results.categories).map(([category, data]) => `
### ${category.toUpperCase()}
- Tests: ${data.total}
- Passing: ${data.passing}
- Failing: ${data.failing}
- Duration: ${data.duration}ms
- Success Rate: ${data.total > 0 ? ((data.passing / data.total) * 100).toFixed(1) : 0}%
`).join('')}

## Performance Insights
${this.results.slowTests.length > 0 ? `
### Slowest Tests
${this.results.slowTests.map(test => `- ${test.name} (${test.duration}ms)`).join('\n')}
` : 'No slow tests detected (all tests < 100ms)'}

## Recommendations
${this.generateRecommendations()}
`;

        return report;
    }

    /**
     * Generate recommendations based on metrics
     */
    generateRecommendations() {
        const recommendations = [];

        if (this.results.failing > 0) {
            recommendations.push(`- **Fix ${this.results.failing} failing tests** to improve stability`);
        }

        if (this.results.slowTests.length > 0) {
            recommendations.push(`- **Optimize ${this.results.slowTests.length} slow tests** for better performance`);
        }

        const totalDuration = this.results.duration;
        if (totalDuration > 10000) {
            recommendations.push(`- **Reduce test suite duration** (currently ${totalDuration}ms) by parallelizing or optimizing tests`);
        }

        // Check category health
        Object.entries(this.results.categories).forEach(([category, data]) => {
            if (data.failing > 0) {
                recommendations.push(`- **${category} category** has ${data.failing} failing tests requiring attention`);
            }
        });

        if (recommendations.length === 0) {
            recommendations.push('- ✅ Test suite is healthy! All tests passing with good performance.');
        }

        return recommendations.join('\n');
    }

    /**
     * Save metrics to file
     */
    saveMetrics(filename = 'test-metrics.json') {
        const metricsPath = path.join(__dirname, '..', filename);
        fs.writeFileSync(metricsPath, JSON.stringify(this.results, null, 2));
        console.log(`📄 Metrics saved to ${metricsPath}`);
    }

    /**
     * Save report to file
     */
    saveReport(filename = 'test-report.md') {
        const reportPath = path.join(__dirname, '..', filename);
        fs.writeFileSync(reportPath, this.generateReport());
        console.log(`📄 Report saved to ${reportPath}`);
    }
}

// CLI execution
if (require.main === module) {
    const metrics = new TestMetrics();
    
    metrics.runAllMetrics()
        .then(results => {
            console.log('\n📊 Test Metrics Summary:');
            console.log(`Total: ${results.totalTests} tests`);
            console.log(`Passing: ${results.passing}`);
            console.log(`Failing: ${results.failing}`);
            console.log(`Duration: ${results.duration}ms`);
            
            metrics.saveMetrics();
            metrics.saveReport();
            
            console.log('\n' + metrics.generateReport());
            
            process.exit(results.failing > 0 ? 1 : 0);
        })
        .catch(error => {
            console.error('Error running test metrics:', error);
            process.exit(1);
        });
}

module.exports = TestMetrics;