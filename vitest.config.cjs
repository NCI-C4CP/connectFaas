const { defineConfig } = require('vitest/config');

module.exports = defineConfig({
  test: {
    environment: 'node',
    globals: true,
    include: ['test/**/*.test.js'],
    pool: 'forks',
    fileParallelism: true,
    isolate: true,
    restoreMocks: true,
    testTimeout: 5000,
    hookTimeout: 10000,
    silent: 'passed-only',
    coverage: {
      provider: 'v8',
      include: ['utils/**/*.js'],
      exclude: ['test/**'],
      reporter: ['text', 'text-summary'],
      reportsDirectory: './coverage',
    },
  },
});
