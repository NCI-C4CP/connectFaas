# Connect FAAS Testing Framework

This directory contains the ConnectFaas testing framework. The framework is designed to be modular and scalable for future testing needs.

## Directory Structure

```
test/
├── unit/                         # Unit tests
│   ├── dhq.test.js               # DHQ unit tests
│   └── ...                       # Future
├── integration/                  # Integration tests
│   ├── dhq.integration.test.js   # DHQ workflow tests
│   └── ...                       # Future
├── mocks/                        # Mock system
│   ├── firebaseMocks.js          # Firebase Admin SDK mock factory
│   └── firebaseMocks.test.js     # Mock system tests
├── testUtils/                    # Modular test utils
│   ├── csvUtils.js               # CSV data generation utils
│   ├── participantUtils.js       # Participant data generation utils
│   ├── dhqApiUtils.js            # DHQ API response generation utils
│   ├── errorUtils.js             # Error scenario generation utils
│   ├── performanceUtils.js       # Performance data generation utils
│   ├── appSettingsUtils.js       # App settings generation utils
│   └── index.js                  # Central export file
├── shared/                       # Shared test helpers
│   └── testHelpers.js            # Common test setup functions
├── test.js                       # Tests not yet migrated to this format
└── README.md                     # This
```

## Quick Start

### Running Tests

```bash
# Run all tests
npm run test:all

# Run specific test categories
npm run test:unit         # Unit tests only
npm run test:integration  # Integration tests only
npm run test:mocks        # Mock system tests only
```

## Framework Components

### 1. Unit Tests (`unit/`)

**Purpose**: Test individual functions and modules in isolation.

**Adding New Unit Tests**:
```javascript
// test/unit/newModule.test.js
const { expect } = require('chai');
const { createCompleteTestSetup } = require('../shared/testHelpers');

describe('New Module Unit Tests', () => {
    const { factory, mocks, restore } = createCompleteTestSetup();
    
    it('should test specific functionality', () => {
        // Test implementation
    });
});
```

### 2. Integration Tests (`integration/`)

**Purpose**: Test end-to-end workflows and component interactions.

**Current Coverage**:
- `dhq.integration.test.js`: DHQ workflow integration tests
  - CSV processing with Firebase mocks
  - Query scenarios
  - Transaction scenarios
  - Performance and load testing

**Adding New Integration Tests**:
```javascript
// test/integration/newWorkflow.integration.test.js
const { expect } = require('chai');
const { createCompleteTestSetup } = require('../shared/testHelpers');
const TestUtils = require('../testUtils');

describe('New Workflow Integration Tests', () => {
    const { factory, mocks, restore } = createCompleteTestSetup();
    
    it('should test end-to-end workflow', async () => {
        // Setup test data
        const testData = TestUtils.createMockCSVData().createAnalysisResultsCSV(5);
        
        // Test workflow implementation
    });
});
```

### 3. Mock System (`mocks/`)

**Purpose**: Firebase Admin SDK mocking.

**Components**:
- `firebaseMocks.js`: Firebase mock factory
- `firebaseMocks.test.js`: Demo tests

**Using Firebase Mocks**:
```javascript
const { createFirebaseMocks } = require('../mocks/firebaseMocks');

// Basic setup
const { factory, mocks, restore } = createFirebaseMocks({
    setupConsole: false,
    setupModuleMocks: true
});

// Setup collection data
factory.setupCollectionData('participants', mockParticipants, 'state.uid');

// Setup queries
factory.setupQueryResults('participants', queryResults);

// Setup transactions
factory.setupTransaction(transactionHandler);

// Setup batch operations
const batch = factory.setupBatch({ success: true });

// Setup count operations
factory.setupCount('collection/path', 1500);
```

### 4. Test Utilities (`testUtils/`)

**Purpose**: Provide modular, reusable test data generation utilities.

**Components**:
- `csvUtils.js`: CSV data generation for DHQ reports
- `participantUtils.js`: Participant and credential data generation
- `dhqApiUtils.js`: DHQ API response generation
- `errorUtils.js`: Error scenario generation
- `performanceUtils.js`: Performance data generation
- `appSettingsUtils.js`: App settings generation
- `index.js`: Central export maintaining backward compatibility

**Using Test Utilities**:
```javascript
const TestUtils = require('../testUtils');

// Generate CSV data
const csvData = TestUtils.createMockCSVData().createAnalysisResultsCSV(10);

// Generate participant data
const participant = TestUtils.createMockParticipantData().createBasicParticipant('participant123');

// Generate DHQ API responses
const response = TestUtils.createMockDHQResponses().createCompletedRespondentInfo();

// Generate error scenarios
const error = TestUtils.createMockErrorScenarios().createNetworkError('Connection failed');

// Generate performance data
const metrics = TestUtils.createMockPerformanceData().createProcessingMetrics(1000, 950, 50, 2000);
```

### 5. Shared Helpers (`shared/`)

**Purpose**: Test setup functions.

**Components**:
- `testHelpers.js`: Test setup functions, assertions, benchmarking

**Using Shared Helpers**:
```javascript
const { createCompleteTestSetup, assertProcessingResult } = require('../shared/testHelpers');

// Complete test setup
const { factory, mocks, restore } = createCompleteTestSetup();

// Assert processing results
assertProcessingResult(result, {
    documentCount: 10,
    respondentCount: 10,
    skippedCount: 0
});

```

## Testing

### Test Organization

- **Unit Tests**: Test individual functions in isolation
- **Integration Tests**: Test workflows and component interactions
- **Mock Tests**: Demonstrate mock system capabilities
- **Shared Code**: Use utilities and helpers to avoid duplication

### Test Setup

```javascript
// Standard test setup pattern
const { expect } = require('chai');
const { createCompleteTestSetup } = require('../shared/testHelpers');
const TestUtils = require('../testUtils');

describe('Test Suite', () => {
    const { factory, mocks, restore } = createCompleteTestSetup();
    
    // Tests here
});
```

### Data Generation

```javascript
// Use TestUtils for consistent test data
const mockData = TestUtils.createMockParticipantData().createBasicParticipant('user123');
const csvData = TestUtils.createMockCSVData().createAnalysisResultsCSV(5);
```

### Mock Config

```javascript
// Setup mocks before tests
factory.setupCollectionData('participants', mockParticipants);
factory.setupQueryResults('participants', queryResults);
factory.setupTransaction(transactionHandler);
```

### Assertions

```javascript
// Use shared assertion helpers
assertProcessingResult(result, {
    documentCount: 10,
    respondentCount: 10,
    skippedCount: 0
});
```

### Performance Testing

```javascript
// Performance benchmarking
const { benchmarkFunction } = require('../shared/testHelpers');
const { result, processingTime } = benchmarkFunction(() => {
    return prepareDocumentsForFirestore(largeDataset, 'study_123', 'analysisResults');
}, 'Document Processing');

expect(processingTime).to.be.lessThan(5000);
```

### Memory Testing

```javascript
// Memory pressure testing
const perfUtils = TestUtils.createMockPerformanceData();
const memoryUsage = perfUtils.createMemoryUsage(1200); // 1200MB

// Mock process.memoryUsage
process.memoryUsage = () => memoryUsage;
const chunkSize = getDynamicChunkSize();
expect(chunkSize).to.equal(250); // Reduced chunk size for high memory
```

### Error Scenario Testing

```javascript
// Error scenario generation
const errorUtils = TestUtils.createMockErrorScenarios();
const networkError = errorUtils.createNetworkError('Connection timeout');
const apiError = errorUtils.createDHQAPIError(401, 'Invalid token');

// Test error handling
expect(() => someFunction()).to.throw(networkError.message);
```

## Configuration

### Test Environment

Tests automatically set up the following environment:
- `NODE_ENV=test`
- `DHQ_TOKEN=test-token`
- Global fetch stub
- Firebase mocks (when enabled)

### Memory Management

The framework includes memory-aware testing:
- Dynamic chunk sizing based on memory usage
- Memory pressure simulation
- Performance thresholds and monitoring

### Console Output

Console mocking is disabled by default in most tests to prevent conflicts when running multiple test files together. Enable only when needed for specific tests.

## Troubleshooting

### Common Issues

1. **Module mocking conflicts**: Disable `setupModuleMocks: false` for tests that don't need Firebase
2. **Console mocking conflicts**: Use `setupConsole: false` (default) unless specifically needed
3. **Test isolation**: Each test file runs independently with proper cleanup

### Debugging

```javascript
// Enable console logging for debugging
const { factory } = createFirebaseMocks({ setupConsole: true });
const consoleMocks = factory.setupConsoleMocks();
consoleMocks.log.calledWith('Debug message');

// Check mock state
console.log(factory.mockDocs);
console.log(factory.mockCollections);
```

## Future Expansion

This framework is designed to scale. To add new test categories:

1. **Create new directories** under `test/` as needed
2. **Add comprehensive documentation** for new features
3. **Add new utilities** to `testUtils/` for specific data generation
4. **Update package.json** with new test scripts
5. **Follow existing patterns** for consistency
6. **Update this README** with new documentation and usage examples
