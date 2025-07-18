# Connect FAAS Testing Framework

Comprehensive testing framework for the ConnectFaas project, designed to be modular, scalable, and maintainable.

## Directory Structure

```
test/
├── unit/                         # Unit tests
│   ├── dhq.test.js               # DHQ unit tests
│   └── fileProcessing.test.js    # File processing tests
├── integration/                  # Integration tests
│   └── dhq.integration.test.js   # DHQ workflow tests
├── mocks/                        # Modular mock system
│   ├── firebaseMocks.test.js     # Mock system tests
│   ├── mockFactory.js            # Mock orchestration
│   ├── core/                     # Core mock implementations
│   │   ├── firestoreMocks.js     # Firestore mocking
│   │   ├── firebaseAuthMocks.js  # Auth mocking
│   │   └── storageMocks.js       # Storage mocking
│   └── helpers/                  # Mock utilities
│       └── mockHelpers.js        # Helper functions
├── testUtils/                    # Test data generation
│   ├── csvUtils.js               # CSV data generation
│   ├── participantUtils.js       # Participant data generation
│   ├── dhqApiUtils.js            # DHQ API responses
│   ├── errorUtils.js             # Error scenarios
│   ├── performanceUtils.js       # Performance data
│   ├── appSettingsUtils.js       # App settings
│   └── index.js                  # Central export
├── shared/                       # Shared utilities
│   ├── testHelpers.js            # Test setup functions
│   └── errorScenarios.js         # Error testing utilities
├── scripts/                      # Test automation
│   └── testMetrics.js            # Metrics and monitoring
├── constants.js                  # Test constants
└── README.md                     # This file
```

## Quick Start

### Running Tests

```bash
# Run all tests
npm run test:all

# Run by category
npm run test:unit               # Unit tests only
npm run test:integration        # Integration tests only
npm run test:mocks              # Mock system tests only

# Run specific modules
npm run test:dhq                # DHQ-related tests
npm run test:fileProcessing     # File processing tests

# Test utilities
npm run test:watch              # Watch mode
npm run test:metrics            # Generate metrics report
npm run test:report             # View test report
```

## Framework Components

### 1. Constants (`constants.js`)

**Purpose**: Centralized test constants to reduce hardcoded values and improve maintainability.

**Usage**:
```javascript
const TEST_CONSTANTS = require('../constants');

// Use centralized values instead of hardcoded strings
const participantId = TEST_CONSTANTS.PARTICIPANT_IDS.DEFAULT; // 'participant1'
const studyId = TEST_CONSTANTS.STUDY_IDS.DEFAULT; // 'study_123'
```

### 2. Unit Tests (`unit/`)

**Purpose**: Test individual functions and modules in isolation.

**Adding New Unit Tests**:
```javascript
// test/unit/newModule.test.js
const { expect } = require('chai');
const { setupTestSuite } = require('../shared/testHelpers');

let factory, mocks, newModule;

before(() => {
    const mockSystem = setupTestSuite({
        setupConsole: false,
        setupModuleMocks: true
    });
    factory = mockSystem.factory;
    mocks = mockSystem.mocks;
    
    // Load modules after mocking is set up
    newModule = require('../../utils/newModule');
});

describe('New Module Unit Tests', () => {
    it('should test specific functionality', () => {
        // Test implementation
    });
});
```

### 3. Integration Tests (`integration/`)

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
const { setupTestSuite } = require('../shared/testHelpers');
const TestUtils = require('../testUtils');

// Set up test environment, mocks, and cleanup
const { factory, mocks } = setupTestSuite({
    setupConsole: false,
    setupModuleMocks: true
});

describe('New Workflow Integration Tests', () => {
    it('should test end-to-end workflow', async () => {
        // Setup test data
        const testData = TestUtils.createMockCSVData().createAnalysisResultsCSV(5);
        
        // Test workflow implementation
    });
});
```

### 4. Mock System (`mocks/`)

**Purpose**: Firebase SDK mocks.

**Components**:
- `mockFactory.js`: Mock orchestration
- `core/`: Firestore, Auth, Storage mocks
- `helpers/`: Mock utilities

**Using Firebase Mocks**:
```javascript
const { createFirebaseMocks } = require('../mocks/mockFactory.js');

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

// Setup document retrieval
factory.setupDocumentRetrieval('collection/path', 'docId', mockData);
```

### 5. Test Utilities (`testUtils/`)

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
const participant = TestUtils.createMockParticipantData().createNotStartedDHQParticipant('participant123');

// Generate DHQ API responses
const response = TestUtils.createMockDHQResponses().createCompletedRespondentInfo();

// Generate error scenarios
const error = TestUtils.createMockErrorScenarios().createNetworkError('Connection failed');

// Generate performance data
const metrics = TestUtils.createMockPerformanceData().createProcessingMetrics(1000, 950, 50, 2000);
```

### 6. Shared Helpers (`shared/`)

**Purpose**: Test setup functions and utilities.

**Components**:
- `testHelpers.js`: Test setup, assertions, benchmarking, console stubbing
- `errorScenarios.js`: Comprehensive error testing utilities

**Using Shared Helpers**:
```javascript
const { setupTestSuite, assertResult, createConsoleSafeStub } = require('../shared/testHelpers');
const ErrorScenarios = require('../shared/errorScenarios');

// Complete test setup
const { factory, mocks } = setupTestSuite({
    setupConsole: false,
    setupModuleMocks: true
});

// Assert processing results
assertResult(result, {
    documentCount: 10,
    respondentCount: 10,
    skippedCount: 0
});

// Safe console stubbing
const consoleStub = createConsoleSafeStub('warn');

// Error scenario testing
const errorScenarios = new ErrorScenarios();
const networkError = errorScenarios.createNetworkError('Connection failed');
```

### 7. Test Metrics (`scripts/`)

**Purpose**: Automated test metrics and monitoring.

**Components**:
- `testMetrics.js`: Test metrics collection and reporting

**Using Test Metrics**:
```bash
# Generate comprehensive metrics report
npm run test:metrics

# View formatted report
npm run test:report
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
const { setupTestSuite } = require('../shared/testHelpers');
const TestUtils = require('../testUtils');
const TEST_CONSTANTS = require('../constants');

// Set up test environment, mocks, and cleanup
const { factory, mocks } = setupTestSuite({
    setupConsole: false,
    setupModuleMocks: true
});

describe('Test Suite', () => {
    // Tests here
});
```

### Data Generation

```javascript
// Use TestUtils for consistent test data
const mockData = TestUtils.createMockParticipantData().createNotStartedDHQParticipant(TEST_CONSTANTS.PARTICIPANT_IDS.DEFAULT);
const csvData = TestUtils.createMockCSVData().createAnalysisResultsCSV(5);
```

### Mock Config

```javascript
// Setup mocks before tests
factory.setupCollectionData(TEST_CONSTANTS.COLLECTIONS.PARTICIPANTS, mockParticipants);
factory.setupQueryResults(TEST_CONSTANTS.COLLECTIONS.PARTICIPANTS, queryResults);
factory.setupTransaction(transactionHandler);
```

### Assertions

```javascript
// Use shared assertion helpers
assertResult(result, {
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
    return prepareDocumentsForFirestore(largeDataset, TEST_CONSTANTS.STUDY_IDS.DEFAULT, TEST_CONSTANTS.DOCS.ANALYSIS_RESULTS);
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
const ErrorScenarios = require('../shared/errorScenarios');
const errorScenarios = new ErrorScenarios();

const networkError = errorScenarios.createNetworkError('Connection timeout');
const apiError = errorScenarios.createDHQAPIError(401, 'Invalid token');

// Test error handling
expect(() => someFunction()).to.throw(networkError.message);

// Async error testing
await errorScenarios.testAsyncError(asyncFunction, expectedError, ...args);
```

## Configuration

### Test Environment

Tests automatically set up the following environment:
- `NODE_ENV=test`
- `DHQ_TOKEN=test-token`
- Global fetch stub
- Firebase mocks (when enabled)

### Console Output

Console mocking is disabled by default in most tests to prevent conflicts when running multiple test files together. Enable only when needed for specific tests.

## Troubleshooting

### Common Issues

1. **Module mocking conflicts**: Disable `setupModuleMocks: false` for tests that don't need Firebase
2. **Console mocking conflicts**: Use `createConsoleSafeStub()` helper to prevent conflicts
3. **Test isolation**: Each test file runs independently with automatic cleanup
4. **"Already wrapped" errors**: Framework prevents these by checking `isSinonProxy` before wrapping
5. **Firebase module loading**: Load Firebase modules after mock setup in `before()` hooks
6. **Hardcoded values**: Use `TEST_CONSTANTS` instead of hardcoded strings for maintainability

### Debugging

```javascript
// Enable console logging for debugging
const { factory, mocks } = setupTestSuite({ setupConsole: true });
const consoleMocks = factory.setupConsoleMocks();
consoleMocks.log.calledWith('Debug message');

// Check mock state
console.log(factory.mockDocs);
console.log(factory.mockCollections);
console.log(factory.testDocumentRegistry);

// Validate test state
const { validateTestState } = require('../shared/testHelpers');
const state = validateTestState();
if (!state.clean) console.log('Test isolation issues:', state.issues);
```

## Future Expansion

This framework is designed to scale. To add new test categories:

1. **Create new directories** under `test/` as needed
2. **Add comprehensive documentation** for new features
3. **Add new utilities** to `testUtils/` for specific data generation
4. **Update package.json** with new test scripts
5. **Follow existing patterns** for consistency
6. **Update this README** with new documentation and usage examples
