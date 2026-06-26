const eventsPath = require.resolve("../../utils/events");
const firestorePath = require.resolve("@google-cloud/firestore");
const storagePath = require.resolve("@google-cloud/storage");
const bigqueryPath = require.resolve("@google-cloud/bigquery");

const mocks = {
  databasePath: vi.fn(),
  exportDocuments: vi.fn(),
  file: vi.fn(),
  load: vi.fn(),
  table: vi.fn(),
  bucket: vi.fn(),
  dataset: vi.fn(),
};

const originalModules = new Map();

const mockRequire = (modulePath, exports) => {
  if (!originalModules.has(modulePath)) originalModules.set(modulePath, require.cache[modulePath]);
  require.cache[modulePath] = {
    id: modulePath,
    filename: modulePath,
    loaded: true,
    exports,
  };
};

const restoreRequireMocks = () => {
  for (const [modulePath, originalModule] of originalModules.entries()) {
    if (originalModule) require.cache[modulePath] = originalModule;
    else delete require.cache[modulePath];
  }
  originalModules.clear();
  delete require.cache[eventsPath];
};

const loadEventsWithMocks = () => {
  mockRequire(firestorePath, {
    v1: {
      FirestoreAdminClient: function FirestoreAdminClient() {
        return {
          databasePath: mocks.databasePath,
          exportDocuments: mocks.exportDocuments,
        };
      },
    },
  });

  mockRequire(storagePath, {
    Storage: function Storage() {
      return { bucket: mocks.bucket };
    },
  });

  mockRequire(bigqueryPath, {
    BigQuery: function BigQuery() {
      return { dataset: mocks.dataset };
    },
  });

  delete require.cache[eventsPath];
  return require("../../utils/events");
};

describe("Firestore export to BigQuery sync", () => {
  let events;

  beforeEach(() => {
    vi.clearAllMocks();
    process.env.GCLOUD_BUCKET = "test-bucket";
    process.env.GCLOUD_PROJECT = "test-project";

    mocks.databasePath.mockImplementation((projectId, databaseId) => `projects/${projectId}/databases/${databaseId}`);
    mocks.exportDocuments.mockResolvedValue([]);
    mocks.file.mockImplementation((fileName) => ({ fileName }));
    mocks.load.mockResolvedValue([{ status: {} }]);
    mocks.table.mockReturnValue({ load: mocks.load });
    mocks.dataset.mockReturnValue({ table: mocks.table });
    mocks.bucket.mockReturnValue({ file: mocks.file });

    events = loadEventsWithMocks();
  });

  afterEach(() => {
    delete process.env.GCLOUD_BUCKET;
    delete process.env.GCLOUD_PROJECT;
    restoreRequireMocks();
  });

  it("includes selfReportCancerDx in the scheduled Firestore export", async () => {
    await events.runFirestoreExport();

    expect(mocks.exportDocuments).toHaveBeenCalledWith(expect.objectContaining({
      collectionIds: expect.arrayContaining(["selfReportCancerDx"]),
    }));
  });

  it("imports selfReportCancerDx Firestore exports into the matching BigQuery table", async () => {
    await events.importToBigQuery({
      body: {
        id: "test-bucket/2026-06-24T00:00:00/all_namespaces/kind_selfReportCancerDx/all_namespaces_kind_selfReportCancerDx.export_metadata",
      },
    });

    expect(mocks.dataset).toHaveBeenCalledWith("Connect");
    expect(mocks.table).toHaveBeenCalledWith("selfReportCancerDx");
    expect(mocks.load).toHaveBeenCalledWith(
      { fileName: "2026-06-24T00:00:00/all_namespaces/kind_selfReportCancerDx/all_namespaces_kind_selfReportCancerDx.export_metadata" },
      expect.objectContaining({
        sourceFormat: "DATASTORE_BACKUP",
        writeDisposition: "WRITE_TRUNCATE",
      }),
    );
  });
});
