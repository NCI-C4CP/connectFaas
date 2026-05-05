const firestore = require('@google-cloud/firestore');
const {BigQuery} = require('@google-cloud/bigquery');
const {Storage} = require('@google-cloud/storage');
const { getResponseJSON } = require('./shared');

const collectionNameArray = ['participants', 'biospecimen', 'boxes', 'module1_v1', 'module1_v2', 'module2_v1', 'module2_v2', 'module3_v1', 'module4_v1', 'bioSurvey_v1', 'menstrualSurvey_v1', 'clinicalBioSurvey_v1', 'covid19Survey_v1', 'kitAssembly', 'mouthwash_v1', 'cancerOccurrence', 'promis_v1', 'experience2024', 'birthdayCard', 'cancerScreeningHistorySurvey', 'dhqAnalysisResults', 'dhqDetailedAnalysis', 'dhqRawAnswers', 'preference2026'];

const runFirestoreExport = async () => {
  await exportCollectionsToBucket(collectionNameArray);
};

const importToBigQuery = async (eventData, context) => {
  await importCollectionsToBigQuery(eventData, collectionNameArray);
};

const runExportNotificationsToBucket = async () => {
  await exportCollectionsToBucket(["notifications"]);
};

const firestoreExport = async (req, res) => {
  // Preserve direct invocation compatibility for non-HTTP call sites.
  if (!req || !res) {
    return runFirestoreExport();
  }

  if (req.method !== 'POST') {
    return res.status(405).json(getResponseJSON('Method not allowed. Use POST.', 405));
  }

  try {
    await runFirestoreExport();
    return res.status(200).json(getResponseJSON('Firestore export triggered successfully.', 200));
  } catch (error) {
    console.error('Failed to trigger Firestore export.', error);
    return res.status(500).json(getResponseJSON('Failed to trigger Firestore export.', 500));
  }
};

const exportNotificationsToBucket = async (req, res) => {
  // Preserve direct invocation compatibility for non-HTTP call sites.
  if (!req || !res) {
    return runExportNotificationsToBucket();
  }

  if (req.method !== 'POST') {
    return res.status(405).json(getResponseJSON('Method not allowed. Use POST.', 405));
  }

  try {
    await runExportNotificationsToBucket();
    return res.status(200).json(getResponseJSON('Notifications export triggered successfully.', 200));
  } catch (error) {
    console.error('Failed to trigger notifications export.', error);
    return res.status(500).json(getResponseJSON('Failed to trigger notifications export.', 500));
  }
};

const importNotificationsToBigquery = async (eventData, context) => {
  await importCollectionsToBigQuery(eventData, ["notifications"]);
};

/**
 * Export collections from Firestore to Bucket
 * @param {string[]} collectionNameArray Array of collection names
 */
async function exportCollectionsToBucket(collectionNameArray) {
  if (collectionNameArray.length === 0) return;

  const client = new firestore.v1.FirestoreAdminClient();
  const gcsBucket = process.env.GCLOUD_BUCKET;
  const bucket = `gs://${gcsBucket}`;
  const projectId = process.env.GCP_PROJECT || process.env.GCLOUD_PROJECT;
  const databaseName = client.databasePath(projectId, "(default)");

  try {
    await client.exportDocuments({
      name: databaseName,
      outputUriPrefix: bucket,
      collectionIds: collectionNameArray,
    });

    console.log(`Exported ${collectionNameArray.length > 1 ? "collections" : "collection"} ${collectionNameArray.join(", ")} to bucket ${bucket}.`);
  } catch (error) {
    console.error(`Error occurred when exporting to bucket ${bucket}.`, error);
  }

}

/**
 * Import collections from Bucket to BigQuery
 * @param {Object} gcsEvent
 * @param {string[]} collectionNameArray Array of collection names
 */
async function importCollectionsToBigQuery(gcsEvent, collectionNameArray) {
  const eventBody = gcsEvent?.body;
  const eventName = eventBody?.id;

  if (!eventName || !eventName.includes('.export_metadata')) return;

  const gcsBucket = process.env.GCLOUD_BUCKET;

  let tableName = "";
  for (const collectionName of collectionNameArray) {
    if (eventName.includes(collectionName)) {
      tableName = collectionName;
      break;
    }
  }

  if (tableName === "") return;

  console.log(`Processing file: ${eventName}`);
  const storage = new Storage();
  const bigquery = new BigQuery();
  const datasetName = "Connect";
  const metadata = {
    sourceFormat: "DATASTORE_BACKUP",
    createDisposition: "CREATE_IF_NEEDED",
    writeDisposition: "WRITE_TRUNCATE",
    location: "US",
  };

  try {
    const [job] = await bigquery
      .dataset(datasetName)
      .table(tableName)
      .load(storage.bucket(gcsBucket).file(eventName), metadata);

    if (job.status.errorResult) {
      console.error(`Failed to import '${tableName}' to BigQuery.`, job.status.errorResult);
    }

    console.log(`Imported '${tableName}' to BigQuery.`);
  } catch (err) {
    console.error(`Error occured when importing to BigQuery:`, err);
  }
}

module.exports = {
  importToBigQuery,
  runFirestoreExport,
  firestoreExport,
  runExportNotificationsToBucket,
  exportNotificationsToBucket,
  importNotificationsToBigquery,
};
