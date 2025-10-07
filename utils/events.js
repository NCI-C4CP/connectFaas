const firestore = require('@google-cloud/firestore');
const {BigQuery} = require('@google-cloud/bigquery');
const {Storage} = require('@google-cloud/storage');

const collectionNameArray = ['participants', 'biospecimen', 'boxes', 'module1_v1', 'module1_v2', 'module2_v1', 'module2_v2', 'module3_v1', 'module4_v1', 'bioSurvey_v1', 'menstrualSurvey_v1', 'clinicalBioSurvey_v1', 'covid19Survey_v1', 'kitAssembly', 'mouthwash_v1', 'cancerOccurrence', 'promis_v1', 'experience2024', 'birthdayCard', 'cancerScreeningHistorySurvey', 'dhqAnalysisResults', 'dhqDetailedAnalysis', 'dhqRawAnswers', 'preference2025'];

const firestoreExport = async (eventData, context) => {
  await exportCollectionsToBucket(collectionNameArray);
};

const importToBigQuery = async (eventData, context) => {
  await importCollectionsToBigQuery(eventData, collectionNameArray);
};

const exportNotificationsToBucket = async (eventData, context) => {
  await exportCollectionsToBucket(["notifications"]);
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
 * @param {Event} gcsEvent 
 * @param {string[]} collectionNameArray Array of collection names
 */
async function importCollectionsToBigQuery(gcsEvent, collectionNameArray) {
  if (!gcsEvent.name.includes(".export_metadata")) return;

  let tableName = "";
  for (const collectionName of collectionNameArray) {
    if (gcsEvent.name.includes(collectionName)) {
      tableName = collectionName;
      break;
    }
  }

  if (tableName === "") return;

  console.log(`Processing file: ${gcsEvent.name}`);
  const gcsBucket = process.env.GCLOUD_BUCKET;
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
      .load(storage.bucket(gcsBucket).file(gcsEvent.name), metadata);

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
  firestoreExport,
  exportNotificationsToBucket,
  importNotificationsToBigquery,
};
