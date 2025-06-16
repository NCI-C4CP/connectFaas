const { Storage } = require("@google-cloud/storage");
const Busboy = require("busboy");
const { savePathologyReportNamesToFirestore, getUploadedPathologyReportNamesFromFirestore } = require("./firestore");

const storage = new Storage();
const tierStr = process.env.GCLOUD_PROJECT.split("-").slice(3, 5).join("-").toLowerCase();
const pathologyReports = "pathology-reports";

async function awaitNewBucketReady(bucketName, maxRetries = 10, delayMs = 500) {
  let retries = 0;
  while (retries < maxRetries) {
    const [exists] = await storage.bucket(bucketName).exists();
    if (exists) return;
    await new Promise((res) => setTimeout(res, delayMs));
    retries++;
  }
  throw new Error(`Bucket ${bucketName} not ready after ${maxRetries} retries`);
}

const uploadPathologyReports = (req, res) => {
  if (req.method !== "POST") {
    return res.status(405).json({ message: "Only POST requests are accepted!", code: 405 });
  }

  if (!req.headers["content-type"] || !req.headers["content-type"].includes("multipart/form-data")) {
    return res.status(400).json({ message: "content-type must be multipart/form-data", code: 400 });
  }

  const Connect_ID = req.query.Connect_ID;
  const siteAcronymLower = req.query.siteAcronym?.toLowerCase();

  if (!Connect_ID || !siteAcronymLower) {
    return res.status(400).json({ message: "Missing Connect_ID or siteAcronym in query parameters", code: 400 });
  }

  const bucketName = `${pathologyReports}_${siteAcronymLower}_${tierStr}`;
  const busboy = Busboy({ headers: req.headers });
  const uploadPromises = [];
  let fields = {};
  let bucket = null;

  busboy.on("field", (fieldname, val) => {
    fields[fieldname] = val;
  });

  busboy.on("file", async (fieldname, file, { filename, mimetype }) => {
    if (!bucket) {
      const [exists] = await storage.bucket(bucketName).exists();
      if (!exists) {
        await storage.createBucket(bucketName, {
          location: "US",
          project: storage.projectId,
        });
        await awaitNewBucketReady(bucketName);
      }
      bucket = storage.bucket(bucketName);
    }

    const filePath = `${Connect_ID}/${filename}`;
    const fileUpload = bucket.file(filePath);
    const stream = file.pipe(
      fileUpload.createWriteStream({
        metadata: { contentType: mimetype },
      })
    );

    uploadPromises.push(
      new Promise((resolve, reject) => {
        stream.on("finish", () => resolve(filename));
        stream.on("error", (error) => {
          console.error(`Error streaming file "${filename}" to bucket:`, error);
          reject(error);
        });
      })
    );
  });

  busboy.on("finish", async () => {
    try {
      const promiseResults = await Promise.allSettled(uploadPromises);
      const successFilenames = promiseResults
        .filter((result) => result.status === "fulfilled")
        .map((result) => result.value);

      if (successFilenames.length === 0) {
        return res.status(500).json({ message: "Failed to upload files!", code: 500 });
      }

      await savePathologyReportNamesToFirestore({ bucketName, Connect_ID: parseInt(Connect_ID), filenames: successFilenames });

      const [allFiles] = await bucket.getFiles({ prefix: `${Connect_ID}/` });
      const allFilenameArray = allFiles.map((f) => f.name.replace(`${Connect_ID}/`, ""));

      return res.status(200).json({ message: "Files uploaded successfully", data: allFilenameArray, code: 200 });
    } catch (error) {
      return res.status(500).json({
        message: "Error uploading files: " + error.message,
        code: 500,
      });
    }
  });

  busboy.end(req.rawBody);
};

const getUploadedPathologyReportNames = async (req, res) => {
  if (req.method !== "GET") {
    return res.status(405).json({ message: "Only GET requests are accepted!", code: 405 });
  }

  const Connect_ID = req.query.Connect_ID;
  const siteAcronymLower = req.query.siteAcronym?.toLowerCase();
  if (!Connect_ID || !siteAcronymLower) {
    return res.status(400).json({ message: "Missing Connect_ID or siteAcronym in query parameters", code: 400 });
  }
  
  const bucketName = `${pathologyReports}_${siteAcronymLower}_${tierStr}`;
  try {
    const filenames = await getUploadedPathologyReportNamesFromFirestore({ bucketName, Connect_ID: parseInt(Connect_ID) });

    return res.status(200).json({ code: 200, data: filenames });
  } catch (error) {
    return res.status(500).json({
      message: "Error retrieving uploaded pathology report names: " + error.message,
      code: 500,
    });
  }
};

module.exports = {
  uploadPathologyReports,
  getUploadedPathologyReportNames,
};
