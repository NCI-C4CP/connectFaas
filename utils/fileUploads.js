const Busboy = require("@fastify/busboy");
const {
  storage,
  savePathologyReportNamesToFirestore,
  getUploadedPathologyReportNamesFromFirestore,
  getEhrDeliveries,
  updateEhrDeliveries,
} = require("./firestore");

const validTierStr = ["dev", "stg", "prod"];
const tierStr = process.env.GCLOUD_PROJECT?.split("-")[4];
const ehrProdBucketNames = {
  bswh: "ehr_bswh",
  hp: "ehr_healthpartners",
  hfhs: "ehr_henry_ford",
  kpco: "ehr_kp_colorado",
  kpga: "ehr_kp_georgia",
  kphi: "ehr_kp_hawaii",
  kpnw: "ehr_kp_northwest",
  mfc: "ehr_marshfield",
  sfh: "ehr_sanford",
  ucm: "ehr_uchicago",
};

let ehrBucketNames = {};
if (tierStr === "prod") {
  ehrBucketNames = ehrProdBucketNames;
} else if (tierStr === "stg" || tierStr === "dev") {
  for (const [key, value] of Object.entries(ehrProdBucketNames)) {
    ehrBucketNames[key] = `${value}_${tierStr}`;
  }
  ehrBucketNames.nih = `ehr_nci_${tierStr}`; // NCI bucket is only in dev/stg
}

const awaitNewBucketReady = async (bucketName, maxRetries = 10, delayMs = 500) => {
  let retries = 0;
  while (retries < maxRetries) {
    const [exists] = await storage.bucket(bucketName).exists();
    if (exists) return;
    await new Promise((res) => setTimeout(res, delayMs));
    retries++;
  }

  throw new Error(`Bucket not ready.`);
};

/**
 * Streams files from a multipart/form-data HTTP request to a specified GCS bucket and path.
 * Used for small files (under 32 MB).
 * @param {Request} httpRequest
 * @param {string} bucketName
 * @param {string} pathInBucket
 * @returns
 */
const streamRequestFilesToBucket = async (httpRequest, bucketName, pathInBucket = "") => {
  let bucket = null;
  try {
    const [exists] = await storage.bucket(bucketName).exists();
    if (!exists) {
      await storage.createBucket(bucketName, {
        location: "US",
        project: storage.projectId,
      });
      await awaitNewBucketReady(bucketName);
    }
    bucket = storage.bucket(bucketName);
  } catch (error) {
    throw new Error(`Failed to create or access bucket ${bucketName}: ${error.message}`);
  }

  const busboy = new Busboy({ headers: httpRequest.headers });
  const uploadPromises = [];
  let failureFilenames = [];
  let successFilenames = [];
  let allFilenames = [];
  let fields = {};

  return new Promise((resolve, reject) => {
    busboy.on("field", (fieldname, val) => {
      fields[fieldname] = val;
    });

    busboy.on("file", (fieldname, file, filename, encoding, mimetype) => {
      try {
        const filePath = pathInBucket ? `${pathInBucket}/${filename}` : filename;
        const stream = file.pipe(
          bucket.file(filePath).createWriteStream({
            metadata: { contentType: mimetype },
            resumable: true,
            timeout: 0,
          })
        );

        uploadPromises.push(
          new Promise((res, rej) => {
            stream.on("finish", () => res(filename));
            stream.on("error", (error) => {
              failureFilenames.push(filename);
              console.error(`Error streaming file ${filename} to bucket ${bucketName}:`, error);
              rej(error);
            });
          })
        );
      } catch (error) {
        failureFilenames.push(filename);
        console.error(`Error processing file "${filename}":`, error);
      }
    });

    busboy.on("finish", async () => {
      try {
        const promiseResults = await Promise.allSettled(uploadPromises);
        successFilenames = promiseResults
          .filter((result) => result.status === "fulfilled")
          .map((result) => result.value);
        if (pathInBucket) {
          const [allFiles] = await bucket.getFiles({ prefix: `${pathInBucket}/`, fields: "items(name)" });
          allFilenames = allFiles.map((f) => f.name.replace(`${pathInBucket}/`, ""));
        }

        resolve({ successFilenames, failureFilenames, allFilenames, fields });
      } catch (error) {
        reject(error);
      }
    });

    busboy.on("error", (error) => {
      reject(error);
    });

    busboy.end(httpRequest.rawBody);
  });
};

const uploadPathologyReports = async (req, res) => {
  if (!validTierStr.includes(tierStr)) {
    return res.status(500).json({ message: `Invalid tier "${tierStr}"`, code: 500 });
  }

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

  const bucketName = `pathology-reports_${siteAcronymLower}_connect-${tierStr}`;

  try {
    const { successFilenames, failureFilenames, allFilenames } = await streamRequestFilesToBucket(
      req,
      bucketName,
      Connect_ID
    );

    await savePathologyReportNamesToFirestore({
      bucketName,
      Connect_ID: parseInt(Connect_ID),
      filenames: successFilenames,
    });

    if (successFilenames.length === 0) {
      return res.status(500).json({ message: "Failed to upload files!", code: 500 });
    }

    if (failureFilenames.length > 0) {
      return res.status(207).json({
        message: "Some files failed to upload",
        data: { successFilenames, failureFilenames, allFilenames },
        code: 207,
      });
    }

    return res.status(200).json({
      message: "Files uploaded successfully",
      data: { successFilenames, allFilenames },
      code: 200,
    });
  } catch (error) {
    return res.status(500).json({
      message: "Error uploading pathology reports: " + error.message,
      code: 500,
    });
  }
};

const getUploadedPathologyReportNames = async (req, res) => {
  if (!validTierStr.includes(tierStr)) {
    return res.status(500).json({ message: `Invalid tier "${tierStr}"`, code: 500 });
  }

  if (req.method !== "GET") {
    return res.status(405).json({ message: "Only GET requests are accepted!", code: 405 });
  }

  const Connect_ID = req.query.Connect_ID;
  const siteAcronymLower = req.query.siteAcronym?.toLowerCase();
  if (!Connect_ID || !siteAcronymLower) {
    return res.status(400).json({ message: "Missing Connect_ID or siteAcronym in query parameters", code: 400 });
  }

  const bucketName = `pathology-reports_${siteAcronymLower}_connect-${tierStr}`;
  try {
    const filenames = await getUploadedPathologyReportNamesFromFirestore({
      bucketName,
      Connect_ID: parseInt(Connect_ID),
    });

    return res.status(200).json({ code: 200, data: filenames });
  } catch (error) {
    return res.status(500).json({
      message: "Error retrieving uploaded pathology report names: " + error.message,
      code: 500,
    });
  }
};

const createSignedUploadUrl = (bucketName, filenameWithPath, contentType) => {
  return storage
    .bucket(bucketName)
    .file(filenameWithPath)
    .getSignedUrl({
      version: "v4",
      action: "write",
      expires: Date.now() + 10000, // 10 seconds.
      contentType: contentType || "application/octet-stream",
    });
};

/**
 * Generates a signed URL for each file, so that client can upload EHRs directly to GCS.
 * Uploads large files (multiple GB).
 * @param {Request} req
 * @param {Response} res
 * @param {string} acronym - Site acronym for bucket name
 * @returns
 */
const createEhrUploadUrls = async (req, res, acronym) => {
  if (!validTierStr.includes(tierStr)) {
    return res.status(500).json({ message: `Invalid tier "${tierStr}"`, code: 500 });
  }

  if (!acronym) {
    return res.status(400).json({ message: "Missing acronym for EHR uploads!", code: 400 });
  }

  if (req.method !== "POST") {
    return res.status(405).json({ message: "Only POST requests are accepted!", code: 405 });
  }

  const acronymLower = acronym.toLowerCase();
  const bucketName = ehrBucketNames[acronymLower];
  if (!bucketName) {
    return res.status(400).json({ message: `No bucket for ${acronym} in ${tierStr} tier.`, code: 400 });
  }

  const { fileInfoArray, name, uploadStartedAt } = req.body;
  if (!fileInfoArray || !Array.isArray(fileInfoArray) || fileInfoArray.length === 0 || !name) {
    return res.status(400).json({ message: "Invalid request body", code: 400 });
  }
  try {
    const urlPromises = [];
    for (const item of fileInfoArray) {
      const { filename, contentType } = item;
      if (!filename || !contentType) {
        return res.status(400).json({ message: "Each file must have a filename and contentType", code: 400 });
      }
      urlPromises.push(createSignedUploadUrl(bucketName, `${name}/${filename}`, contentType));
    }

    const results = await Promise.all(urlPromises);
    const signedUrls = {};
    results.forEach((item, index) => {
      signedUrls[fileInfoArray[index].filename] = item[0];
    });

    await updateEhrDeliveries(acronymLower, [{ name, uploadStartedAt }]);
    return res.status(200).json({ data: { signedUrls }, code: 200 });
  } catch (error) {
    return res.status(500).json({
      message: "Error generating signed URLs: " + error.message,
      code: 500,
    });
  }
};

const getUploadedEhrNames = async (req, res, acronym) => {
  if (!validTierStr.includes(tierStr)) {
    return res.status(500).json({ message: `Invalid tier "${tierStr}"`, code: 500 });
  }

  if (req.method !== "GET") {
    return res.status(405).json({ message: "Only GET requests are accepted!", code: 405 });
  }

  const acronymLower = acronym.toLowerCase();
  const bucketName = ehrBucketNames[acronymLower];
  if (!bucketName) {
    return res
      .status(400)
      .json({ message: `No bucket for ${acronym} exists in ${tierStr} tier.`, code: 400 });
  }

  let result = {
    uploadedFileNames: [],
    uploadStartedAt: "",
    name: "",
  };

  try {
    const [exists] = await storage.bucket(bucketName).exists();
    if (!exists) {
      return res.status(404).json({ message: `Bucket ${bucketName} does not exist.`, code: 404 });
    }

    const bucket = storage.bucket(bucketName);
    const siteData = await getEhrDeliveries(acronymLower);
    let recentDeliveries = siteData.recentDeliveries || [];
    if (recentDeliveries.length === 0) {
      return res.status(200).json({ code: 200, data: result });
    }

    const deliveryCount = recentDeliveries.length;
    while (recentDeliveries.length > 0) {
      const delivery = recentDeliveries[recentDeliveries.length - 1];
      const deliveryName = delivery.name;
      const [files] = await bucket.getFiles({
        prefix: `${deliveryName}/`,
        fields: "items(name)",
      });

      if (files.length === 0) {
        recentDeliveries.pop();
      } else {
        result.uploadedFileNames = files.map((f) => f.name.replace(`${deliveryName}/`, ""));
        result.uploadStartedAt = delivery.uploadStartedAt || "";
        result.name = delivery.name || "";
        break;
      }
    }

    recentDeliveries = recentDeliveries.slice(-3); // Keep max of 3 recent deliveries
    if (deliveryCount > recentDeliveries.length) {
      await updateEhrDeliveries(acronymLower, recentDeliveries, true);
    }

    return res.status(200).json({ code: 200, data: result });
  } catch (error) {
    return res.status(500).json({
      message: "Error retrieving uploaded EHR names: " + error.message,
      code: 500,
    });
  }
};

module.exports = {
  uploadPathologyReports,
  getUploadedPathologyReportNames,
  createEhrUploadUrls,
  getUploadedEhrNames,
};
