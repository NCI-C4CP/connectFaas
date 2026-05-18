const { FieldValue } = require("firebase-admin/firestore");
const { randomUUID } = require("crypto");
const sgMail = require("@sendgrid/mail");
const fieldMapping = require("./fieldToConceptIdMapping");
const { getResponseJSON, getEasternDateKey, getSecret, listOfCollectionsRelatedToDataDestruction, developmentTier, validEmailFormat } = require("./shared");
const { db, storage, isValidPathologyBucketName, getAppSettings } = require("./firestore");
const { resolvePolicyForDestruction, getCurrentPolicy, describeStubVariables, validateDestroyedStub } = require("./dataDestructionPolicy");

const AUDIT_MODE = "audit";
const CLEANUP_MODE = "cleanup";
const CLEANUP_CONFIRMATION = "DELETE_ORPHANED_DATA";
const MAX_IN_QUERY_SIZE = 30;
const BATCH_DELETE_LIMIT = 400;

const TOKEN_KEYED_COLLECTIONS = new Set(["notifications", "ssn", "emailAddressStatus"]);
const DHQ_KEYED_COLLECTIONS = new Set(["dhqAnalysisResults", "dhqDetailedAnalysis", "dhqRawAnswers"]);

const toFieldKey = (field) => field.toString();

const chunkArray = (items, size) => {
    const chunks = [];
    for (let index = 0; index < items.length; index += size) {
        chunks.push(items.slice(index, index + size));
    }
    return chunks;
};

const getAuditDateStamp = (date = new Date()) => getEasternDateKey(date).replace(/-/g, "");

/**
 * Build the exact artifact names expected by the Box audit folder and analytics ingest.
 */
const buildAuditFileNames = (date = new Date()) => {
    const dateStamp = typeof date === "string" ? date : getAuditDateStamp(date);
    return {
        summaryFileName: `${dateStamp}_data_destruction_summary.json`,
        participantsFileName: `${dateStamp}_data_destruction_participants.ndjson`,
    };
};

const getProjectContext = () => ({
    projectId: process.env.GCLOUD_PROJECT || "",
    tier: developmentTier,
});

const parseRequestBody = (body) => {
    if (!body) return {};
    if (typeof body === "string") {
        return JSON.parse(body);
    }
    return body;
};

const normalizeAuditOptions = (rawOptions = {}) => {
    const options = parseRequestBody(rawOptions);
    const mode = options.mode || AUDIT_MODE;
    const dryRun = options.dryRun === true;

    if (![AUDIT_MODE, CLEANUP_MODE].includes(mode)) {
        const error = new Error("Invalid mode. Use audit or cleanup.");
        error.statusCode = 400;
        throw error;
    }

    if (dryRun && mode !== CLEANUP_MODE) {
        const error = new Error("dryRun is only valid with cleanup mode.");
        error.statusCode = 400;
        throw error;
    }

    if (mode === CLEANUP_MODE && !dryRun && options.confirmCleanup !== CLEANUP_CONFIRMATION) {
        const error = new Error("Cleanup mode requires confirmCleanup: DELETE_ORPHANED_DATA (or dryRun: true).");
        error.statusCode = 400;
        throw error;
    }

    if (mode !== CLEANUP_MODE && options.connectIds !== undefined) {
        const error = new Error("connectIds can only be used with cleanup mode.");
        error.statusCode = 400;
        throw error;
    }

    const connectIds = options.connectIds || [];
    if (!Array.isArray(connectIds)) {
        const error = new Error("connectIds must be an array of numbers.");
        error.statusCode = 400;
        throw error;
    }

    const normalizedConnectIds = connectIds.map((connectId) => Number(connectId));
    if (normalizedConnectIds.some((connectId) => !Number.isFinite(connectId))) {
        const error = new Error("connectIds must be an array of numbers.");
        error.statusCode = 400;
        throw error;
    }

    return {
        mode,
        dryRun,
        connectIds: normalizedConnectIds,
    };
};

const queryDestroyedParticipants = async ({ connectIds = [] } = {}) => {
    const dataHasBeenDestroyed = toFieldKey(fieldMapping.participantMap.dataHasBeenDestroyed);
    const participantDocs = [];

    if (connectIds.length > 0) {
        for (const chunk of chunkArray(connectIds, MAX_IN_QUERY_SIZE)) {
            const snapshot = await db
                .collection("participants")
                .where(dataHasBeenDestroyed, "==", fieldMapping.yes)
                .where("Connect_ID", "in", chunk)
                .get();
            participantDocs.push(...snapshot.docs);
        }
        return participantDocs;
    }

    const snapshot = await db
        .collection("participants")
        .where(dataHasBeenDestroyed, "==", fieldMapping.yes)
        .get();

    return snapshot.docs;
};

/**
 * Mirror the production data-destruction lookup rules. Most related collections are
 * keyed by Connect_ID; notifications, SSN, and email-status data are token-keyed;
 * DHQ exports are keyed by DHQ username when one exists.
 */
const getDataDestructionCollectionQuerySpec = (collection, participant) => {
    if (TOKEN_KEYED_COLLECTIONS.has(collection)) {
        return {
            collection,
            field: "token",
            value: participant.token,
            skipped: !participant.token,
            skipReason: "Missing token for token-keyed collection lookup.",
        };
    }

    if (DHQ_KEYED_COLLECTIONS.has(collection)) {
        const dhq3UsernameField = toFieldKey(fieldMapping.dhq3Username);
        return {
            collection,
            field: dhq3UsernameField,
            value: participant[dhq3UsernameField],
            skipped: !participant[dhq3UsernameField],
            skipReason: "No DHQ username retained for DHQ-keyed collection lookup.",
        };
    }

    return {
        collection,
        field: "Connect_ID",
        value: participant.Connect_ID,
        skipped: participant.Connect_ID === undefined || participant.Connect_ID === null,
        skipReason: "Missing Connect_ID for Connect_ID-keyed collection lookup.",
    };
};

const getQuerySnapshot = async (collection, field, value) => {
    let query = db.collection(collection).where(field, "==", value);
    if (typeof query.select === "function") {
        query = query.select();
    }
    return query.get();
};

const deleteRefsInBatches = async (refs) => {
    let deletedCount = 0;
    for (const chunk of chunkArray(refs, BATCH_DELETE_LIMIT)) {
        const batch = db.batch();
        chunk.forEach((ref) => batch.delete(ref));
        await batch.commit();
        deletedCount += chunk.length;
    }
    return deletedCount;
};

/**
 * Find, and in cleanup mode delete, related collection documents that should not
 * remain after data destruction. The audit output intentionally records collection
 * counts only; related collection doc IDs may contain normalized emails or other PII.
 */
const auditRelatedCollections = async ({ participant, mode, dryRun }) => {
    const orphanedCollections = [];
    const collectionErrors = [];
    const warnings = [];
    const cleanupActions = [];

    for (const collection of listOfCollectionsRelatedToDataDestruction) {
        const spec = getDataDestructionCollectionQuerySpec(collection, participant);

        if (spec.skipped) {
            if (!DHQ_KEYED_COLLECTIONS.has(collection)) {
                warnings.push(`${collection}: ${spec.skipReason}`);
            }
            continue;
        }

        try {
            const snapshot = await getQuerySnapshot(collection, spec.field, spec.value);
            if (snapshot.size === 0) continue;

            const docs = snapshot.docs.map((doc) => ({
                ref: doc.ref || db.collection(collection).doc(doc.id),
            }));

            orphanedCollections.push({
                collection,
                queryField: spec.field,
                count: docs.length,
            });

            if (mode === CLEANUP_MODE) {
                if (dryRun) {
                    cleanupActions.push({
                        type: "deleteRelatedDocs",
                        collection,
                        count: docs.length,
                        dryRun: true,
                    });
                } else {
                    const deletedCount = await deleteRefsInBatches(docs.map((doc) => doc.ref));
                    cleanupActions.push({
                        type: "deleteRelatedDocs",
                        collection,
                        count: deletedCount,
                    });
                }
            }
        } catch (error) {
            collectionErrors.push(`${collection}: ${error.message}`);
        }
    }

    return {
        orphanedCollections,
        collectionErrors,
        warnings,
        cleanupActions,
    };
};

/**
 * Audit pathology metadata and storage cleanup without writing file names to the
 * participant artifact. Cleanup mode deletes metadata only after bucket access and
 * storage-file deletion have succeeded for that bucket.
 */
const auditPathologyReports = async ({ participant, mode, dryRun, bucketExistsCache }) => {
    const connectId = participant.Connect_ID;
    const pathologyReports = {
        metadataCount: 0,
        invalidMetadataCount: 0,
        storageFileCount: 0,
        bucketsChecked: [],
    };
    const storageErrors = [];
    const cleanupActions = [];

    if (connectId === undefined || connectId === null) {
        return {
            pathologyReports,
            storageErrors: ["pathologyReports: missing Connect_ID for pathology lookup"],
            cleanupActions,
        };
    }

    const fileNameCidStr = toFieldKey(fieldMapping.pathologyReportFilename);
    try {
        const snapshot = await db
            .collection("pathologyReports")
            .where("Connect_ID", "==", connectId)
            .select("bucketName", fileNameCidStr)
            .get();

        pathologyReports.metadataCount = snapshot.size;
        if (snapshot.size === 0) {
            return { pathologyReports, storageErrors, cleanupActions };
        }

        const docsByBucket = new Map();
        for (const doc of snapshot.docs) {
            const docData = typeof doc.data === "function" ? doc.data() : {};
            const { bucketName } = docData;

            if (!isValidPathologyBucketName(bucketName)) {
                pathologyReports.invalidMetadataCount++;
                storageErrors.push(`pathologyReports: Connect_ID ${connectId} has a metadata doc with missing or cross-tier bucketName ${JSON.stringify(bucketName)}`);
                continue;
            }

            if (!docsByBucket.has(bucketName)) {
                docsByBucket.set(bucketName, []);
            }
            docsByBucket.get(bucketName).push(doc);
        }

        const metadataRefsEligibleForDelete = [];
        for (const [bucketName, docs] of docsByBucket.entries()) {
            try {
                pathologyReports.bucketsChecked.push(bucketName);
                const bucket = storage.bucket(bucketName);
                let exists;
                if (bucketExistsCache && bucketExistsCache.has(bucketName)) {
                    exists = bucketExistsCache.get(bucketName);
                } else {
                    [exists] = await bucket.exists();
                    if (bucketExistsCache) bucketExistsCache.set(bucketName, exists);
                }
                if (!exists) {
                    storageErrors.push(`pathologyReports: bucket ${bucketName} does not exist`);
                    continue;
                }

                const [files] = await bucket.getFiles({ prefix: `${connectId}/` });
                pathologyReports.storageFileCount += files.length;

                if (mode === CLEANUP_MODE) {
                    if (dryRun) {
                        cleanupActions.push({
                            type: "deletePathologyStorageFiles",
                            bucketName,
                            count: files.length,
                            dryRun: true,
                        });
                        docs.forEach((doc) => metadataRefsEligibleForDelete.push(doc.ref || db.collection("pathologyReports").doc(doc.id)));
                        continue;
                    }

                    let deletedFileCount = 0;
                    let failedFileCount = 0;
                    await Promise.all(files.map(async (file) => {
                        try {
                            await file.delete();
                            deletedFileCount++;
                        } catch (error) {
                            failedFileCount++;
                        }
                    }));

                    if (failedFileCount > 0) {
                        storageErrors.push(`pathologyReports: failed to delete ${failedFileCount} storage file(s) from bucket ${bucketName}`);
                        continue;
                    }

                    cleanupActions.push({
                        type: "deletePathologyStorageFiles",
                        bucketName,
                        count: deletedFileCount,
                    });

                    docs.forEach((doc) => metadataRefsEligibleForDelete.push(doc.ref || db.collection("pathologyReports").doc(doc.id)));
                }
            } catch (error) {
                storageErrors.push(`pathologyReports: ${bucketName}: ${error.message}`);
            }
        }

        if (mode === CLEANUP_MODE && metadataRefsEligibleForDelete.length > 0) {
            if (dryRun) {
                cleanupActions.push({
                    type: "deletePathologyMetadata",
                    collection: "pathologyReports",
                    count: metadataRefsEligibleForDelete.length,
                    dryRun: true,
                });
            } else {
                const deletedMetadataCount = await deleteRefsInBatches(metadataRefsEligibleForDelete);
                cleanupActions.push({
                    type: "deletePathologyMetadata",
                    collection: "pathologyReports",
                    count: deletedMetadataCount,
                });
            }
        }
    } catch (error) {
        storageErrors.push(`pathologyReports: ${error.message}`);
    }

    return {
        pathologyReports,
        storageErrors,
        cleanupActions,
    };
};

const cleanupUnexpectedStubFields = async ({ participantDocId, validation, dryRun }) => {
    const fieldsToDelete = [
        ...validation.unexpectedStubFields,
        ...validation.unexpectedNestedFields,
    ];

    if (fieldsToDelete.length === 0) return null;

    if (dryRun) {
        return {
            type: "deleteUnexpectedStubFields",
            collection: "participants",
            count: fieldsToDelete.length,
            dryRun: true,
        };
    }

    const updateData = {};
    fieldsToDelete.forEach((fieldPath) => {
        updateData[fieldPath] = FieldValue.delete();
    });

    await db.collection("participants").doc(participantDocId).update(updateData);

    return {
        type: "deleteUnexpectedStubFields",
        collection: "participants",
        count: fieldsToDelete.length,
    };
};

/**
 * Convert raw findings into the participant-level status used by analytics QC.
 */
const resolveParticipantStatus = ({
    validation,
    orphanedCollections,
    pathologyReports,
    collectionErrors,
    storageErrors,
}) => {
    if (collectionErrors.length > 0 || storageErrors.length > 0) return "error";

    if (
        validation.missingRequiredStubFields.length > 0 ||
        validation.unexpectedStubFields.length > 0 ||
        validation.unexpectedNestedFields.length > 0 ||
        orphanedCollections.length > 0 ||
        pathologyReports.metadataCount > 0 ||
        pathologyReports.storageFileCount > 0
    ) {
        return "fail";
    }

    if (validation.missingDefaultRetainedFields.length > 0) {
        return "warn";
    }

    return "pass";
};

/**
 * Audit one destroyed participant stub plus every related data source that production
 * cleanup is expected to remove.
 */
const auditParticipantDataDestruction = async ({ doc, mode, dryRun, runId, projectId, tier, checkedAt, bucketExistsCache }) => {
    const participant = doc.data();
    const destructionIso = participant[toFieldKey(fieldMapping.participantMap.dateTimeDataDestroyed)] || null;
    const appliedPolicy = resolvePolicyForDestruction(destructionIso, tier);
    const validation = validateDestroyedStub(participant, appliedPolicy);
    const relatedResult = await auditRelatedCollections({ participant, mode, dryRun });
    const pathologyResult = await auditPathologyReports({ participant, mode, dryRun, bucketExistsCache });

    const cleanupActions = [
        ...relatedResult.cleanupActions,
        ...pathologyResult.cleanupActions,
    ];
    const collectionErrors = [...relatedResult.collectionErrors];
    const storageErrors = [...pathologyResult.storageErrors];
    const warnings = [...relatedResult.warnings];

    if (mode === CLEANUP_MODE && collectionErrors.length === 0 && storageErrors.length === 0) {
        try {
            const cleanupAction = await cleanupUnexpectedStubFields({
                participantDocId: doc.id,
                validation,
                dryRun,
            });
            if (cleanupAction) cleanupActions.push(cleanupAction);
        } catch (error) {
            collectionErrors.push(`participants: ${error.message}`);
        }
    } else if (
        mode === CLEANUP_MODE &&
        (validation.unexpectedStubFields.length > 0 || validation.unexpectedNestedFields.length > 0)
    ) {
        warnings.push("Skipped unexpected stub cleanup because collection or storage checks had errors.");
    }

    const status = resolveParticipantStatus({
        validation,
        orphanedCollections: relatedResult.orphanedCollections,
        pathologyReports: pathologyResult.pathologyReports,
        collectionErrors,
        storageErrors,
    });

    return {
        runId,
        projectId,
        tier,
        mode,
        dryRun,
        connectId: participant.Connect_ID,
        participantDocId: doc.id,
        policyVersion: validation.policyVersion,
        policyResolution: {
            destructionAt: destructionIso,
            effectiveFrom: appliedPolicy.effectiveFrom,
            appliedDeltas: appliedPolicy.appliedDeltas,
        },
        status,
        orphanedCollections: relatedResult.orphanedCollections,
        pathologyReports: pathologyResult.pathologyReports,
        unexpectedStubFields: validation.unexpectedStubFields,
        unexpectedNestedFields: validation.unexpectedNestedFields,
        missingRequiredStubFields: validation.missingRequiredStubFields,
        missingDefaultRetainedFields: validation.missingDefaultRetainedFields,
        collectionErrors,
        storageErrors,
        cleanupActions,
        warnings,
        checkedAt,
    };
};

/**
 * Roll participant-level findings into the JSON summary uploaded beside the NDJSON file.
 */
const summarizeParticipantResults = ({ participantResults, runId, projectId, tier, mode, dryRun, startedAt, completedAt }) => {
    const statusCounts = { pass: 0, warn: 0, fail: 0, error: 0 };
    const policyVersionsApplied = {};
    const findingCounts = {
        orphanedCollectionDocs: 0,
        pathologyMetadataDocs: 0,
        pathologyStorageFiles: 0,
        unexpectedStubFields: 0,
        unexpectedNestedFields: 0,
        missingRequiredStubFields: 0,
        missingDefaultRetainedFields: 0,
        collectionErrors: 0,
        storageErrors: 0,
    };
    const cleanupCounts = {
        relatedDocsDeleted: 0,
        pathologyStorageFilesDeleted: 0,
        pathologyMetadataDocsDeleted: 0,
        unexpectedStubFieldsDeleted: 0,
    };

    participantResults.forEach((result) => {
        statusCounts[result.status] = (statusCounts[result.status] || 0) + 1;
        policyVersionsApplied[result.policyVersion] = (policyVersionsApplied[result.policyVersion] || 0) + 1;

        findingCounts.orphanedCollectionDocs += result.orphanedCollections.reduce((sum, item) => sum + item.count, 0);
        findingCounts.pathologyMetadataDocs += result.pathologyReports.metadataCount;
        findingCounts.pathologyStorageFiles += result.pathologyReports.storageFileCount;
        findingCounts.unexpectedStubFields += result.unexpectedStubFields.length;
        findingCounts.unexpectedNestedFields += result.unexpectedNestedFields.length;
        findingCounts.missingRequiredStubFields += result.missingRequiredStubFields.length;
        findingCounts.missingDefaultRetainedFields += result.missingDefaultRetainedFields.length;
        findingCounts.collectionErrors += result.collectionErrors.length;
        findingCounts.storageErrors += result.storageErrors.length;

        result.cleanupActions.forEach((action) => {
            if (action.type === "deleteRelatedDocs") cleanupCounts.relatedDocsDeleted += action.count;
            if (action.type === "deletePathologyStorageFiles") cleanupCounts.pathologyStorageFilesDeleted += action.count;
            if (action.type === "deletePathologyMetadata") cleanupCounts.pathologyMetadataDocsDeleted += action.count;
            if (action.type === "deleteUnexpectedStubFields") cleanupCounts.unexpectedStubFieldsDeleted += action.count;
        });
    });

    return {
        runId,
        projectId,
        tier,
        mode,
        dryRun,
        policyVersionsApplied,
        currentPolicyView: describeStubVariables(getCurrentPolicy(tier)),
        participantCounts: {
            total: participantResults.length,
            status: statusCounts,
        },
        findingCounts,
        cleanupCounts,
        boxFiles: null,
        boxUploadError: null,
        emailDelivery: null,
        startedAt,
        completedAt,
    };
};

/**
 * Fetch a Box access token with Client Credentials Grant.
 */
const getBoxAccessToken = async ({
    getSecretFn = getSecret,
    fetchFn = fetch,
} = {}) => {
    const clientIdSecretName = process.env.BOX_CLIENT_ID_SECRET;
    const clientSecretSecretName = process.env.BOX_CLIENT_SECRET;
    const enterpriseId = process.env.BOX_ENTERPRISE_ID;

    if (!clientIdSecretName || !clientSecretSecretName || !enterpriseId) {
        throw new Error("Box upload is not configured. Missing BOX_CLIENT_ID_SECRET, BOX_CLIENT_SECRET, or BOX_ENTERPRISE_ID.");
    }

    const [clientId, clientSecret] = await Promise.all([
        getSecretFn(clientIdSecretName),
        getSecretFn(clientSecretSecretName),
    ]);

    const params = new URLSearchParams({
        grant_type: "client_credentials",
        client_id: clientId,
        client_secret: clientSecret,
        box_subject_type: "enterprise",
        box_subject_id: enterpriseId,
    });

    const response = await fetchFn("https://api.box.com/oauth2/token", {
        method: "POST",
        headers: {
            "Content-Type": "application/x-www-form-urlencoded",
        },
        body: params,
    });
    const responseBody = await response.json();

    if (!response.ok) {
        throw new Error(`Box token request failed with status ${response.status}`);
    }

    if (!responseBody.access_token) {
        throw new Error("Box token response did not include access_token.");
    }

    return responseBody.access_token;
};

const getBoxUploadedFile = (responseBody) => {
    const file = responseBody?.entries?.[0];
    if (!file?.id) {
        throw new Error("Box upload response did not include a file id.");
    }
    return {
        fileId: file.id,
        fileName: file.name,
    };
};

const createBoxUploadForm = ({ fileName, content, contentType, folderId }) => {
    const form = new FormData();
    form.append("attributes", JSON.stringify({
        name: fileName,
        parent: { id: folderId },
    }));
    form.append("file", new Blob([content], { type: contentType }), fileName);
    return form;
};

const uploadBoxFileVersion = async ({
    accessToken,
    fileId,
    fileName,
    content,
    contentType,
    fetchFn = fetch,
}) => {
    const form = new FormData();
    form.append("attributes", JSON.stringify({ name: fileName }));
    form.append("file", new Blob([content], { type: contentType }), fileName);

    const response = await fetchFn(`https://upload.box.com/api/2.0/files/${fileId}/content`, {
        method: "POST",
        headers: {
            Authorization: `Bearer ${accessToken}`,
        },
        body: form,
    });
    const responseBody = await response.json();

    if (!response.ok) {
        throw new Error(`Box file version upload failed for ${fileName} with status ${response.status}`);
    }

    return getBoxUploadedFile(responseBody);
};

const uploadBoxFile = async ({
    accessToken,
    folderId,
    fileName,
    content,
    contentType,
    fetchFn = fetch,
}) => {
    const response = await fetchFn("https://upload.box.com/api/2.0/files/content", {
        method: "POST",
        headers: {
            Authorization: `Bearer ${accessToken}`,
        },
        body: createBoxUploadForm({ fileName, content, contentType, folderId }),
    });
    const responseBody = await response.json();

    if (response.status === 409 && responseBody?.context_info?.conflicts?.id) {
        return uploadBoxFileVersion({
            accessToken,
            fileId: responseBody.context_info.conflicts.id,
            fileName,
            content,
            contentType,
            fetchFn,
        });
    }

    if (!response.ok) {
        throw new Error(`Box upload failed for ${fileName} with status ${response.status}`);
    }

    return getBoxUploadedFile(responseBody);
};

/**
 * Upload the audit summary JSON and participant NDJSON to Box. The summary is uploaded
 * once more after Box returns its file ID so the final summary artifact self-identifies.
 */
const uploadAuditArtifacts = async ({
    summary,
    participantsNdjson,
    fileNames,
    settings = {},
    getSecretFn = getSecret,
    fetchFn = fetch,
} = {}) => {
    const folderId = extractBoxFolderIdFromSettings(settings);
    if (!folderId) {
        throw new Error("Box upload is not configured. appSettings (connectFaas).dataDestructionAudit.boxFolderID is empty or missing.");
    }

    const accessToken = await getBoxAccessToken({ getSecretFn, fetchFn });

    summary.boxFiles = {
        summary: { fileName: fileNames.summaryFileName, fileId: null },
        participants: { fileName: fileNames.participantsFileName, fileId: null },
    };

    const participantUpload = await uploadBoxFile({
        accessToken,
        folderId,
        fileName: fileNames.participantsFileName,
        content: participantsNdjson,
        contentType: "application/x-ndjson",
        fetchFn,
    });
    summary.boxFiles.participants.fileId = participantUpload.fileId;

    const summaryUpload = await uploadBoxFile({
        accessToken,
        folderId,
        fileName: fileNames.summaryFileName,
        content: JSON.stringify(summary, null, 2),
        contentType: "application/json",
        fetchFn,
    });
    summary.boxFiles.summary.fileId = summaryUpload.fileId;

    await uploadBoxFileVersion({
        accessToken,
        fileId: summaryUpload.fileId,
        fileName: fileNames.summaryFileName,
        content: JSON.stringify(summary, null, 2),
        contentType: "application/json",
        fetchFn,
    });

    return {
        summaryFileName: fileNames.summaryFileName,
        participantsFileName: fileNames.participantsFileName,
        summaryFileId: summaryUpload.fileId,
        participantsFileId: participantUpload.fileId,
    };
};

const parseEmailRecipients = (raw) => {
    if (!raw || typeof raw !== "string") return [];
    return raw
        .split(/[,;\s]+/)
        .map((entry) => entry.trim())
        .filter((entry) => entry && !entry.startsWith("TODO_") && validEmailFormat.test(entry));
};

/**
 * Fetch the audit's runtime settings sub-object from
 * appSettings → connectFaas → dataDestructionAudit. One Firestore read per run.
 * Returns the sub-object directly (or {} if the doc/field is missing).
 */
const getDataDestructionAuditSettings = async (getAppSettingsFn = getAppSettings) => {
    const settings = await getAppSettingsFn("connectFaas", ["dataDestructionAudit"]);
    return settings?.dataDestructionAudit ?? {};
};

/**
 * Pure extractor: emailRecipients from a resolved dataDestructionAudit settings
 * sub-object. Accepts a native array (preferred) or a comma/semicolon/whitespace-
 * separated string (graceful fallback). Filters TODO placeholders and entries
 * that fail validEmailFormat.
 */
const extractEmailRecipientsFromSettings = (settings = {}) => {
    const raw = settings.emailRecipients;
    if (Array.isArray(raw)) {
        return raw.filter((entry) =>
            typeof entry === "string" &&
            !entry.startsWith("TODO_") &&
            validEmailFormat.test(entry)
        );
    }
    if (typeof raw === "string") {
        return parseEmailRecipients(raw);
    }
    return [];
};

/**
 * Get boxFolderID from appSettings -> connectFaas -> dataDestructionAudit.boxFolderID.
 */
const extractBoxFolderIdFromSettings = (settings = {}) => {
    const raw = settings.boxFolderID;
    if (typeof raw !== "string") return null;
    if (raw.length === 0 || raw.startsWith("TODO_")) return null;
    return raw;
};

/**
 * Best-effort email delivery of the same JSON summary + NDJSON content that Box receives.
 * Calls SendGrid directly so this module does not depend on notifications.js.
 */
const emailAuditArtifacts = async ({
    summary,
    participantsNdjson,
    fileNames,
    settings = {},
    getSecretFn = getSecret,
    sgClient = sgMail,
} = {}) => {
    const recipients = extractEmailRecipientsFromSettings(settings);
    if (recipients.length === 0) {
        throw new Error("Email delivery is not configured. appSettings (connectFaas).dataDestructionAudit.emailRecipients is empty or missing.");
    }

    const apiKeySecret = process.env.GCLOUD_SENDGRID_SECRET;
    if (!apiKeySecret) {
        throw new Error("Email delivery is not configured. Missing GCLOUD_SENDGRID_SECRET.");
    }

    const apiKey = await getSecretFn(apiKeySecret);
    sgClient.setApiKey(apiKey);

    const summaryJson = JSON.stringify(summary, null, 2);
    const subjectPrefix = summary.dryRun ? "DRY RUN " : "";
    const modeLabel = summary.mode === CLEANUP_MODE ? "Cleanup" : "Audit";
    const statusCounts = summary.participantCounts.status;
    const policyHistogram = Object.entries(summary.policyVersionsApplied || {})
        .map(([version, count]) => `${version}=${count}`)
        .join(" ") || "(none)";

    const msg = {
        to: recipients,
        from: {
            name: process.env.SG_FROM_NAME || "Connect for Cancer Prevention Study",
            email: process.env.SG_FROM_EMAIL || "no-reply-myconnect@mail.nih.gov",
        },
        subject: `${subjectPrefix}Data Destruction ${modeLabel} — ${summary.tier} — ${summary.startedAt}`,
        text: [
            `Run ID: ${summary.runId}`,
            `Tier: ${summary.tier}`,
            `Mode: ${summary.mode}${summary.dryRun ? " (dry run)" : ""}`,
            `Participants: ${summary.participantCounts.total}`,
            `Status counts: pass=${statusCounts.pass} warn=${statusCounts.warn} fail=${statusCounts.fail} error=${statusCounts.error}`,
            `Policy versions applied: ${policyHistogram}`,
            "",
            `Attachments:`,
            `  - ${fileNames.summaryFileName}`,
            `  - ${fileNames.participantsFileName}`,
        ].join("\n"),
        attachments: [
            {
                content: Buffer.from(summaryJson).toString("base64"),
                filename: fileNames.summaryFileName,
                type: "application/json",
                disposition: "attachment",
            },
            {
                content: Buffer.from(participantsNdjson).toString("base64"),
                filename: fileNames.participantsFileName,
                type: "application/x-ndjson",
                disposition: "attachment",
            },
        ],
    };

    await sgClient.send(msg);

    return { recipients, attachments: [fileNames.summaryFileName, fileNames.participantsFileName] };
};

const buildParticipantsNdjson = (participantResults) => (
    participantResults.map((result) => JSON.stringify(result)).join("\n") +
    (participantResults.length > 0 ? "\n" : "")
);

/**
 * Main audit workflow used by the HTTP function and direct unit-test invocation.
 *
 * Box upload and email are both best-effort: failures are logged on the summary and
 * do not abort the audit.
 */
const runDataDestructionAudit = async (rawOptions = {}, dependencies = {}) => {
    const options = normalizeAuditOptions(rawOptions);
    const nowFn = dependencies.now || (() => new Date());
    const started = nowFn();
    const startedAt = started.toISOString();
    const runDate = getAuditDateStamp(started);
    const runId = `${runDate}-${options.mode}${options.dryRun ? "-dryrun" : ""}-${randomUUID()}`;
    const { projectId, tier } = getProjectContext();

    const participantDocs = await queryDestroyedParticipants({
        connectIds: options.mode === CLEANUP_MODE ? options.connectIds : [],
    });

    const settingsFn = dependencies.getDataDestructionAuditSettings || getDataDestructionAuditSettings;
    const settings = await settingsFn(dependencies.getAppSettingsFn || getAppSettings);

    const participantResults = [];
    // Cache bucket.exists() results across participants in this run so we don't repeat per participant.
    const bucketExistsCache = new Map();
    for (const doc of participantDocs) {
        participantResults.push(await auditParticipantDataDestruction({
            doc,
            mode: options.mode,
            dryRun: options.dryRun,
            runId,
            projectId,
            tier,
            checkedAt: nowFn().toISOString(),
            bucketExistsCache,
        }));
    }

    const completedAt = nowFn().toISOString();
    const summary = summarizeParticipantResults({
        participantResults,
        runId,
        projectId,
        tier,
        mode: options.mode,
        dryRun: options.dryRun,
        startedAt,
        completedAt,
    });

    const fileNames = buildAuditFileNames(runDate);
    const participantsNdjson = buildParticipantsNdjson(participantResults);

    const boxFn = dependencies.uploadAuditArtifacts || uploadAuditArtifacts;
    try {
        const uploadResult = await boxFn({
            summary,
            participantsNdjson,
            fileNames,
            settings,
            getSecretFn: dependencies.getSecretFn || getSecret,
            fetchFn: dependencies.fetchFn || fetch,
        });
        summary.boxFiles = {
            summary: { fileName: uploadResult.summaryFileName, fileId: uploadResult.summaryFileId },
            participants: { fileName: uploadResult.participantsFileName, fileId: uploadResult.participantsFileId },
        };
    } catch (error) {
        console.error("Data destruction audit: Box upload failed.", error);
        summary.boxFiles = null;
        summary.boxUploadError = error.message;
    }

    const emailFn = dependencies.emailAuditArtifacts || emailAuditArtifacts;
    try {
        const emailResult = await emailFn({
            summary,
            participantsNdjson,
            fileNames,
            settings,
            getSecretFn: dependencies.getSecretFn || getSecret,
            sgClient: dependencies.sgClient || sgMail,
        });
        summary.emailDelivery = {
            recipients: emailResult.recipients,
            attachments: emailResult.attachments,
            error: null,
        };
    } catch (error) {
        console.error("Data destruction audit: email delivery failed.", error);
        summary.emailDelivery = {
            recipients: null,
            attachments: null,
            error: error.message,
        };
    }

    return {
        summary,
        participantResults,
    };
};

/**
 * HTTP Cloud Run function entry point. Scheduled runs use default read-only audit mode;
 * cleanup mode is manual and requires an exact confirmation string (or dryRun: true).
 */
const auditDataDestruction = async (req, res) => {
    console.log("Received request for auditDataDestruction");

    if (!req || !res) {
        return runDataDestructionAudit();
    }

    if (req.method !== "POST") {
        return res.status(405).json(getResponseJSON("Method not allowed. Use POST.", 405));
    }

    try {
        const result = await runDataDestructionAudit(req.body || {});

        // Both delivery channels failing means the report reached nobody.
        const boxFailed = Boolean(result.summary?.boxUploadError);
        const emailFailed = Boolean(result.summary?.emailDelivery?.error);
        if (boxFailed && emailFailed) {
            console.error(
                `Data destruction audit ${result.summary.runId} delivered no artifacts. ` +
                `Box: ${result.summary.boxUploadError}; Email: ${result.summary.emailDelivery.error}`
            );
        }

        return res.status(200).json({
            ...getResponseJSON("Data destruction audit completed.", 200),
            summary: result.summary,
        });
    } catch (error) {
        const statusCode = error.statusCode || 500;
        console.error("Error in auditDataDestruction:", error);
        return res.status(statusCode).json(getResponseJSON(error.message || "Failed to run data destruction audit.", statusCode));
    }
};

module.exports = {
    AUDIT_MODE,
    CLEANUP_MODE,
    CLEANUP_CONFIRMATION,
    TOKEN_KEYED_COLLECTIONS,
    DHQ_KEYED_COLLECTIONS,
    auditDataDestruction,
    runDataDestructionAudit,
    auditParticipantDataDestruction,
    auditRelatedCollections,
    auditPathologyReports,
    buildAuditFileNames,
    buildParticipantsNdjson,
    getAuditDateStamp,
    getBoxAccessToken,
    uploadBoxFile,
    uploadBoxFileVersion,
    uploadAuditArtifacts,
    emailAuditArtifacts,
    parseEmailRecipients,
    getDataDestructionAuditSettings,
    extractEmailRecipientsFromSettings,
    extractBoxFolderIdFromSettings,
    getDataDestructionCollectionQuerySpec,
    getProjectContext,
    normalizeAuditOptions,
};
