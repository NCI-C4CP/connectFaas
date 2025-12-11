const admin = require('firebase-admin');
const { FieldValue } = require('firebase-admin/firestore');
const { getResponseJSON, getSecret, developmentTier } = require("./shared");
const { extractZipFiles, validateCSVRow, streamCSVRows } = require('./fileProcessing');
const { normalizeIso8601Timestamp } = require('./validation');
const fieldMapping = require('./fieldToConceptIdMapping');
const db = admin.firestore();

const API_ROOT = 'https://www.dhq3.org/api-home/root/study-list/';
const PROCESSING_CHUNK_SIZE = 1000;
const MILLISECONDS_PER_DAY = 86400000;
const MEMORY_WARNING_THRESHOLD = 1750;

const dhqCompletionStatusMapping = {
    [fieldMapping.notStarted]: fieldMapping.dhq3NotYetBegun,
    [fieldMapping.started]: fieldMapping.dhq3InProgress,
    [fieldMapping.submitted]: fieldMapping.dhq3Completed,
}

const getDHQHeaders = (method, dhqToken) => {
    return method === "GET"
        ? { "Authorization": "Token " + dhqToken }
        : { "Authorization": "Token " + dhqToken, "Content-Type": "application/json" };
}

/**
 * Fetch data from the DHQ API.
 * @param {string} url - The URL to fetch from.
 * @param {string} method - The HTTP method to use.
 * @param {Object} headers - The headers to send with the request.
 * @param {Object} data - The data to send with the request.
 * @param {number} attempt - The current attempt number.
 * @returns {Promise<Object>} - The response from the DHQ API.
 * 
 * From DHQ API docs:
 * 200/201 = success
 * 400 = bad request (don't retry)
 * 401 = invalid auth token (don't retry)
 * 404 = no permission/not found (don't retry)
 * 5xx = server errors (retry)
 */
const fetchDHQAPIData = async (url, method, headers, data, attempt = 1) => {
    const maxRetries = 3;
    const retryDelay = 2000; // 2 seconds
    
    const options = { method, headers };
    if (data !== null && data !== undefined) {
        options.body = JSON.stringify(data);
    }

    try {
        const response = await fetch(url, options);
        
        if (!response.ok) {
            const errorText = await response.text();

            if (response.status >= 500 && attempt <= maxRetries) {
                console.warn(`DHQ API server error (${response.status}), attempt ${attempt}/${maxRetries}. Retrying in ${retryDelay}ms...`);
                await new Promise(resolve => setTimeout(resolve, retryDelay));
                return fetchDHQAPIData(url, method, headers, data, attempt + 1);
            }
            
            let errorMessage = `DHQ API Error ${response.status}`;
            if (response.status === 400) errorMessage += ' (Bad Request - check parameters)';
            else if (response.status === 401) errorMessage += ' (Invalid or missing auth token)';
            else if (response.status === 404) errorMessage += ' (No permission or resource not found)';
            
            errorMessage += `: ${errorText}`;
            
            throw new Error(errorMessage);
        }

        const contentType = response.headers.get("Content-Type");

        switch (contentType) {
            case "application/json":
                return await response.json();
            case "application/pdf":
            case "application/zip":
                const responseBlob = await response.blob();
                const arrayBuffer = await responseBlob.arrayBuffer();
                const base64String = Buffer.from(arrayBuffer).toString('base64');
                return { data: base64String, contentType: contentType };
            default:
                return await response.text();
        }

    } catch (error) {
        if (attempt <= maxRetries) {
            console.warn(`DHQ API network error on attempt ${attempt}/${maxRetries}. Retrying in ${retryDelay}ms...`);
            await new Promise(resolve => setTimeout(resolve, retryDelay));
            return fetchDHQAPIData(url, method, headers, data, attempt + 1);
        }
        
        console.error("DHQ API call failed:", error);
        throw error;
    }
}

/**
 * Cloud Function handler. Called by index.js onRequest wrapper.
 * DHQ's survey completion doesn't provide a direct way to update the participant's completion status.
 * This function checks 'started' participants' DHQ survey status nightly and updates their progress status in Firestore.
 * @param {Request} req - HTTP request
 * @param {Response} res - HTTP response
 */

const scheduledSyncDHQ3Status = async (req, res) => {
    console.log('Scheduled update of DHQ3 progress status started.');
    let updatesToProcess = 0;
    let successCount = 0;
    let errorCount = 0;

    try {
        const participantsSnapshot = await db.collection('participants')
            .where(fieldMapping.dhq3SurveyStatus.toString(), '==', fieldMapping.started)
            .select('state', fieldMapping.dhq3StudyID.toString(), fieldMapping.dhq3Username.toString(), fieldMapping.dhq3SurveyStatus.toString())
            .get();

        if (participantsSnapshot.empty) {
            console.log('No participants with DHQ survey status "started" found.');
            return res.status(200).json(getResponseJSON("No participants with DHQ survey status 'started' to update.", 200));
        }

        updatesToProcess = participantsSnapshot.size;
        console.log(`Found ${updatesToProcess} participants to process.`);

        // Get DHQ configuration (token and batch parameters)
        const { dhqToken, syncBatchSize, syncBatchDelay } = await getDHQConfig();
        if (!dhqToken) {
            console.error('DHQ API token not found in secret manager.');
            return res.status(500).json(getResponseJSON("DHQ API token not found in secret manager.", 500));
        }

        // Process in batches to avoid overwhelming the DHQ API
        const participants = participantsSnapshot.docs;
        console.log(`Processing ${participants.length} participants in batches of ${syncBatchSize} with ${syncBatchDelay}ms delay between batches`);

        for (let i = 0; i < participants.length; i += syncBatchSize) {
            const batch = participants.slice(i, i + syncBatchSize);
            const batchNumber = Math.floor(i / syncBatchSize) + 1;
            const totalBatches = Math.ceil(participants.length / syncBatchSize);
            const batchPromises = [];
            
            for (const participantDoc of batch) {
                const participantData = participantDoc.data();
                const uid = participantData.state.uid;
                const dhq3StudyID = participantData[fieldMapping.dhq3StudyID];
                const dhq3Username = participantData[fieldMapping.dhq3Username];
                const dhq3SurveyStatus = participantData[fieldMapping.dhq3SurveyStatus];
                const dhq3SurveyStatusExternal = participantData[fieldMapping.dhq3SurveyStatusExternal];

                if (!uid) {
                    console.error(`Participant document ${participantDoc.id} missing state.uid. Skipping.`);
                    errorCount++;
                    continue;
                }
                if (!dhq3Username) {
                    console.error(`Participant ${uid} has no DHQ username. Skipping.`);
                    errorCount++;
                    continue;
                }
                if (!dhq3StudyID) {
                    console.error(`Participant ${uid} has no DHQ studyID (${fieldMapping.dhq3StudyID}). Skipping.`);
                    errorCount++;
                    continue;
                }

                batchPromises.push(
                    syncDHQ3RespondentInfo(dhq3StudyID, dhq3Username, dhq3SurveyStatus, dhq3SurveyStatusExternal, uid, dhqToken)
                        .then(result => ({ status: 'fulfilled', value: { uid, result } }))
                        .catch(error => ({ status: 'rejected', reason: { uid, error } }))
                );
            }

            const batchResults = await Promise.allSettled(batchPromises);
            batchResults.forEach(result => {
                if (result.status === 'fulfilled') {
                    successCount++;
                } else {
                    console.error(`Error updating participant ${result.reason.uid}:`, result.reason.error);
                    errorCount++;
                }
            });
            
            console.log(`Batch ${batchNumber} completed out of ${totalBatches} batches. Success: ${batchResults.filter(r => r.status === 'fulfilled').length}, Errors: ${batchResults.filter(r => r.status === 'rejected').length}`);
            
            // Add delay between batches
            if (i + syncBatchSize < participants.length) {
                await new Promise(resolve => setTimeout(resolve, syncBatchDelay));
            }
        }

        console.log(`DHQ Status Sync Summary: Total Found=${updatesToProcess}, Succeeded=${successCount}, Failed/Skipped=${errorCount}`);
        return res.status(200).json(getResponseJSON("DHQ survey status sync completed successfully.", 200));

    } catch (error) {
        console.error("Error running scheduledSyncDHQ3Status:", error);
        return res.status(500).json(getResponseJSON("Error in scheduledSyncDHQ3Status: " + error.message, 500));
    }
}

/**
 * The 'Respondent Get Information' endpoint exposes the participant's survey status and stats.
 * We use it to sync the DHQ respondent information with the Connect participant profile.
 * @param {string} studyID - The ID of the study the respondent is assigned to.
 * @param {string} respondentUsername - The DHQ-assigned username for the respondent.
 * @param {number} dhq3SurveyStatus - The current status of the participant's DHQ survey in Firestore.
 * @param {number} dhq3SurveyStatusExternal - The current external status of the participant's DHQ survey (from DHQ).
 * @param {string} uid - The participant's Connect UID.
 * @param {string} dhqToken - The DHQ API token for authentication. If not provided, it will be fetched from the secret manager.
 * @return {Promise<Object>} - Returns a promise that resolves to the respondent's information.
 *     username: 'CCC00002',
 *     active_status: 1 = "Active", 2 = "Inactive"
 *     questionnaire_status: 1 = "Not yet begun", 2 = "In progress", 3 = "Completed" 
 *     status_date: e.g. '2024-04-11T16:29:53.601000Z', 
 *     device_used: 'PC', 
 *     browser_used: 'Safari', 
 *     total_time_logged_in: e.g. '00:05:38.280738',
 *     number_of_times_logged_in: 2,
 *     login_durations: [ '00:00:46.688120', '00:04:51.592618' ], 
 *     language: 'en' or 'es'
 *     viewed_rnr_report: bool, 
 *     downloaded_rnr_report: bool 
 *     viewed_hei_report: bool
 *     downloaded_hei_report: bool
 */

const syncDHQ3RespondentInfo = async (studyID, respondentUsername, dhq3SurveyStatus, dhq3SurveyStatusExternal, uid, dhqToken = null) => {
    try {
        // The DHQ API token is required for authentication. It's passed in by the scheduled Cloud (bulk) function.
        // It needs to be fetched by the one-off version (called from the PWA).
        if (!dhqToken) {
            dhqToken = await getSecret(process.env.DHQ_TOKEN);
            if (!dhqToken) {
                console.error('DHQ API token not found in secret manager.');
                throw new Error('DHQ API token not found.');
            }
        }

        studyID = studyID.replace(/^study_/, '');

        const url = `${API_ROOT}${studyID}/respondent-get-information/`;
        const method = "POST";
        const headers = getDHQHeaders(method, dhqToken);
        const data = { respondent_username: respondentUsername };

        const results = await fetchDHQAPIData(url, method, headers, data);

        // If the the survey is completed in DHQ, update the participant profile with the completed timestamp and flag.
        if (results?.questionnaire_status === 3 && (dhq3SurveyStatus !== fieldMapping.submitted || dhq3SurveyStatusExternal !== fieldMapping.dhq3Completed)) {
            await updateDHQ3ProgressStatus(
                true,
                fieldMapping.submitted,
                results?.status_date || '',
                results?.viewed_hei_report || false,
                results?.language?.toLowerCase(),
                uid
            );

        // If the survey is only started in DHQ, sanity-check the status with the participant profile. This should always be set on the survey's 'start' click in the PWA.
        } else if (results?.questionnaire_status === 2 && (dhq3SurveyStatus !== fieldMapping.started || dhq3SurveyStatusExternal !== fieldMapping.dhq3InProgress)) {
            await updateDHQ3ProgressStatus(
                false,
                fieldMapping.started,
                results?.status_date || '',
                results?.viewed_hei_report || false,
                results?.language?.toLowerCase(),
                uid
            );
        }

        return results;

    } catch (error) {
        console.error("Error:", error);
        throw new Error(`Failed to get respondent info: ${error.message}`);
    }
}

/**
 * Update the participant's Firestore profile with the DHQ3 survey progress status.
 * If survey is completed, set the DHQ completion timestamp.
 * The DHQ API provides the status_date field with the completion timestamp, but it needs to be normalized to the Connect ISO 8601 format (3 digits in the milliseconds category).
 * @param {boolean} isSubmitted - True if the DHQ3 survey has been completed (API questionnaire_status 3).
 * @param {number} completionStatus - The Connect completion status to set in the participant profile (e.g. fieldMapping.started or fieldMapping.submitted).
 * @param {string} dhqSubmittedTimestamp - The ISO 8601 Timestamp from the DHQ API's status_date field. Populated by DHQ on survey completion.
 * @param {boolean} viewedHEIReport - True if the participant has viewed the HEI report (from DHQ API).
 * @param {string} language - The language of the participant's survey (from DHQ API). Expected values: 'en', 'es'.
 * @param {string} uid - The participant's Connect UID.
 * @returns {Promise<void>} - Promise resolves when the participant profile is updated.
 */

const updateDHQ3ProgressStatus = async (isSubmitted, completionStatus, dhqSubmittedTimestamp, viewedHEIReport, language, uid) => {
    try {
        let updateData = {
            [fieldMapping.dhq3SurveyStatus]: completionStatus,
            [fieldMapping.dhq3SurveyStatusExternal]: dhqCompletionStatusMapping[completionStatus],
        };

        if (isSubmitted) {
            if (language === 'en') {
                updateData[fieldMapping.dhq3Language] = fieldMapping.english;
            } else if (language === 'es') {
                updateData[fieldMapping.dhq3Language] = fieldMapping.spanish;
            } else if (!language) {
                // Language not specified by DHQ API. No language field will be set
                console.error(`No language specified for participant ${uid} - language field will not be set.`);
            } else {
                console.error(`Error: Invalid language ${language} for participant ${uid}.`);
            }

            if (dhqSubmittedTimestamp) {
                const parsedTimestamp = normalizeIso8601Timestamp(dhqSubmittedTimestamp);
                updateData[fieldMapping.dhq3SurveyCompletionTime] = parsedTimestamp;
            } else {
                updateData[fieldMapping.dhq3SurveyCompletionTime] = new Date().toISOString();
            }

            const viewedStatus = viewedHEIReport === true || viewedHEIReport === 'true' ? fieldMapping.reportStatus.viewed : fieldMapping.reportStatus.unread;

            // Trigger the report availability at survey completion
            updateData[fieldMapping.dhq3HEIReportStatusInternal] = fieldMapping.reportStatus.unread;
            updateData[fieldMapping.dhq3HEIReportStatusExternal] = viewedStatus;
        }

        const participantSnapshot = await db.collection('participants')
            .where('state.uid', '==', uid)
            .select()
            .get();

        if (participantSnapshot.empty) {
            console.error(`Error: No participant found with UID ${uid}.`);
            throw new Error(`Participant with UID ${uid} not found.`);
        }

        const participantDocRef = participantSnapshot.docs[0].ref;
        await participantDocRef.update(updateData);

    } catch (error) {
        console.error("Error: updateDHQ3ProgressStatus():", error);
        throw new Error(`Failed to update DHQ3 progress status: ${error.message}`);
    }
}

const updateDHQReportViewedStatus = async (uid, studyID, respondentUsername, isDeclined = false, dhqToken = null) => {

    try {
        if (!dhqToken) {
            dhqToken = await getSecret(process.env.DHQ_TOKEN);
            if (!dhqToken) {
                console.error('DHQ API token not found in secret manager.');
                throw new Error('DHQ API token not found.');
            }
        }

        studyID = studyID.replace(/^study_/, '');

        const url = `${API_ROOT}${studyID}/respondent-get-information/`;
        const method = "POST";
        const headers = getDHQHeaders(method, dhqToken);
        const data = { respondent_username: respondentUsername };

        const [dhqAPIResults, participantSnapshot] = await Promise.all([
            !isDeclined ? fetchDHQAPIData(url, method, headers, data) : Promise.resolve(null),
            db.collection('participants').where('state.uid', '==', uid)
                .select(
                    fieldMapping.dhq3HEIReportStatusInternal.toString(),
                    fieldMapping.dhq3HEIReportStatusExternal.toString(),
                    fieldMapping.dhq3HEIReportFirstViewedISOTime.toString(),
                    fieldMapping.dhq3HEIReportFirstDeclinedISOTime.toString()
                )
                .get()
        ]);

        if (participantSnapshot.empty) {
            console.error(`Error: No participant found with UID ${uid}.`);
            throw new Error(`Participant with UID ${uid} not found.`);
        }

        const participantDocRef = participantSnapshot.docs[0].ref;
        const participantData = participantSnapshot.docs[0].data();

        const currentViewedInternalStatus = participantData[fieldMapping.dhq3HEIReportStatusInternal];
        const currentViewedExternalStatus = participantData[fieldMapping.dhq3HEIReportStatusExternal];
        const currentFirstViewedISOTime = participantData[fieldMapping.dhq3HEIReportFirstViewedISOTime];
        const currentFirstDeclinedISOTime = participantData[fieldMapping.dhq3HEIReportFirstDeclinedISOTime];

        let updateObj = {};

        // Declined path (Only set declined timestamp on the first decline action)
        if (isDeclined) {
            if (currentViewedInternalStatus !== fieldMapping.reportStatus.declined) {
                updateObj[fieldMapping.dhq3HEIReportStatusInternal] = fieldMapping.reportStatus.declined;
            }

            if (!currentFirstDeclinedISOTime) {
                updateObj[fieldMapping.dhq3HEIReportFirstDeclinedISOTime] = new Date().toISOString();
            }
            
        // Viewed path (Only set viewed timestamp on the first view action)
        // Update the external status based on the DHQ API results.
        } else {
            if (currentViewedExternalStatus !== fieldMapping.reportStatus.viewed) {
                updateObj[fieldMapping.dhq3HEIReportStatusExternal] = dhqAPIResults?.viewed_hei_report === true || dhqAPIResults?.viewed_hei_report === 'true'
                    ? fieldMapping.reportStatus.viewed
                    : fieldMapping.reportStatus.unread;
            }

            if (currentViewedInternalStatus !== fieldMapping.reportStatus.viewed) {
                updateObj[fieldMapping.dhq3HEIReportStatusInternal] = fieldMapping.reportStatus.viewed;
            }

            if (!currentFirstViewedISOTime) {
                updateObj[fieldMapping.dhq3HEIReportFirstViewedISOTime] = new Date().toISOString();
            }
        }

        if (Object.keys(updateObj).length > 0) {
            await participantDocRef.update(updateObj);
        }

    } catch (error) {
        console.error("Error: updateDHQ3ReportViewedStatus():", error);
        throw new Error(`Failed to update DHQ3 report viewed status: ${error.message}`);
    }
}

/**
 * Retrieve the DHQ HEI report for a participant.
 * @param {string} studyID - The DHQ study ID.
 * @param {string} respondentUsername - The respondent's DHQ username.
 * @returns {Promise<Object>} - The DHQ HEI report data.
 */

const retrieveDHQHEIReport = async (studyID, respondentUsername) => {
    try {
        const dhqToken = await getSecret(process.env.DHQ_TOKEN);
        if (!dhqToken) {
            console.error('DHQ API token not found in secret manager.');
            throw new Error('DHQ API token not found.');
        }

        studyID = studyID.replace(/^study_/, '');
        
        const url = `${API_ROOT}${studyID}/download-hei-report/`;
        const method = "POST";
        const headers = getDHQHeaders(method, dhqToken);
        const data = { respondent_username: respondentUsername };

        // Returns the HEI report as a base64-encoded string.
        // { data: Base64-encoded PDF data, contentType: 'application/pdf' }
        const reportData = await fetchDHQAPIData(url, method, headers, data);
        
        if (!reportData || reportData.contentType !== 'application/pdf') {
            throw new Error('Failed to retrieve HEI report or report is not a PDF.');
        }

        return reportData;

    } catch (error) {
        console.error("Error: retrieveDHQHEIReport():", error);
        throw new Error(`Failed to retrieve DHQ HEI report: ${error.message}`);
    }
}

/**
 * Atomically allocate a DHQ3 credential from the availableCredentials pool (Firestore -> DHQ -> studyID -> availableCredentials) to a participant.
 * Also set the DHQ3 survey status to "started" and the start time to the current time.
 * @param {Array<string>} availableCredentialPools - Array of DHQ3 study IDs with available credentials (a credential provides access to the DHQ3 survey).
 * @param {string} uid - The UID of the participant in Connect App. 
 * @returns {Promise<Object>} - Promise resolves to the allocated credential object.
 */

const allocateDHQ3Credential = async (availableCredentialPools, uid) => {
    try {
        // Fetch the participant from Firestore and sanity check for an existing DHQ credential.
        const participantSnapshot = await db.collection('participants').where('state.uid', '==', uid).get();
        if (participantSnapshot.size !== 1) {
            console.error(`Error: Expected exactly one participant with UID ${uid}, but found ${participantSnapshot.size}.`);
            throw new Error(`Participant with UID ${uid} not found or multiple participants found.`);
        }
        
        const participant = participantSnapshot.docs[0].data();
        const participantDocRef = participantSnapshot.docs[0].ref;

        const dhqUUID = participant[fieldMapping.dhq3UUID];
        if (dhqUUID) {
            return {
                [fieldMapping.dhq3StudyID]: participant?.[fieldMapping.dhq3StudyID],
                [fieldMapping.dhq3Username]: participant?.[fieldMapping.dhq3Username],
                [fieldMapping.dhq3UUID]: participant?.[fieldMapping.dhq3UUID],
                [fieldMapping.dhq3SurveyStatus]: participant?.[fieldMapping.dhq3SurveyStatus],
                [fieldMapping.dhq3SurveyStatusExternal]: participant?.[fieldMapping.dhq3SurveyStatusExternal],
                [fieldMapping.dhq3SurveyStartTime]: participant?.[fieldMapping.dhq3SurveyStartTime],
            };
        }

        // Iterate availableCredentialPools and allocate a DHQ3 credential.
        let allocatedCredentialData = null;
        for (const dhqStudy of availableCredentialPools) {
            const credentialsCollectionRef = db.collection('dhq3SurveyCredentials').doc(dhqStudy).collection('availableCredentials');
            
            let isAllocationSuccessful = false;
            let isCredentialPoolEmpty = false;

            try {
                await db.runTransaction(async (transaction) => {
                    // Query for an available credential
                    // If empty, no credentials are left in this study pool (10k per study).
                    // If credential is found (9999 of every 10000 cases):
                    //      •UUID is the document ID.
                    //      •The document contains the 'username' property.
                    //      •The Study ID is the parent document ID of availableCollections.
                    //      •Delete the DHQ credential from the pool and update the participant profile with the allocated credential.
                    const credentialQuery = credentialsCollectionRef.limit(1);
                    const credentialSnapshot = await transaction.get(credentialQuery);

                    if (credentialSnapshot.empty) {
                        isCredentialPoolEmpty = true;

                    } else {
                        const credentialDoc = credentialSnapshot.docs[0];
                        const credentialData = credentialDoc.data();

                        // Sanity check the username
                        const usernameCheckQuery = db.collection('participants').where(fieldMapping.dhq3Username.toString(), '==', credentialData.username).limit(1);
                        const existingUsernameSnapshot = await transaction.get(usernameCheckQuery);

                        // This should never happen given DHQ's sequential username generation and our use of UUID as the document ID (guaranteed unique).
                        if (!existingUsernameSnapshot.empty) {
                            throw new Error(`Data integrity error: Username ${credentialData.username} already allocated.`);
                        }

                        allocatedCredentialData = {
                            [fieldMapping.dhq3StudyID]: dhqStudy,
                            [fieldMapping.dhq3Username]: credentialData.username,
                            [fieldMapping.dhq3UUID]: credentialDoc.id,
                            [fieldMapping.dhq3SurveyStatus]: fieldMapping.notStarted,
                            [fieldMapping.dhq3SurveyStatusExternal]: fieldMapping.dhq3NotYetBegun,
                        };

                        transaction.delete(credentialDoc.ref);
                        transaction.update(participantDocRef, allocatedCredentialData);

                        isAllocationSuccessful = true;
                        isCredentialPoolEmpty = false;
                    }
                });

                // Break on successful allocation.
                // If the current study's availableCredential pool is empty,
                // Mark it as depleted in appSettings, then continue the loop to the next study pool.
                // We don't want to search that credential pool in the future.

                if (isAllocationSuccessful) {
                    break;
                }
                
                if (isCredentialPoolEmpty) {
                    await markDHQ3CredentialAsDepleted(dhqStudy);
                }

            } catch (transactionError) {
                console.error(`Transaction attempt failed for study ${dhqStudy}, participant ${participant['Connect_ID']}. Error:`, transactionError);
            }
        }

        if (allocatedCredentialData) {
            return allocatedCredentialData;

        } else {
            console.error(`Error: Failed to allocate DHQ3 credential for participant ${participant['Connect_ID']}. Check study availableCredential pools. Or transactions failed.`);
            throw new Error('Failed to allocate DHQ3 credential - all pools empty or an error occurred.');
        }

    } catch (error) {
        console.error("Error:", error);
        throw new Error(`Failed to allocate DHQ3 credential: ${error.message}`);
    }
}

/**
 * Once all availableCredentials for a DHQ study are used, mark the study as depleted in appSettings.
 * This will stop from searching that study's availableCredentials pool in the future.
 * @param {string} studyID - The ID of the DHQ study to mark as depleted.
 * @returns {Promise<void>} - Promise resolves when the study is marked as depleted.
 */

const markDHQ3CredentialAsDepleted = async (studyID) => {
    try {
        const appSettingsQuery = await db.collection('appSettings').where('appName', '==', 'connectApp').get();
        if (appSettingsQuery.empty) {
            console.error(`Error: No app settings found for connectApp.`);
            throw new Error('App settings not found.');
        }

        // Update the appSettings document with the new depleted credentials array.
        // arrayUnion handles duplicate checking.
        const appSettingsRef = appSettingsQuery.docs[0].ref;
        await appSettingsRef.update({
            ['dhq.dhqDepletedCredentials']: FieldValue.arrayUnion(studyID)
        });

    } catch (error) {
        console.error(`Error marking study ${studyID} as depleted:`, error);
        throw new Error(`Failed to mark study ${studyID} as depleted: ${error.message}`);
    }
}

/**
 * Scheduled function to count available DHQ credentials.
 * Count until we hit the lowCredentialWarningThreshold. This threshold is stored in Firestore -> appSettings -> dhq.
 * If >= lowCredentialWarningThreshold credentials remain, stop counting. If < lowCredentialWarningThreshold, send a warning email to the Connect team.
 * Credentials are refreshed with a locally run function after the Connect team creates a new 'study' in DHQ.
 */

const scheduledCountDHQ3Credentials = async (req, res) => {
    console.log('Scheduled count of available DHQ credentials started.');
    
    try {
        const appSettingsQuery = await db.collection('appSettings')
            .where('appName', '==', 'connectApp')
            .select('dhq')
            .get();

        if (appSettingsQuery.empty) {
            console.error('No app settings found for connectApp.');
            return res.status(500).json(getResponseJSON("App settings not found.", 500));
        }

        const appSettingsData = appSettingsQuery.docs[0].data();
        const lowCredentialWarningThreshold = appSettingsData.dhq.lowCredentialWarningThreshold || 1000;
        const dhqStudyIDs = appSettingsData.dhq.dhqStudyIDs || [];                      // List of DHQ study IDs from appSettings.
        const depletedDHQStudyIDs = appSettingsData.dhq.dhqDepletedCredentials || [];   // List of DHQ study IDs without availableCredentials (skip these in credential search).
        const availableCredentialPools = dhqStudyIDs.filter(studyID => !depletedDHQStudyIDs.includes(studyID));
        let runningCredentialCount = 0;

        for (const studyID of availableCredentialPools) {
            const credentialCollectionRef = db.collection('dhq3SurveyCredentials').doc(studyID).collection('availableCredentials');
            const credentialCount = await credentialCollectionRef.count().get().then(snapshot => snapshot.data().count);
            runningCredentialCount += credentialCount;
            
            // Break if we hit the threshold. Will return from here in the vast majority of cases.
            if (runningCredentialCount >= lowCredentialWarningThreshold) {
                console.log(`Running credential count is above threshold. ${runningCredentialCount} credentials remaining. Last checked study: ${studyID}. No action needed.`);
                return res.status(200).json(getResponseJSON("Running credential count is above threshold. No action needed.", 200));
            }
        }

        // Create an email warning for the CCC team. This will run daily until the credentials are replenished.
        console.warn(`Running credential count is below threshold: ${runningCredentialCount} Credentials remaining. Sending warning email.`);

        const sendGridSecret = await getSecret(process.env.GCLOUD_SENDGRID_SECRET);
        const sgMail = require('@sendgrid/mail');
        sgMail.setApiKey(sendGridSecret);

        const emailTo = 'ConnectCC@nih.gov';

        const developmentTier = process.env.GCLOUD_PROJECT === 'nih-nci-dceg-connect-prod-6d04'
            ? 'PROD'
            : process.env.GCLOUD_PROJECT === 'nih-nci-dceg-connect-stg-5519'
                ? 'STAGE'
                : 'DEV';

        const warningEmail = {
            to: emailTo, 
            from: {
                name: process.env.SG_FROM_NAME || 'Connect for Cancer Prevention Study',
                email: process.env.SG_FROM_EMAIL || 'no-reply-myconnect@mail.nih.gov'
            },
            subject: `Low DHQ3 Credential Pool Warning: DHQ3 credentials are running low in ${developmentTier}.`,
            html: `<p>Dear Connect Team,</p>
                <p>This is an automated WARNING message to inform you that the available DHQ3 credentials are running low in ${developmentTier}. </p>
                <p>Please take action to replenish the credentials as soon as possible.</p>
                <p>Thank you,</p>
                <p>Connect for Cancer Prevention Study Team</p>`,
        };

        sgMail.send(warningEmail).then(() => {
            console.log('Email sent to ' + emailTo)
        }).catch((error) => {
            throw new Error(`Error: scheduledCountDHQ3Credentials Failed to send warning email: ${error.message}`);
        });

        return res.status(200).json(getResponseJSON(`Available DHQ credentials check completed successfully. Low credential warning email sent to ${emailTo}`, 200));

    } catch (error) {
        console.error("Error updating available DHQ credentials count:", error);
        return res.status(500).json(getResponseJSON("Failed to complete available DHQ credentials count.", 500));
    }
};

/**
 * Get DHQ configuration from app settings and validate DHQ token.
 * @returns {Promise<Object>} - Object containing dhqStudyIDs array, dhqToken string, and date filtering settings
 * @throws {Error} - If app settings not found, no study IDs, or DHQ token not found
 */
const getDHQConfig = async () => {
    const appSettingsQuery = await db.collection('appSettings')
        .where('appName', '==', 'connectApp')
        .select('dhq')
        .get();

    if (appSettingsQuery.empty) {
        throw new Error('App settings not found.');
    }

    const appSettingsData = appSettingsQuery.docs[0].data();
    const dhqStudyIDs = appSettingsData.dhq.dhqStudyIDs || [];
    const useDateFiltering = appSettingsData.dhq.useDateFiltering || false;
    const useLanguageFlag = appSettingsData.dhq.useLanguageFlag || false;
    const dateFilterStartDaysAgo = appSettingsData.dhq.dateFilterStartDaysAgo || 90; // Default to 90 days ago
    const dateFilterEndDaysAgo = appSettingsData.dhq.dateFilterEndDaysAgo || 0; // Default to "today"
    const syncBatchSize = appSettingsData.dhq.syncBatchSize || 1; // Default to sequential processing
    const syncBatchDelay = appSettingsData.dhq.syncBatchDelay || 0; // Default to 0ms delay between batches
    
    if (dhqStudyIDs.length === 0) {
        throw new Error('No DHQ study IDs found in app settings.');
    }

    const recentlyCompletedDHQStudyIDs = await getRecentlyCompletedDHQStudies(dhqStudyIDs, dateFilterStartDaysAgo);
    if (recentlyCompletedDHQStudyIDs.length === 0) {
        console.log(`No DHQ studies have been completed in the last ${dateFilterStartDaysAgo} days.`);
    }

    const dhqToken = await getSecret(process.env.DHQ_TOKEN);
    if (!dhqToken) {
        throw new Error('DHQ API token not found.');
    }

    return { dhqStudyIDs, dhqToken, recentlyCompletedDHQStudyIDs, useDateFiltering, useLanguageFlag, dateFilterStartDaysAgo, dateFilterEndDaysAgo, syncBatchSize, syncBatchDelay };
};

/**
 * Get a list of DHQ studies with survey completions in the last n days (setting @ Firestore -> appSettings -> dhq.dateFilterStartDaysAgo).
 * Based on query from the Firestore 'participants' collection. Limit(1) because we only need to know whether a recent completion exists.
 * We'll only process studies with survey completions in the last n days.
 * @param {Array<string>} studyIDs - Array of study IDs from appSettings.dhq.dhqStudyIDs.
 * @param {number} dateFilterStartDaysAgo - Number of days to look back for survey completions.
 * @returns {Promise<Array<string>>} - Array of study IDs that have recent completions.
 */
const getRecentlyCompletedDHQStudies = async (studyIDs, dateFilterStartDaysAgo = 90) => {
    // Calculate the cutoff date as an ISO string.
    const cutoffDate = new Date(Date.now() - dateFilterStartDaysAgo * MILLISECONDS_PER_DAY).toISOString();
    console.log(`Looking for DHQ completions since: ${cutoffDate} (${dateFilterStartDaysAgo} days ago)`);

    const studyQueries = studyIDs.map(async studyID => {
        const participantsQuery = await db.collection('participants')
            .where(fieldMapping.dhq3StudyID.toString(), '==', studyID)
            .where(fieldMapping.dhq3SurveyStatus.toString(), '==', fieldMapping.submitted)
            .where(fieldMapping.dhq3SurveyCompletionTime.toString(), '>=', cutoffDate)
            .limit(1)
            .get();

        if (participantsQuery.empty) {
            return { studyID, hasRecentCompletions: false };
        }

        console.log(`Found recent completions for study ${studyID}.`);
            return { studyID, hasRecentCompletions: true };
    });

    const results = await Promise.allSettled(studyQueries);

    const recentlyCompletedDHQStudyIDs = [];
    let errorCount = 0;

    results.forEach((result, index) => {
        if (result.status === 'fulfilled') {
            const { studyID, hasRecentCompletions } = result.value;
            if (hasRecentCompletions) {
                recentlyCompletedDHQStudyIDs.push(studyID);
            }
        } else {
            console.error(`Query for study ${studyIDs[index]} failed:`, result.reason);
            errorCount++;
        }
    });

    console.log(`Found ${recentlyCompletedDHQStudyIDs.length} studies with recent completions:`, recentlyCompletedDHQStudyIDs);
    if (errorCount > 0) {
        console.warn(`${errorCount} study queries failed, but continuing with successful results.`);
    }

    return recentlyCompletedDHQStudyIDs;
};

/**
 * HTTP handler to trigger report generation for all DHQ studies.
 * This process sends a confirmation email, but no listenable event is returned.
 * This runs on a nightly Cloud Scheduler job, before `processDHQReports` processes the incoming data.
 * @param {Request} req - HTTP request
 * @param {Response} res - HTTP response
 */
const generateDHQReports = async (req, res) => {
    console.log('Triggering DHQ report generation for all studies with recent completions.');
    if (req.method !== 'POST') {
        return res.status(405).json(getResponseJSON("Method not allowed. Use POST.", 405));
    }

    try {
        const { dhqStudyIDs, dhqToken, recentlyCompletedDHQStudyIDs, useDateFiltering, dateFilterStartDaysAgo, dateFilterEndDaysAgo } = await getDHQConfig();

        if (recentlyCompletedDHQStudyIDs.length === 0) {
            console.log('No studies have recent DHQ completions. Skipping report generation.');
            return res.status(200).json(getResponseJSON('No studies have recent DHQ completions. Report generation skipped.', 200));
        }

        console.log(`Processing ${recentlyCompletedDHQStudyIDs.length} studies with recent completions out of ${dhqStudyIDs.length} total studies.`);

        const results = {
            totalStudies: dhqStudyIDs.length,
            studiesWithRecentCompletions: recentlyCompletedDHQStudyIDs.length,
            successful: [],
            failed: [],
            summary: {
                totalSuccessful: 0,
                totalFailed: 0,
            },
        };

        const reportOptions = {
            include_incomplete_questionnaires: false,
            analysis: true,
            detailed_analysis: true,
            excel_answer: false,
            raw_answer: true,
        };

        // Add date filtering if enabled (Firestore -> appSettings -> dhq.useDateFiltering).
        // reportOptions includes the `start_date` and `end_date` params in mm/dd/yyyy format.
        // The start_date is (now - dateFilterStartDaysAgo). The end_date is (now - dateFilterEndDaysAgo).
        // The dateFilterStartDaysAgo and dateFilterEndDaysAgo are set in Firestore -> appSettings -> dhq.dateFilterStartDaysAgo and dhq.dateFilterEndDaysAgo.
        if (useDateFiltering) {
            const startDate = new Date(Date.now() - dateFilterStartDaysAgo * MILLISECONDS_PER_DAY).toLocaleDateString('en-US', { month: '2-digit', day: '2-digit', year: 'numeric' });
            const endDate = new Date(Date.now() - dateFilterEndDaysAgo * MILLISECONDS_PER_DAY).toLocaleDateString('en-US', { month: '2-digit', day: '2-digit', year: 'numeric' });

            reportOptions.start_date = startDate;
            reportOptions.end_date = endDate;
            console.log(`Date filtering enabled. Using start_date: ${startDate}, end_date: ${endDate} (date filter range: ${dateFilterStartDaysAgo} to ${dateFilterEndDaysAgo} days ago).`);

        } else {
            console.log('Date filtering disabled. Processing all available data.');
        }

        for (const studyID of recentlyCompletedDHQStudyIDs) {
            const studyIDToProcess = studyID.replace(/^study_/, '');

            try {
                console.log(`Triggering DHQ report generation for study ${studyIDToProcess}...`);

                const url = `${API_ROOT}${studyIDToProcess}/start-dhq-report/`;
                const method = 'POST';
                const headers = getDHQHeaders(method, dhqToken);
                const result = await fetchDHQAPIData(url, method, headers, reportOptions);

                console.log(`DHQ report generation triggered successfully for study ${studyIDToProcess}.`);
                results.successful.push({
                    studyID: studyIDToProcess,
                    message: 'Report generation triggered successfully',
                });
                results.summary.totalSuccessful++;
            
            } catch (error) {
                console.error(`Failed to trigger DHQ report generation for study ${studyIDToProcess}:`, error.message);
                results.failed.push({
                    studyID: studyIDToProcess,
                    error: error.message,
                });
                results.summary.totalFailed++;
            }
        }

        console.log(`DHQ Report Generation Summary: Total Studies: ${results.totalStudies}, Successful: ${results.summary.totalSuccessful}, Failed: ${results.summary.totalFailed}`);

        if (results.failed.length > 0) {
            console.log('Failed studies:', results.failed.map(f => f.studyID).join(', '));
        }

        const response = getResponseJSON(results.summary.totalSuccessful > 0 ? 'DHQ report generation completed successfully.' : 'DHQ report generation failed for all studies.', results.summary.totalSuccessful > 0 ? 200 : 500);
        response.results = results;

        return res.status(response.code).json(response);

    } catch (error) {
        console.error("Error triggering DHQ report generation:", error);
        return res.status(500).json(getResponseJSON("Failed to trigger DHQ report generation: " + error.message, 500));
    }
};

/**
 * HTTP handler for downloading DHQ reports
 * Note: Report generation needs to be triggered (generateDHQReports) before the updated reports are available.
 * That process sends a confirmation email, but no listenable event exists.
 * This call is delayed in Cloud Scheduler because of that waiting time.
 * @param {Request} req - HTTP request
 * @param {Response} res - HTTP response
 */

const processDHQReports = async (req, res) => {
    console.log('Processing DHQ reports for all studies.');
    logMemoryUsage('Function start');
    try {
        if (req.method !== 'POST') {
            return res.status(405).json(getResponseJSON("Method not allowed. Use POST.", 405));
        }

        const { dhqStudyIDs, dhqToken, recentlyCompletedDHQStudyIDs, useDateFiltering } = await getDHQConfig();
        
        if (recentlyCompletedDHQStudyIDs.length === 0) {
            console.log('No studies have recent DHQ completions. Skipping report processing.');
            return res.status(200).json(getResponseJSON("No studies have recent DHQ completions. Report processing skipped.", 200));
        }
        
        console.log(`Processing DHQ reports for ${recentlyCompletedDHQStudyIDs.length} studies with recent completions out of ${dhqStudyIDs.length} total studies.`);

        const fileTypeMap = {
            0: { name: 'Analysis Results', processor: processAnalysisResultsCSV },
            1: { name: 'Detailed Analysis', processor: processDetailedAnalysisCSV },
            4: { name: 'Raw Answers', processor: processRawAnswersCSV }
        };

        const overallSummary = {
            totalStudies: dhqStudyIDs.length,
            studiesWithRecentCompletions: recentlyCompletedDHQStudyIDs.length,
            studyResults: {},
            totalSuccessful: 0,
            totalFailed: 0
        };

        for (const studyID of recentlyCompletedDHQStudyIDs) {
            const studyIDToProcess = studyID.replace(/^study_/, '');
            console.log(`\n=== Processing Study: ${studyIDToProcess} ===`);
            
            const summary = {
                studyID: studyIDToProcess,
                results: {},
                totalSuccessful: 0,
                totalFailed: 0
            };

            for (const [fileTypeNum, fileConfig] of Object.entries(fileTypeMap)) {
                try {
                    console.log(`\n--- Processing ${fileConfig.name} (File Type ${fileTypeNum}) for study ${studyIDToProcess} ---`);
                    let reportData = await downloadDHQReport(studyIDToProcess, parseInt(fileTypeNum), dhqToken);

                    if (!reportData?.data || !reportData?.contentType?.includes('zip')) {
                        throw new Error('No ZIP data received from DHQ API');
                    }

                    let processingResult;

                    console.log('Extracting ZIP file contents...');
                    logMemoryUsage(`Before ZIP extraction - ${fileConfig.name}`);
                    let extractedFiles = await extractZipFiles(reportData.data);
                    logMemoryUsage(`After ZIP extraction - ${fileConfig.name}`);
                    
                    // Clear the original ZIP data to free memory
                    reportData.data = null;
                    reportData = null;
                    
                    for (const file of extractedFiles) {
                        if (file.filename.toLowerCase().endsWith('.csv')) {

                            logMemoryUsage(`Before CSV processing - ${fileConfig.name}`);
                            processingResult = await fileConfig.processor(file.content.toString('utf8'), studyIDToProcess);
                            logMemoryUsage(`After CSV processing - ${fileConfig.name}`, true);
                            
                            // Delete CSV content to free memory
                            delete file.content;
                        }
                    }
                    
                    // Clear extracted files array
                    extractedFiles = null;

                    if (processingResult?.success) {
                        console.log(`Successfully processed ${fileConfig.name}: ${processingResult.newDocuments} new documents`);
                        summary.results[fileTypeNum] = {
                            fileType: fileConfig.name,
                            ...processingResult
                        };
                        summary.totalSuccessful++;

                    } else {
                        throw new Error(processingResult?.error || `Failed to process ${fileConfig.name}`);
                    }
                    
                } catch (error) {
                    console.error(`Error processing file type ${fileTypeNum} (${fileConfig.name}) for study ${studyIDToProcess}:`, error);
                    summary.results[fileTypeNum] = {
                        fileType: fileConfig.name,
                        success: false,
                        error: error.message
                    };
                    summary.totalFailed++;
                }
            }

            overallSummary.studyResults[studyIDToProcess] = summary;
            overallSummary.totalSuccessful += summary.totalSuccessful;
            overallSummary.totalFailed += summary.totalFailed;

            console.log(`\n--- Study ${studyIDToProcess} Processing Summary ---`);
            console.log(`File Types Processed: ${Object.keys(fileTypeMap).length}. Total: ${summary.totalSuccessful + summary.totalFailed}. Successful: ${summary.totalSuccessful}. Failed: ${summary.totalFailed}.`);
        }

        console.log('\n=== Overall DHQ Report Processing Summary ===');
        console.log(`Total Studies Processed: ${overallSummary.totalStudies}. Successful: ${overallSummary.totalSuccessful}. Failed: ${overallSummary.totalFailed}.`);

        const response = getResponseJSON('DHQ reports processing completed for all studies.', 200);
        response.summary = overallSummary;
        
        console.log('DHQ Report Processing Response:', JSON.stringify(response, null, 2));

        return res.status(200).json(response);

    } catch (error) {
        console.error("Error in processDHQReports:", error);
        return res.status(500).json(getResponseJSON("Failed to download DHQ report: " + error.message, 500));
    }
};

/**
 * Download generated report files for the given study
 * @param {string} studyID - The ID of the study to download reports from
 * @param {number} downloadFileType - File type to download (0-6)
 *   0: Analysis Results File (zip with csv)                -- Used (processAnalysisResultsCSV)
 *   1: Detailed Analysis File (zip with csv)               -- Used (processDetailedAnalysisCSV)
 *   2: Detailed Analysis Data Dictionary (text file .dic)  -- Not used
 *   3: Excel Answer File (zip with csv)                    -- Not used
 *   4: Raw Answer File (zip with csv)                      -- Used (processRawAnswersCSV)
 *   5: Survey Response Codes Data Dictionary (csv)         -- Not used
 *   6: Survey Question Data Dictionary (csv)               -- Not used
 * @returns {Promise<Object>} - The downloaded file data
 */

const downloadDHQReport = async (studyID, downloadFileType, dhqToken) => {
    console.log('Downloading DHQ report for study:', studyID, 'with file type:', downloadFileType);
    try {
        studyID = studyID.replace(/^study_/, '');
        
        const url = `${API_ROOT}${studyID}/download-dhq-report/`;
        const method = "POST";
        const headers = getDHQHeaders(method, dhqToken);
        const data = { download_file_type: downloadFileType };

        return await fetchDHQAPIData(url, method, headers, data);

    } catch (error) {
        console.error("Error downloading DHQ report:", error);
        throw new Error(`Failed to download DHQ report: ${error.message}`);
    }
};

/**
 * Create a document ID from a respondent ID (ensure no duplicate processing).
 * @param {string} respondentId - The original respondent ID
 * @returns {string} - Document ID for Firestore
 */
const createResponseDocID = (respondentId) => {
    if (!respondentId) return null;
    
    // Firestore document IDs cannot contain: / \\ . # $ [ ]
    return respondentId.toString().replace(/[\/\\\.#\$\[\]]/g, '_');
};

/**
 * Get dynamic chunk size based on current memory usage.
 * Reduce chunk size as memory usage increases.
 * @returns {number} Appropriate chunk size based on current memory conditions.
 * Note: GCP allocation is 2048MB. Location: config -> (dev/stage/prod) -> processDHQReports.yaml -> _MEMORY.
 */
const getDynamicChunkSize = () => {
    const currentMemory = process.memoryUsage();
    const baseChunkSize = PROCESSING_CHUNK_SIZE;
    const heapUsedMB = Math.round(currentMemory.heapUsed / 1024 / 1024);
    
    if (heapUsedMB > 1500) {
        console.warn('Memory usage is high (> 1500MB). Reducing chunk size to 100.');
        return 100;
    }
    else if (heapUsedMB > 1250) {
        console.warn('Memory usage is high (> 1250MB). Reducing chunk size to 250.');
        return 250;
    }
    else if (heapUsedMB > 1000) {
        console.warn('Memory usage is high (> 1000MB). Reducing chunk size to 500.');
        return 500;
    }
    
    return baseChunkSize;
};

/**
 * Batch-write data to Firestore.
 * @param {string} collectionName - The Firestore collection name
 * @param {Array<Object>} documents - Array of documents to write, each with { id, data }
 * @returns {Promise<Object>} - Result object with success/failure counts
 */
const batchWriteToFirestore = async (collectionName, documents, dynamicBatchSize = 500) => {
    const batchSize = Math.min(dynamicBatchSize, 500);
    const merge = true;
    const totalDocuments = documents.length;
    let successCount = 0;
    let errorCount = 0;
    const errors = [];

    console.log(`Starting batch write to collection '${collectionName}' with ${totalDocuments} documents (batch size: ${batchSize})`);

    try {
        for (let i = 0; i < totalDocuments; i += batchSize) {
            const batch = db.batch();
            const currentBatch = documents.slice(i, i + batchSize);
            const batchNumber = Math.floor(i / batchSize) + 1;
            const totalBatches = Math.ceil(totalDocuments / batchSize);
            let batchErrorCount = 0;

            console.log(`Processing batch ${batchNumber}/${totalBatches} (${currentBatch.length} documents)`);

            for (const document of currentBatch) {
                try {
                    if (!document.id || !document.data) {
                        console.warn('Skipping invalid document - missing id or data:', document);
                        batchErrorCount++;
                        continue;
                    }

                    const docRef = db.collection(collectionName).doc(document.id);
                    
                    merge
                        ? batch.set(docRef, document.data, { merge: true })
                        : batch.set(docRef, document.data);
                    
                } catch (error) {
                    console.error(`Error preparing document ${document.id} for batch:`, error);
                    errors.push({ documentId: document.id, error: error.message });
                    batchErrorCount++;
                }
            }

            try {
                await batch.commit();
                const batchSuccessCount = currentBatch.length - batchErrorCount;
                successCount += batchSuccessCount;
                console.log(`Batch ${batchNumber} committed successfully`);

            } catch (error) {
                console.error(`Error committing batch ${batchNumber}:`, error);
                errorCount += currentBatch.length;
                errors.push({ batch: batchNumber, error: error.message });
            }
        }

        const result = {
            totalDocuments,
            successCount,
            errorCount,
            errors: errors.length > 0 ? errors : undefined
        };

        console.log(`Batch write completed. Success: ${successCount}, Errors: ${errorCount}`);
        return result;

    } catch (error) {
        console.error('Error in batchWriteToFirestore:', error);
        throw new Error(`Batch write failed: ${error.message}`);
    }
};

/**
 * Get processed respondent IDs from the tracking collection to avoid duplicate processing.
 * Location: Firestore -> dhq3SurveyCredentials -> studyID -> responseTracking
 * @param {string} studyID - The study ID to check
 * @param {string} collectionName - The collection name (dhqAnalysisResults, dhqDetailedAnalysis, dhqRawAnswers)
 * @returns {Promise<Set<string>>} - Set of processed respondent IDs
 */
const getProcessedRespondentIds = async (studyID, collectionName) => {
    try {
        const trackingCollection = `dhq3SurveyCredentials/${studyID}/responseTracking`;
        const docRef = db.collection(trackingCollection).doc(collectionName);
        const doc = await docRef.get();
        
        if (doc.exists) {
            const data = doc.data();
            return new Set(data[fieldMapping.dhq3ProcessedRespondentArray.toString()] || []);
        }
        
        return new Set();

    } catch (error) {
        console.error('Error getting processed respondent IDs:', error);
        return new Set();
    }
};

/**
 * Update the processing tracking with newly processed respondent IDs.
 * Location: Firestore -> dhq3SurveyCredentials -> studyID -> responseTracking
 * @param {string} studyID - The study ID
 * @param {string} collectionName - The collection name (dhqAnalysisResults, dhqDetailedAnalysis, dhqRawAnswers)
 * @param {Array<string>} newRespondentIds - Array of newly processed respondent IDs
 * @returns {Promise<void>}
 */
const updateProcessingTracking = async (studyID, collectionName, newRespondentIds) => {
    try {
        if (!newRespondentIds || newRespondentIds.length === 0) {
            return;
        }

        const trackingCollection = `dhq3SurveyCredentials/${studyID}/responseTracking`;
        const docRef = db.collection(trackingCollection).doc(collectionName);
        
        await db.runTransaction(async (transaction) => {
            // Use transaction to ensure atomic read-modify-write operation
            transaction.set(docRef, {
                [fieldMapping.dhq3StudyID]: studyID,
                [fieldMapping.docLastUpdatedTimestamp]: new Date().toISOString(),
                [fieldMapping.dhq3ProcessedRespondentArray]: FieldValue.arrayUnion(...newRespondentIds),
            }, { merge: true });
        });

        console.log(`Updated processing tracking for ${studyID} ${collectionName} with ${newRespondentIds.length} new respondent IDs`);

    } catch (error) {
        console.error('Error updating processing tracking:', error);
        throw error;
    }
};

/**
 * Sanitize keys names for BigQuery compatibility
 * Handles special characters in DHQ keys, such as "()-* ".
 * BigQuery allows: letters, numbers, and underscores
 * Ensures valid column naming and prevents collisions
 */
const sanitizeFieldName = (fieldName) => {
    const originalFieldName = fieldName;
    
    if (!fieldName || typeof fieldName !== 'string') {
        throw new Error(`Invalid field name: ${fieldName} (must be a non-empty string)`);
    }
    
    // Handle leading asterisk
    if (fieldName.startsWith('*')) {
        fieldName = 'star_' + fieldName.substring(1);
    }
    
    fieldName = fieldName
        .replace(/[^a-zA-Z0-9_]/g, '_') // Replace all invalid characters with underscores
        .replace(/_+/g, '_')            // Collapse multiple consecutive underscores into single underscore
        .replace(/^_+|_+$/g, '');       // Remove leading and trailing underscores
    
    // Sanity check for empty string
    if (!fieldName) {
        throw new Error(`Field name "${originalFieldName}" became empty after sanitization`);
    }
    
    // Ensure field name starts with letter or underscore (BQ requirement)
    if (!/^[a-zA-Z_]/.test(fieldName)) {
        fieldName = 'field_' + fieldName;
    }
    
    return fieldName;
}

/**
 * Process Analysis Results CSV and write to dhqAnalysisResults collection.
 * This CSV file has a header and one row per respondent.
 * Firestore: one document per respondent. Batching speeds up processing.
 * @param {string} csvContent - The CSV content as a string.
 * @param {string} studyID - The study ID for tracking purposes.
 * @returns {Promise<Object>} - Processing result with success/failure counts.
 */
const processAnalysisResultsCSV = async (csvContent, studyID) => {
    const collectionName = 'dhqAnalysisResults';

    if (!studyID.startsWith('study_')) studyID = `study_${studyID}`;

    const processedIds = await getProcessedRespondentIds(studyID, collectionName);
    console.log('Streaming Analysis Results CSV for study', studyID, 'Already processed IDs:', processedIds.size);
    logMemoryUsage(`Starting ${collectionName} processing`);

    const dynamicBatchSize = Math.min(getDynamicChunkSize(), 500);
    let headerRow = null;
    let respondentIdIdx = -1;
    let totalRows = 0;
    let skippedRespondents = 0;
    let newDocuments = 0;

    let batch = db.batch();
    let batchCount = 0;
    let batchRespondentIds = [];
    const successfulRespondentIds = [];

    for await (const row of streamCSVRows(csvContent)) {
        // First non-comment line is the header
        if (!headerRow) {
            headerRow = row.map(h => (h || '').toString().trim());
            respondentIdIdx = headerRow.indexOf('Respondent ID');
            if (respondentIdIdx === -1) {
                throw new Error('Respondent ID column not found in Analysis Results CSV');
            }
            continue;
        }

        totalRows++;
        const respondentId = row[respondentIdIdx];

        if (!respondentId || processedIds.has(respondentId)) {
            skippedRespondents++;
            continue;
        }

        const documentData = {};
        for (let i = 0; i < headerRow.length; i++) {
            if (i === respondentIdIdx) continue;
            const key = sanitizeFieldName(headerRow[i]);
            documentData[key] = row[i];
        }
        documentData[fieldMapping.dhq3Username] = respondentId;
        documentData[fieldMapping.dhq3StudyID] = studyID;
        documentData[fieldMapping.dhq3ResponseProcessedTime] = new Date().toISOString();

        const docId = createResponseDocID(respondentId);
        if (!docId) {
            skippedRespondents++;
            continue;
        }

        const docRef = db.collection(collectionName).doc(docId);
        batch.set(docRef, documentData, { merge: true });
        batchCount++;
        batchRespondentIds.push(respondentId);

        if (batchCount >= dynamicBatchSize) {
            try {
                await batch.commit();
                newDocuments += batchCount;
                successfulRespondentIds.push(...batchRespondentIds);
                logMemoryUsage(`Batch committed - ${newDocuments} documents processed`);
            } catch (err) {
                console.error('Error committing Analysis Results batch:', err);
            }
            batch = db.batch();
            batchCount = 0;
            batchRespondentIds = [];
        }
    }

    // Handle the last batch
    if (batchCount > 0) {
        try {
            await batch.commit();
            newDocuments += batchCount;
            successfulRespondentIds.push(...batchRespondentIds);
            logMemoryUsage(`Batch committed - ${newDocuments} documents processed`);
        } catch (e) {
            console.error('Error committing final Analysis Results batch:', e);
        }
    }

    // Update tracking collection
    if (successfulRespondentIds.length > 0) {
        try {
            await updateProcessingTracking(studyID, collectionName, successfulRespondentIds);
        } catch (error) {
            console.error('Error updating processing tracking:', error);
        }
    }

    logMemoryUsage(`Completed ${collectionName} processing`, true);

    return {
        success: true,
        collectionName,
        totalRows,
        newDocuments,
        skippedRespondents,
    };
};

/**
 * Process Detailed Analysis CSV and write to dhqDetailedAnalysis collection.
 * This CSV file has a header and one row per question per respondent (variable number of rows per respondent depending on the respondent's answers)
 * @param {string} csvContent - The CSV content as a string.
 * @param {string} studyID - The study ID for tracking purposes.
 * @returns {Promise<Object>} - Processing result with success/failure counts.
 */
const processDetailedAnalysisCSV = async (csvContent, studyID) => {
    const collectionName = 'dhqDetailedAnalysis';
    const trackingBatchSize = 25; // Number of respondents for responseTracking update

    if (!studyID.startsWith('study_')) studyID = `study_${studyID}`;

    const processedIds = await getProcessedRespondentIds(studyID, collectionName);
    console.log('Streaming Detailed Analysis CSV for study', studyID, 'Already processed IDs:', processedIds.size);
    logMemoryUsage(`Starting ${collectionName} processing`);

    let headerRow = null;
    let respondentIdIdx = -1;
    let questionIdIdx = -1;
    let foodIdIdx = -1;
    let totalRows = 0;
    let skippedRows = 0;
    let skippedRespondents = new Set();
    let newDocuments = 0;

    let currentRespondent = null;
    let currentRespondentBatch = db.batch();
    let currentRespondentDocCount = 0;
    let successfulRespondentIds = new Set();
    
    const dynamicBatchSize = Math.min(getDynamicChunkSize(), 500);

    // Flush tracking buffer when the threshold is reached or on final flush
    async function flushTrackingBuffer(force = false) {
        const shouldFlush = force ? successfulRespondentIds.size > 0 : successfulRespondentIds.size >= trackingBatchSize;
        if (!shouldFlush) return;
        try {
            await updateProcessingTracking(studyID, collectionName, Array.from(successfulRespondentIds));
            successfulRespondentIds.clear();
        } catch (trackingError) {
            const context = force ? `final process tracking update: (${successfulRespondentIds.size}) respondents` : `buffered respondents (${successfulRespondentIds.size})`;
            console.error(`Error flushing ${context}:`, trackingError);
        }
    }

    // Write current respondent batch with optional completion tracking
    async function writeCurrentRespondent(isCompleteRespondent = false) {
        if (!currentRespondent || currentRespondentDocCount === 0) return;
        
        try {
            await currentRespondentBatch.commit();
            newDocuments += currentRespondentDocCount;
            
            // Only track respondent as complete when finishing all their documents
            // Batch responseTracking updates since this CSV can be massive
            if (isCompleteRespondent) {
                successfulRespondentIds.add(currentRespondent);
                await flushTrackingBuffer(false);
            }
            
        } catch (error) {
            const batchType = isCompleteRespondent ? 'respondent' : 'partial batch';
            console.error(`Error committing ${batchType} for ${currentRespondent}:`, error);
        }
        
        // Reset batch for next set of documents
        currentRespondentBatch = db.batch();
        currentRespondentDocCount = 0;
    }

    for await (const row of streamCSVRows(csvContent)) {
        if (!headerRow) {
            headerRow = row.map(h => (h || '').toString().trim());
            respondentIdIdx = headerRow.indexOf('Respondent ID');
            questionIdIdx = headerRow.indexOf('Question ID');
            foodIdIdx = headerRow.indexOf('Food ID');
            if (respondentIdIdx === -1 || questionIdIdx === -1) {
                throw new Error('Required columns missing in Detailed Analysis CSV');
            }
            continue;
        }

        totalRows++;
        const respondentId = row[respondentIdIdx];
        const questionIdRaw = row[questionIdIdx];
        const foodIdRaw = foodIdIdx !== -1 ? row[foodIdIdx] : undefined;

        if (!respondentId || skippedRespondents.has(respondentId) || processedIds.has(respondentId) || !questionIdRaw) {
            skippedRows++;
            skippedRespondents.add(respondentId);
            continue;
        }

        // Write the previous respondent once we encounter a new one. That respondent's processing is completed.
        if (currentRespondent !== respondentId) {
            await writeCurrentRespondent(true);
            currentRespondent = respondentId;
        }

        const formattedQuestionId = `Q${questionIdRaw.toString().padStart(3, '0')}`;
        const foodIdSanitized = foodIdRaw ? foodIdRaw.replace(/[. ]/g, '_') : 'NONE';
        const docId = `${respondentId}_${formattedQuestionId}_${foodIdSanitized}`;

        const rowData = {};
        for (let i = 0; i < headerRow.length; i++) {
            if (i === respondentIdIdx) continue;
            const key = sanitizeFieldName(headerRow[i]);
            rowData[key] = row[i];
        }

        const documentData = {
            [fieldMapping.dhq3Username]: respondentId,
            [fieldMapping.dhq3StudyID]: studyID,
            [fieldMapping.dhq3ResponseProcessedTime]: new Date().toISOString(),
            ...rowData,
        };

        // Add to current respondent's batch and check batch size
        const docRef = db.collection(collectionName).doc(docId);
        currentRespondentBatch.set(docRef, documentData, { merge: true });
        currentRespondentDocCount++;

        // Commit partial batch if needed
        if (currentRespondentDocCount >= dynamicBatchSize) {
            await writeCurrentRespondent(false);
        }
    }

    // Write the final respondent and update responseTracking
    await writeCurrentRespondent(true);

    // Flush the remaining tracking updates
    await flushTrackingBuffer(true);

    logMemoryUsage(`Completed ${collectionName} processing`, true);

    return {
        success: true,
        collectionName,
        totalRows,
        newDocuments,
        skippedRows,
        skippedRespondents: skippedRespondents.size,
    };
};

/**
 * Process Raw Answers CSV and write to dhqRawAnswers collection.
 * This CSV file has a header and one row per question per respondent (e.g. 700+ rows per respondent).
 * Firestore: one document per respondent. Write once accumulated per respondent.No batching.
 * @param {string} csvContent - The CSV content as a string.
 * @param {string} studyID - The study ID for tracking purposes.
 * @returns {Promise<Object>} - Processing result with success/failure counts.
 */
const processRawAnswersCSV = async (csvContent, studyID) => {
    const collectionName = 'dhqRawAnswers';

    if (!studyID.startsWith('study_')) studyID = `study_${studyID}`;

    const processedIds = await getProcessedRespondentIds(studyID, collectionName);
    console.log('Streaming Raw Answers CSV for study', studyID, 'Already processed IDs:', processedIds.size);
    logMemoryUsage(`Starting ${collectionName} processing`);

    let headerRow = null;
    let respondentIdx = -1;
    let questionIdx = -1;
    let answerIdx = -1;
    let totalRows = 0;
    let skippedRows = 0;
    let newDocuments = 0;

    // Process one respondent at a time
    let currentRespondent = null;
    let currentRespondentData = null;
    const successfulRespondentIds = new Set();
    const skippedRespondents = new Set();

    async function writeCurrentRespondent() {
        if (!currentRespondent || !currentRespondentData) return;
        
        try {
            const docId = createResponseDocID(currentRespondent);
            if (!docId) {
                console.warn(`Could not create document ID for respondent: ${currentRespondent}`);
                return;
            }
            
            const docRef = db.collection(collectionName).doc(docId);
            await docRef.set(currentRespondentData, { merge: true });
            newDocuments++;
            successfulRespondentIds.add(currentRespondent);
            
        } catch (error) {
            console.error(`Error writing respondent ${currentRespondent}:`, error);
        }
    }

    for await (const row of streamCSVRows(csvContent)) {
        if (!headerRow) {
            headerRow = row.map(h => (h || '').toString().trim());
            respondentIdx = headerRow.indexOf('Respondent Login ID');
            questionIdx = headerRow.indexOf('Question ID');
            answerIdx = headerRow.indexOf('Answer');
            if (respondentIdx === -1 || questionIdx === -1 || answerIdx === -1) {
                throw new Error('Required columns missing in Raw Answers CSV');
            }
            continue;
        }

        totalRows++;
        const respondentId = row[respondentIdx];
        const questionIdRaw = row[questionIdx];
        const answer = row[answerIdx];

        if (!respondentId || processedIds.has(respondentId) || !questionIdRaw) {
            skippedRows++;
            skippedRespondents.add(respondentId);
            continue;
        }

        if (answer === '.') { // Skip missing answers
            continue;
        }

        // Write the previous respondent once we encounter a new one
        if (currentRespondent !== respondentId) {
            await writeCurrentRespondent();
            
            // Start new respondent
            currentRespondent = respondentId;
            currentRespondentData = {
                [fieldMapping.dhq3Username]: respondentId,
                [fieldMapping.dhq3StudyID]: studyID,
                [fieldMapping.dhq3ResponseProcessedTime]: new Date().toISOString(),
            };
        }

        // Add question to current respondent
        const formattedQuestionId = questionIdRaw.replace(/^Q(\d+)$/, (match, num) => `Q${num.padStart(3, '0')}`);
        const sanitizedQuestionId = sanitizeFieldName(formattedQuestionId);
        currentRespondentData[sanitizedQuestionId] = answer;
    }

    // Write the final respondent
    await writeCurrentRespondent();

    if (successfulRespondentIds.size > 0) {
        try {
            await updateProcessingTracking(studyID, collectionName, Array.from(successfulRespondentIds));
        } catch (error) {
            console.error('Error updating processing tracking:', error);
        }
    }

    logMemoryUsage(`Completed ${collectionName} processing`, true);

    return {
        success: true,
        collectionName,
        totalRows,
        newDocuments,
        skippedRows,
        skippedRespondents: skippedRespondents.size,
    };
};

/**
 * Log current memory usage with context for monitoring
 * @param {string} context - Description of current operation
 * @param {boolean} forceGC - Whether to force garbage collection (if --expose-gc enabled)
 */
const logMemoryUsage = (context, forceGC = false) => {
    if (forceGC && global.gc) {
        global.gc();
    }
    
    const memUsage = process.memoryUsage();
    const heapUsedMB = Math.round(memUsage.heapUsed / 1024 / 1024);
    const heapTotalMB = Math.round(memUsage.heapTotal / 1024 / 1024);
    const externalMB = Math.round(memUsage.external / 1024 / 1024);
    const rssMB = Math.round(memUsage.rss / 1024 / 1024);

    // Log memory usage in dev only. Warn in all envs if memory usage is high.
    if (developmentTier === 'DEV') {
        console.log(`MEMORY: ${context}: Heap Used: ${heapUsedMB}MB, Heap Total: ${heapTotalMB}MB, External: ${externalMB}MB, RSS: ${rssMB}MB`);
    }

    if (heapUsedMB > MEMORY_WARNING_THRESHOLD) {
        console.warn(`MEMORY WARNING: High memory usage detected (${heapUsedMB}MB) during: ${context}`);
    }
    
    return { heapUsedMB, heapTotalMB, externalMB, rssMB };
};

module.exports = {
    scheduledSyncDHQ3Status,
    syncDHQ3RespondentInfo,
    allocateDHQ3Credential,
    retrieveDHQHEIReport,
    updateDHQReportViewedStatus,
    scheduledCountDHQ3Credentials,
    generateDHQReports,
    processDHQReports,
    processAnalysisResultsCSV,
    processDetailedAnalysisCSV,
    processRawAnswersCSV,
    getDHQConfig,
    getRecentlyCompletedDHQStudies,
    getProcessedRespondentIds,
    updateProcessingTracking,
    batchWriteToFirestore,
    createResponseDocID,
    getDynamicChunkSize,
    sanitizeFieldName,
    logMemoryUsage
};
