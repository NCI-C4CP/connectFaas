const admin = require('firebase-admin');
const { FieldValue } = require('firebase-admin/firestore');
const { getResponseJSON, setHeadersDomainRestricted, setHeaders, logIPAddress, getSecret } = require("./shared");
const { retrieveUserProfile } = require('./firestore');
const { normalizeIso8601Timestamp } = require('./validation');
const fieldMapping = require('./fieldToConceptIdMapping');
const db = admin.firestore();

const API_ROOT = 'https://www.dhq3.org/api-home/root/study-list/';

const getDHQHeaders = (method, dhqToken) => {
    return method === "GET"
        ? { "Authorization": "Token " + dhqToken }
        : { "Authorization": "Token " + dhqToken, "Content-Type": "application/json" };
}

const fetchDHQAPIData = async (url, method, headers, data) => {    
    const options = { method, headers };
    if (data !== null && data !== undefined) {
        options.body = JSON.stringify(data);
    }

    try {
        const response = await fetch(url, options);
        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`Error ${response.status}: ${errorText}`);
        }

        const contentType = response.headers.get("Content-Type");

        switch (contentType) {
            case "application/json":
                return await response.json();
            case "application/zip":
                const blob = await response.blob();
                return await blob.arrayBuffer();
            default:
                return await response.text();
        }

    } catch (error) {
        console.error("API call failed:", error);
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

        // If we have updates to process, we'll need the DHQ API token from secret manager.
        const dhqToken = await getSecret(process.env.DHQ_TOKEN);
        if (!dhqToken) {
            console.error('DHQ API token not found in secret manager.');
            return res.status(500).json(getResponseJSON("DHQ API token not found in secret manager.", 500));
        }

        const updatePromises = [];

        for (const participantDoc of participantsSnapshot.docs) {
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
                errorCount++; // Count as an error/skip
                continue;
            }
            if (!dhq3StudyID) {
                console.error(`Participant ${uid} has no DHQ studyID (${fieldMapping.dhq3StudyID}). Skipping.`);
                errorCount++;
                continue;
            }

            updatePromises.push(
                syncDHQ3RespondentInfo(dhq3StudyID, dhq3Username, dhq3SurveyStatus, dhq3SurveyStatusExternal, uid, dhqToken)
                    .then(result => ({ status: 'fulfilled', value: { uid, result } }))
                    .catch(error => ({ status: 'rejected', reason: { uid, error } }))
            );
        }

        const results = await Promise.allSettled(updatePromises);
        results.forEach(result => {
            if (result.status === 'fulfilled') {
                successCount++;
            } else {
                console.error(`Error updating participant ${result.reason.uid}:`, result.reason.error);
                errorCount++;
            }
        });

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
 *     viewed_rnr_report: bool, 
 *     downloaded_rnr_report: bool 
 *     viewed_hei_report: bool // TODO: support this with reports in phase 2 (post-MVP).
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
        if (results?.questionnaire_status === 3 && (dhq3SurveyStatus !== fieldMapping.submitted || dhq3SurveyStatusExternal !== fieldMapping.submitted)) {
            await updateDHQ3ProgressStatus(true, fieldMapping.submitted, results?.status_date || '', uid);

        // If the survey is only started in DHQ, sanity-check the status with the participant profile. This should always be set on the survey's 'start' click in the PWA.
        } else if (results?.questionnaire_status === 2 && (dhq3SurveyStatus !== fieldMapping.started || dhq3SurveyStatusExternal !== fieldMapping.started)) {
            await updateDHQ3ProgressStatus(false, fieldMapping.started, results?.status_date || '', uid);
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
 */

const updateDHQ3ProgressStatus = async (isSubmitted, completionStatus, dhqSubmittedTimestamp, uid) => {
    try {
        let updateData = {
            [fieldMapping.dhq3SurveyStatus]: completionStatus,
            [fieldMapping.dhq3SurveyStatusExternal]: completionStatus,
        };

        if (isSubmitted) {
            if (dhqSubmittedTimestamp) {
                const parsedTimestamp = normalizeIso8601Timestamp(dhqSubmittedTimestamp);
                updateData[fieldMapping.dhq3SurveyCompletionTime] = parsedTimestamp;

            } else {
                updateData[fieldMapping.dhq3SurveyCompletionTime] = new Date().toISOString();
            }
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
                [fieldMapping.dhq3HEIReportViewed]: participant?.[fieldMapping.dhq3HEIReportViewed],
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

                        allocatedCredentialData = {
                            [fieldMapping.dhq3StudyID]: dhqStudy,
                            [fieldMapping.dhq3Username]: credentialData.username,
                            [fieldMapping.dhq3UUID]: credentialDoc.id,
                            [fieldMapping.dhq3SurveyStatus]: fieldMapping.notStarted,
                            [fieldMapping.dhq3SurveyStatusExternal]: fieldMapping.notStarted,
                            [fieldMapping.dhq3HEIReportViewed]: fieldMapping.no,
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

module.exports = {
    scheduledSyncDHQ3Status,
    syncDHQ3RespondentInfo,
    allocateDHQ3Credential,
}
