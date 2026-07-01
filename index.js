const {onRequest} = require("firebase-functions/v2/https");
const { onTaskDispatched } = require("firebase-functions/v2/tasks");
const { getToken } = require('./utils/validation');
const { getFilteredParticipants, getParticipants, identifyParticipant } = require('./utils/submission');
const { submitParticipantsData, updateParticipantData, getBigQueryData, geocodedAddresses } = require('./utils/sites');
const { sendScheduledNotifications, processNotificationBatchBulkDefault, processNotificationBatchBulkMicrosoft } = require('./utils/notifications');
const { connectApp } = require('./utils/connectApp');
const { biospecimenAPIs } = require('./utils/biospecimen');
const { incentiveCompleted, eligibleForIncentive } = require('./utils/incentive');
const { dashboard } = require('./utils/dashboard');
const { importToBigQuery, firestoreExport, exportNotificationsToBucket } = require('./utils/events');
const { participantDataCleanup } = require('./utils/participantDataCleanup');
const { auditDataDestruction } = require('./utils/dataDestructionAudit');
const { webhook } = require('./utils/webhook');
const { heartbeat } = require('./utils/heartbeat');
const { physicalActivity } = require('./utils/reports');
const { generateDHQReports, processDHQReports, scheduledCountDHQ3Credentials, scheduledSyncDHQ3Status } = require('./utils/dhq');
const { triggerPromisProcessing } = require('./utils/promisHelper');

// API End-Points for Sites
exports.incentiveCompleted = incentiveCompleted;
exports.participantsEligibleForIncentive = eligibleForIncentive;
exports.getParticipantToken = getToken;
exports.getFilteredParticipants = getFilteredParticipants;
exports.getParticipants = getParticipants;
exports.identifyParticipant = identifyParticipant;
exports.submitParticipantsData = submitParticipantsData;
exports.updateParticipantData = updateParticipantData;
exports.getBigQueryData = getBigQueryData;
exports.geocodedAddresses = geocodedAddresses;

// End-Point for Site Manager Dashboard
exports.dashboard = dashboard;

// End-Point for Connect PWA
exports.app = connectApp;

// End-Point for Biospecimen Dashboard
exports.biospecimen = biospecimenAPIs;

// End-Point for Scheduled Notifications Handler
exports.sendScheduledNotifications = onRequest(sendScheduledNotifications);

// Cloud Task handlers for bulk notification batch processing.
// Retry policy and rate limits are managed in the Cloud Tasks queue.
exports.processNotificationBatchBulkDefault = onTaskDispatched(processNotificationBatchBulkDefault);
exports.processNotificationBatchBulkMicrosoft = onTaskDispatched(processNotificationBatchBulkMicrosoft);

// End-Points for Exporting Firestore to Big Query
exports.importToBigQuery = onRequest(importToBigQuery); 
exports.scheduleFirestoreDataExport = onRequest(firestoreExport);
exports.exportNotificationsToBucket = onRequest(exportNotificationsToBucket);

// End-Points for Participant Data Cleaning
exports.participantDataCleanup = onRequest(participantDataCleanup);
exports.auditDataDestruction = onRequest(auditDataDestruction);

// End-Points for Event Webhook
exports.webhook = webhook;

// End-Points for Public Heartbeat
exports.heartbeat = heartbeat;

// End-Points for Return of Information
exports.physicalActivity = physicalActivity;

// End-Points for Nightly DHQ processes.
exports.generateDHQReports = onRequest(generateDHQReports);
exports.processDHQReports = onRequest(processDHQReports);
exports.scheduledSyncDHQ3Status = onRequest(scheduledSyncDHQ3Status);
exports.scheduledCountDHQ3Credentials = onRequest(scheduledCountDHQ3Credentials);

// Temporary End-Point for PROMIS Processing
exports.promis = triggerPromisProcessing;
