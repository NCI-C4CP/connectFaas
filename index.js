const {onRequest} = require("firebase-functions/v2/https");
const { getToken } = require('./utils/validation');
const { getFilteredParticipants, getParticipants, identifyParticipant } = require('./utils/submission');
const { submitParticipantsData, updateParticipantData, getBigQueryData } = require('./utils/sites');
const { sendScheduledNotifications } = require('./utils/notifications');
const { connectApp } = require('./utils/connectApp');
const { biospecimenAPIs } = require('./utils/biospecimen');
const { incentiveCompleted, eligibleForIncentive } = require('./utils/incentive');
const { dashboard } = require('./utils/dashboard');
const { importToBigQuery, firestoreExport, exportNotificationsToBucket, importNotificationsToBigquery } = require('./utils/events');
const { participantDataCleanup } = require('./utils/participantDataCleanup');
const { webhook } = require('./utils/webhook');
const { heartbeat } = require('./utils/heartbeat');
const { physicalActivity } = require('./utils/reports');
const { generateDHQReports, processDHQReports, scheduledCountDHQ3Credentials, scheduledSyncDHQ3Status } = require('./utils/dhq');

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

// End-Point for Site Manager Dashboard
exports.dashboard = dashboard;

// End-Point for Connect PWA
exports.app = connectApp;

// End-Point for Biospecimen Dashboard
exports.biospecimen = biospecimenAPIs;

// End-Point for Scheduled Notifications Handler
exports.sendScheduledNotifications = onRequest(sendScheduledNotifications);

// End-Points for Exporting Firestore to Big Query
exports.importToBigQuery = onRequest(importToBigQuery); 
exports.scheduleFirestoreDataExport = onRequest(firestoreExport);
exports.exportNotificationsToBucket = onRequest(exportNotificationsToBucket);
exports.importNotificationsToBigquery = onRequest(importNotificationsToBigquery);

// End-Points for Participant Data Cleaning
exports.participantDataCleanup = onRequest(participantDataCleanup);

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
