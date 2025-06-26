const {onRequest} = require("firebase-functions/v2/https");
const { getToken, validateUsersEmailPhone } = require('./utils/validation');
const { getFilteredParticipants, getParticipants, identifyParticipant } = require('./utils/submission');
const { submitParticipantsData, updateParticipantData, getBigQueryData } = require('./utils/sites');
const { getParticipantNotification, sendScheduledNotifications } = require('./utils/notifications');
const { connectApp } = require('./utils/connectApp');
const { biospecimenAPIs } = require('./utils/biospecimen');
const { incentiveCompleted, eligibleForIncentive } = require('./utils/incentive');
const { dashboard } = require('./utils/dashboard');
const { importToBigQuery, firestoreExport, exportNotificationsToBucket, importNotificationsToBigquery } = require('./utils/events');
const { participantDataCleanup } = require('./utils/participantDataCleanup');
const { webhook } = require('./utils/webhook');
const { heartbeat } = require('./utils/heartbeat');
const { physicalActivity } = require('./utils/reports');
const { scheduledCountDHQ3Credentials, scheduledSyncDHQ3Status } = require('./utils/dhq');

// API End-Points for Sites

exports.incentiveCompleted = incentiveCompleted;

exports.participantsEligibleForIncentive = eligibleForIncentive;

exports.getParticipantToken = getToken;

exports.validateUsersEmailPhone = validateUsersEmailPhone;

exports.getFilteredParticipants = getFilteredParticipants;

exports.getParticipants = getParticipants;

exports.identifyParticipant = identifyParticipant;

exports.submitParticipantsData = submitParticipantsData;

exports.updateParticipantData = updateParticipantData;

exports.getBigQueryData = getBigQueryData;

exports.getParticipantNotification = getParticipantNotification;


// End-Point for Site Manager Dashboard

exports.dashboard = dashboard;


// End-Point for Connect PWA

exports.app = connectApp;


// End-Point for Biospecimen Dashboard

exports.biospecimen = biospecimenAPIs;


// End-Point for Scheduled Notifications Handler

exports.sendScheduledNotificationsGen2 = onRequest(sendScheduledNotifications);


// End-Points for Exporting Firestore to Big Query

exports.importToBigQuery = importToBigQuery; 

exports.scheduleFirestoreDataExport = firestoreExport;

exports.exportNotificationsToBucket = exportNotificationsToBucket;

exports.importNotificationsToBigquery = importNotificationsToBigquery;

// End-Points for Participant Data Cleaning

exports.participantDataCleanup = participantDataCleanup;

// End-Points for Event Webhook

exports.webhook = webhook;

// End-Points for Public Heartbeat

exports.heartbeat = heartbeat;

// End-Points for Return of Information

exports.physicalActivity = physicalActivity;

// End-Point for Daily DHQ Status Sync and credential check
exports.scheduledSyncDHQ3Status = onRequest(scheduledSyncDHQ3Status);
exports.scheduledCountDHQ3Credentials = onRequest(scheduledCountDHQ3Credentials);
