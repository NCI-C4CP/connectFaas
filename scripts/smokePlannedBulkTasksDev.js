#!/usr/bin/env node

/**
 * Smoke-test planned bulk notification task handlers against dev Firestore.
 *
 * This validates Firestore state transitions while SendGrid remains in noop mode.
 * It does not validate Cloud Tasks dispatch/IAM.
 *
 * Usage:
 *   GCLOUD_PROJECT=nih-nci-dceg-connect-dev node scripts/smokePlannedBulkTasksDev.js --execute --cleanup
 *   GCLOUD_PROJECT=nih-nci-dceg-connect-dev node scripts/smokePlannedBulkTasksDev.js --execute --cleanup --stub-unsubscribe-secret
 */

"use strict";

const DEV_PROJECT = "nih-nci-dceg-connect-dev";
const DEV_UNSUBSCRIBE_SECRET = `projects/${DEV_PROJECT}/secrets/sendgrid-unsubscribe-signing-key/versions/1`;
const PRIMARY_FIELD = "d_821247024";
const PRIMARY_VALUE = "197316935";
const EMAIL_FIELD = "d_335767902";
const FIRST_NAME_FIELD = "d_153098809";
const PREFERRED_LANGUAGE_FIELD = "255077064";
const ENGLISH_LANGUAGE_VALUE = "163149180";

const parseArgs = (argv = process.argv.slice(2)) => {
  const args = new Set(argv.filter((arg) => arg.startsWith("--") && !arg.includes("=")));
  const getArg = (name) => {
    const prefix = `--${name}=`;
    const arg = argv.find((entry) => entry.startsWith(prefix));
    return arg ? arg.slice(prefix.length) : "";
  };

  // Cleanup defaults to ON so a partial smoke run does not leave dev
  // artifacts behind. Use --no-cleanup to retain artifacts for inspection.
  const cleanupDefault = !args.has("--no-cleanup");
  return {
    execute: args.has("--execute"),
    cleanup: args.has("--cleanup") || cleanupDefault,
    force: args.has("--force"),
    allowNonDev: args.has("--allow-non-dev"),
    stubUnsubscribeSecret: args.has("--stub-unsubscribe-secret"),
    project: getArg("project") || process.env.GCLOUD_PROJECT || DEV_PROJECT,
    runId: getArg("run-id"),
    specId: getArg("spec-id"),
  };
};

const assertFirestoreId = (value = "", label = "id") => {
  if (!value || value.includes("/")) {
    throw new Error(`${label} must be a non-empty Firestore document id without slashes.`);
  }
};

const getEasternDateKey = (date = new Date()) =>
  new Intl.DateTimeFormat("en-CA", { timeZone: "America/New_York" }).format(date);

const makeTimestampKey = () => new Date().toISOString().replace(/[-:.TZ]/g, "").slice(0, 14);

const makeRecipient = ({ runId, suffix, email }) => ({
  Connect_ID: `SMOKE-${suffix.toUpperCase()}`,
  token: `${runId}-${suffix}`,
  state: { uid: `uid-${runId}-${suffix}` },
  [PREFERRED_LANGUAGE_FIELD]: ENGLISH_LANGUAGE_VALUE,
  [EMAIL_FIELD]: email,
  [FIRST_NAME_FIELD]: "Smoke",
});

const makeInitialCounts = (planned) => ({
  planned,
  sent: 0,
  filtered: 0,
  suppressed: 0,
  providerFailed: 0,
  providerUnknown: 0,
});

const makeInitialUnsuccessful = () => ({
  filtered: [],
  suppressed: [],
  providerFailed: [],
  providerUnknown: [],
});

const makeBatchDoc = ({
  runId,
  specId,
  runDateKey,
  runSequence,
  lane,
  batchNumber,
  recipients,
}) => ({
  id: `${lane}-batch-${batchNumber}`,
  runId,
  specId,
  runDateKey,
  runSequence,
  lane,
  batchNumber,
  batchSize: recipients.length,
  recipientCount: recipients.length,
  recipients,
  scheduleDelaySeconds: 0,
  status: "planned",
  counts: makeInitialCounts(recipients.length),
  unsuccessful: makeInitialUnsuccessful(),
  createdAt: new Date().toISOString(),
});

const buildSmokePlan = ({ runId, specId }) => {
  const runDateKey = getEasternDateKey();
  const runSequence = 1;
  const now = new Date().toISOString();
  const notificationSpec = {
    id: specId,
    category: "newsletter",
    attempt: "smoke",
    primaryField: PRIMARY_FIELD,
    time: { start: { day: 0, hour: 1, minute: 0 }, stop: { day: 0, hour: 0, minute: 0 } },
    notificationType: ["email"],
    emailField: EMAIL_FIELD,
    firstNameField: FIRST_NAME_FIELD,
    email: {
      english: {
        subject: "Planned Bulk Smoke Test",
        body: "<p>Hello {{firstName}}</p>",
      },
    },
    sms: {},
    conditions: JSON.stringify([[PRIMARY_FIELD, "equals", PRIMARY_VALUE]]),
    // isDraft must be true so that if the smoke run leaves an artifact behind (cleanup interrupted, --no-cleanup explicitly passed, etc.),
    // the daily scheduler does not pick this spec up and dispatch real bulk runs.
    isDraft: true,
    scheduleAt: "codex-smoke",
  };

  const defaultRecipients = [
    makeRecipient({ runId, suffix: "default", email: `${runId}-default@test.gov` }),
    makeRecipient({ runId, suffix: "filtered", email: "noreply@nih.gov" }),
  ];
  const microsoftRecipients = [
    makeRecipient({ runId, suffix: "microsoft", email: `${runId}-microsoft@outlook.com` }),
  ];
  const batchDocs = [
    makeBatchDoc({
      runId,
      specId,
      runDateKey,
      runSequence,
      lane: "default",
      batchNumber: 1,
      recipients: defaultRecipients,
    }),
    makeBatchDoc({
      runId,
      specId,
      runDateKey,
      runSequence,
      lane: "microsoft",
      batchNumber: 1,
      recipients: microsoftRecipients,
    }),
  ];

  const runDoc = {
    id: runId,
    specId,
    category: notificationSpec.category,
    attempt: notificationSpec.attempt,
    runDateKey,
    runSequence,
    status: "planned",
    notificationSpec,
    totalRecipientCount: defaultRecipients.length + microsoftRecipients.length,
    plannedRecipientCount: defaultRecipients.length + microsoftRecipients.length,
    laneRecipientCounts: {
      default: defaultRecipients.length,
      microsoft: microsoftRecipients.length,
    },
    laneBatchCounts: {
      default: 1,
      microsoft: 1,
    },
    settings: {
      bulkDefaultBatchSize: defaultRecipients.length,
      bulkMicrosoftBatchSize: microsoftRecipients.length,
      targetRecipientsPerHour: 5000,
      targetRecipientsPerHourMicrosoft: 1500,
      microsoftBulkDomains: ["outlook.com", "hotmail.com", "live.com", "msn.com"],
    },
    timeParams: {
      startTimeStr: now,
      stopTimeStr: now,
      timeField: PRIMARY_FIELD,
    },
    conditions: [{ field: PRIMARY_FIELD, operator: "equals", value: PRIMARY_VALUE }],
    createdAt: now,
  };

  return { runDoc, batchDocs };
};

const getBatchById = (batches = [], batchId = "") => batches.find((batch) => batch.id === batchId) || {};

const cleanupSmokeArtifacts = async ({ db, runId, specId, notificationIds }) => {
  const runRef = db.collection("notificationBulkRuns").doc(runId);
  const batchSnapshot = await runRef.collection("batches").get();
  const deleteBatch = db.batch();

  batchSnapshot.docs.forEach((doc) => deleteBatch.delete(doc.ref));
  deleteBatch.delete(runRef);
  deleteBatch.delete(db.collection("notificationSpecifications").doc(specId));
  notificationIds.forEach((notificationId) => {
    deleteBatch.delete(db.collection("notifications").doc(notificationId));
  });
  await deleteBatch.commit();
};

const assertNoopSafety = async ({ db, project, allowNonDev }) => {
  if (project !== DEV_PROJECT && !allowNonDev) {
    throw new Error(`Refusing to run against ${project}. Pass --allow-non-dev only for an intentional non-dev noop test.`);
  }

  const settingsSnapshot = await db.collection("appSettings").doc("connectFaas").get();
  const notifications = settingsSnapshot.data()?.notifications || {};
  const deliveryOverride = typeof notifications.sendgridDeliveryModeOverride === "string"
    ? notifications.sendgridDeliveryModeOverride.toLowerCase()
    : "";
  if (deliveryOverride && deliveryOverride !== "noop") {
    throw new Error(
      `connectFaas.notifications.sendgridDeliveryModeOverride is "${deliveryOverride}". ` +
      "Set it to noop or remove it before this smoke test.",
    );
  }
};

const run = async () => {
  const args = parseArgs();
  const runId = args.runId || `codex-planned-bulk-smoke-${makeTimestampKey()}`;
  const specId = args.specId || runId;
  assertFirestoreId(runId, "runId");
  assertFirestoreId(specId, "specId");

  const plan = buildSmokePlan({ runId, specId });
  if (!args.execute) {
    console.log("Dry run only. No Firestore writes performed.");
    console.log(JSON.stringify({
      project: args.project,
      runId,
      specId,
      batchIds: plan.batchDocs.map((batchDoc) => batchDoc.id),
      recipientCounts: plan.runDoc.laneRecipientCounts,
    }, null, 2));
    console.log("\nRun with --execute to write the smoke run and invoke local handlers.");
    return;
  }

  process.env.GCLOUD_PROJECT = args.project;
  process.env.GCP_PROJECT = process.env.GCP_PROJECT || args.project;
  if (!process.env.GCLOUD_UNSUBSCRIBE_SECRET && args.project === DEV_PROJECT) {
    process.env.GCLOUD_UNSUBSCRIBE_SECRET = DEV_UNSUBSCRIBE_SECRET;
  }

  if (args.stubUnsubscribeSecret) {
    const sharedUtils = require("../utils/shared");
    const originalGetSecret = sharedUtils.getSecret;
    sharedUtils.getSecret = async (key) => (
      key === process.env.GCLOUD_UNSUBSCRIBE_SECRET
        ? "local-smoke-unsubscribe-secret"
        : originalGetSecret(key)
    );
  }

  const admin = require("firebase-admin");
  const {
    saveBulkNotificationRunPlan,
    markBulkNotificationBatchEnqueued,
    getBulkNotificationRun,
    getBulkNotificationRunBatches,
    getNotificationRecordId,
  } = require("../utils/firestore");
  const {
    processNotificationBatchBulkDefault,
    processNotificationBatchBulkMicrosoft,
  } = require("../utils/notifications");
  const db = admin.firestore();
  const notificationIds = plan.batchDocs
    .flatMap((batchDoc) => batchDoc.recipients)
    .filter((recipient) => recipient[EMAIL_FIELD] !== "noreply@nih.gov")
    .map((recipient) => getNotificationRecordId({
      notificationSpecificationsID: specId,
      notificationType: "email",
      token: recipient.token,
    }));

  try {
    await assertNoopSafety({ db, project: args.project, allowNonDev: args.allowNonDev });

    const runSnapshot = await db.collection("notificationBulkRuns").doc(runId).get();
    const specSnapshot = await db.collection("notificationSpecifications").doc(specId).get();
    if (!args.force && (runSnapshot.exists || specSnapshot.exists)) {
      throw new Error(`Smoke ids already exist (${runId}/${specId}). Use fresh ids or pass --force.`);
    }

    await db.collection("notificationSpecifications").doc(specId).set({
      ...plan.runDoc.notificationSpec,
      queuedBulkRunDateKey: plan.runDoc.runDateKey,
      queuedBulkRunSequence: plan.runDoc.runSequence,
      queuedBulkRunUpdatedAt: new Date().toISOString(),
      bulkRunSequence: plan.runDoc.runSequence,
      createdBy: "smokePlannedBulkTasksDev",
    });
    await saveBulkNotificationRunPlan(plan);

    for (const batchDoc of plan.batchDocs) {
      await markBulkNotificationBatchEnqueued({
        runId,
        batchId: batchDoc.id,
        taskId: `${runId}-${batchDoc.id}`,
        queueName: batchDoc.lane === "microsoft"
          ? "processNotificationBatchBulkMicrosoft"
          : "processNotificationBatchBulkDefault",
        scheduleDelaySeconds: 0,
        scheduledFor: new Date().toISOString(),
      });
    }

    for (const batchDoc of plan.batchDocs) {
      const handler = batchDoc.lane === "microsoft"
        ? processNotificationBatchBulkMicrosoft
        : processNotificationBatchBulkDefault;
      await handler({
        data: {
          runId,
          batchId: batchDoc.id,
          lane: batchDoc.lane,
          specId,
          runDateKey: plan.runDoc.runDateKey,
          runSequence: plan.runDoc.runSequence,
        },
        id: `${runId}-${batchDoc.id}`,
        retryCount: 0,
      });
    }

    const [finalRun, finalBatches, finalSpec, notificationSnapshots] = await Promise.all([
      getBulkNotificationRun(runId),
      getBulkNotificationRunBatches(runId),
      db.collection("notificationSpecifications").doc(specId).get(),
      db.getAll(...notificationIds.map((id) => db.collection("notifications").doc(id))),
    ]);

    const defaultBatch = getBatchById(finalBatches, "default-batch-1");
    const microsoftBatch = getBatchById(finalBatches, "microsoft-batch-1");
    const finalSpecData = finalSpec.data() || {};
    const acceptedNotifications = notificationSnapshots.filter((snapshot) => {
      const data = snapshot.data() || {};
      return snapshot.exists && data.processingState === "provider_accepted" && data.isSent === true;
    });

    if (finalRun?.status !== "complete") throw new Error(`Expected run complete, found ${finalRun?.status || "missing"}.`);
    if (defaultBatch.status !== "complete") throw new Error(`Expected default batch complete, found ${defaultBatch.status}.`);
    if (microsoftBatch.status !== "complete") throw new Error(`Expected microsoft batch complete, found ${microsoftBatch.status}.`);
    if (defaultBatch.counts?.planned !== 2 || defaultBatch.counts?.sent !== 1 || defaultBatch.counts?.filtered !== 1) {
      throw new Error(`Unexpected default batch counts: ${JSON.stringify(defaultBatch.counts)}`);
    }
    if (microsoftBatch.counts?.planned !== 1 || microsoftBatch.counts?.sent !== 1) {
      throw new Error(`Unexpected microsoft batch counts: ${JSON.stringify(microsoftBatch.counts)}`);
    }
    if (acceptedNotifications.length !== notificationIds.length) {
      throw new Error(`Expected ${notificationIds.length} accepted notifications, found ${acceptedNotifications.length}.`);
    }
    if (finalSpecData.lastRunDateKey !== plan.runDoc.runDateKey || finalSpecData.queuedBulkRunDateKey) {
      throw new Error(`Spec queued/lastRun markers did not finalize as expected: ${JSON.stringify(finalSpecData)}`);
    }

    console.log("Planned bulk task smoke test passed.");
    console.log(JSON.stringify({
      project: args.project,
      runId,
      specId,
      runStatus: finalRun.status,
      batches: finalBatches.map((batch) => ({
        id: batch.id,
        status: batch.status,
        counts: batch.counts,
      })),
      notificationIds,
      cleanup: args.cleanup,
    }, null, 2));
  } finally {
    if (args.cleanup) {
      await cleanupSmokeArtifacts({ db, runId, specId, notificationIds });
      console.log(`Cleaned up smoke artifacts for ${runId}.`);
    }
  }
};

run().catch((error) => {
  console.error("Planned bulk task smoke test failed:", error);
  process.exit(1);
});
