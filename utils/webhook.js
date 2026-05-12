const { EventWebhook, EventWebhookHeader } = require("@sendgrid/eventwebhook");
const { getResponseJSON, getSecret } = require("./shared");
const firestoreUtils = require("./firestore");
const notificationsUtils = require("./notifications");
const { normalizeEmailAddress, getEmailSuppressionPolicyByReason } = require("./emailSuppressionPolicy");

const SENDGRID_WEBHOOK_CONCURRENCY = 10;

const shouldAcceptSendGridExampleEvents = (query = {}) =>
    query.acceptExampleEvents === "true" || process.env.SENDGRID_ACCEPT_EXAMPLE_EVENTS === "true";

// Keep webhook correlation strict. Example/test events should never mutate
// participant notification history or suppression state.
const isSendGridCorrelatedEvent = (event = {}) =>
    Boolean(event.notification_id) && event.gcloud_project === process.env.GCLOUD_PROJECT;

const processVerifiedSendGridEvents = async (events = [], { acceptExampleEvents = false } = {}) => {
    let failedCount = 0;
    let processedCount = 0;
    let exampleEventsAccepted = 0;
    let ignoredCount = 0;

    for (let i = 0; i < events.length; i += SENDGRID_WEBHOOK_CONCURRENCY) {
        const chunk = events.slice(i, i + SENDGRID_WEBHOOK_CONCURRENCY);
        const results = await Promise.allSettled(chunk.map(async (event) => {
            if (isSendGridCorrelatedEvent(event)) {
                await firestoreUtils.processSendGridEvent(event);
                return { type: "processed" };
            }

            if (acceptExampleEvents) {
                console.log(`Accepted SendGrid example event for testing. event=${event?.event || "unknown"} email=${event?.email || "unknown"}`);
                return { type: "example" };
            }

            console.warn(`Ignoring uncorrelated SendGrid webhook event. event=${event?.event || "unknown"} email=${event?.email || "unknown"}`);
            return { type: "ignored" };
        }));

        for (const result of results) {
            if (result.status === "rejected") {
                failedCount++;
                console.error("SendGrid event processing error:", result.reason);
            } else if (result.value.type === "processed") {
                processedCount++;
            } else if (result.value.type === "example") {
                exampleEventsAccepted++;
            } else if (result.value.type === "ignored") {
                ignoredCount++;
            }
        }
    }

    return {
        processed: processedCount,
        exampleEventsAccepted,
        ignored: ignoredCount,
        failed: failedCount,
    };
};

const handleReceivedTwilioEvent = async (req, res) => {
    const isRequestValid = await notificationsUtils.validateTwilioRequest(req);
    if (!isRequestValid) {
        return res.status(403).json(getResponseJSON("Invalid Twilio signature.", 403));
    }

    try {
        await firestoreUtils.processTwilioEvent(req.body);

        return res.status(200).json({ code: 200 });
    } catch (e) {
        console.error("twilioSmsEventWebhook error", e);
        return res
            .status(500)
            .json(getResponseJSON("Internal Server Error!", 500));
    }
};

const handleReceivedSendGridEvent = async (req, res, { forceAcceptExampleEvents = false } = {}) => {
    try {
        const publicKey = await getSecret(process.env.GCLOUD_SENDGRID_EVENT_WEBHOOKSECRET);
        const eventWebhook = new EventWebhook();
        const ecPublicKey = eventWebhook.convertPublicKeyToECDSA(publicKey);
        const isVerified = eventWebhook.verifySignature(
            ecPublicKey,
            req.rawBody,
            req.get(EventWebhookHeader.SIGNATURE()),
            req.get(EventWebhookHeader.TIMESTAMP()),
        );
        if (!isVerified) {
            return res.status(403).send("Forbidden");
        }

        const summary = await processVerifiedSendGridEvents(req.body, { acceptExampleEvents: forceAcceptExampleEvents || shouldAcceptSendGridExampleEvents(req.query) });
        if (summary.failed > 0) {
            console.error(`SendGrid webhook: ${summary.failed}/${req.body.length} events failed`);
        }
        // Only request a SendGrid retry (500) when the entire batch failed. Partial failures return 200 so SendGrid does not redeliver the already-processed events.
        const allFailed = summary.failed > 0 && summary.processed === 0 && summary.exampleEventsAccepted === 0;
        if (allFailed) {
            return res.status(500).json({
                code: 500,
                ...summary,
            });
        }
        return res.status(200).json({
            code: 200,
            processed: summary.processed,
            exampleEventsAccepted: summary.exampleEventsAccepted,
            ignored: summary.ignored,
            failed: summary.failed,
        });
    } catch (e) {
        console.error(e);
        return res.status(500).json({ code: 500 });
    }
};

const handleEmailUnsubscribeRoute = async (req, res) => {
    const { email, token, sig } = req.query;
    if (!email) return res.status(400).json(getResponseJSON("Missing email.", 400));
    // Verify HMAC signature to prevent unauthorized suppression of arbitrary emails.
    if (!token || !sig) return res.status(403).json(getResponseJSON("Missing authentication.", 403));

    const normalizedEmail = normalizeEmailAddress(email);
    const unsubscribePolicy = getEmailSuppressionPolicyByReason("unsubscribed");

    let secret;
    try {
        secret = await notificationsUtils.resolveUnsubscribeSecret();
    } catch (e) {
        console.error("Failed to resolve unsubscribe secret:", e);
        return res.status(500).json(getResponseJSON("Internal configuration error.", 500));
    }

    const expectedSig = notificationsUtils.generateUnsubscribeSignature(normalizedEmail, token, secret);
    if (sig !== expectedSig) return res.status(403).json(getResponseJSON("Invalid signature.", 403));

    try {
        // This route is a bulk-only unsubscribe action.
        // It should stay in place until the future SendGrid `asm.group_id` implementation handles fine-grained suppression
        // E.g., bulk vs transactional email suppression policy.
        await firestoreUtils.addEmailSuppression(
            normalizedEmail,
            unsubscribePolicy.reason,
            null,
            unsubscribePolicy.suppressBulk,
            unsubscribePolicy.suppressTransactional,
            { token },
        );
        return res.status(200).json({ code: 200 });
    } catch (e) {
        console.error("Unsubscribe error:", e);
        return res.status(500).json({ code: 500 });
    }
};

const webhook = async (req, res) => {
    if (req.method !== "POST") {
        return res
            .status(405)
            .json(getResponseJSON("Only POST requests are accepted!", 405));
    }
    if (!req.body) {
        return res.status(400).json(getResponseJSON("Bad request!", 400));
    }

    const query = req.query;
    if (query.api === "twilio-incoming-sms") {
        return await notificationsUtils.handleIncomingSms(req, res);
    } else if (query.api === "twilio-message-status") {
        return await handleReceivedTwilioEvent(req, res);
    } else if (query.api === "sendgrid-email-status") {
        return await handleReceivedSendGridEvent(req, res);
    } else if (query.api === "sendgrid-email-status-test") {
        // The "-test" route accepts SendGrid example/test events for testing in dev/stage.
        if (process.env.GCLOUD_PROJECT === "nih-nci-dceg-connect-prod-6d04") {
            return res.status(404).json(getResponseJSON("Route not available in production.", 404));
        }
        return await handleReceivedSendGridEvent(req, res, { forceAcceptExampleEvents: true });
    } else if (query.api === "email-unsubscribe") {
        return await handleEmailUnsubscribeRoute(req, res);
    }

    return res.status(400).json(getResponseJSON("Bad request!", 400));
};

module.exports = {
    webhook,
};
