const { SecretManagerServiceClient } = require("@google-cloud/secret-manager");
const { EventWebhook, EventWebhookHeader } = require("@sendgrid/eventwebhook");
const { getResponseJSON } = require("./shared");
const { processTwilioEvent, processSendGridEvent } = require("./firestore");
const { handleIncomingSms } = require("./notifications");

/* This function will process the webhook data from Twilio */
const handleReceivedTwilioEvent = async (req, res) => {
    try {
        await processTwilioEvent(req.body);

        return res.status(200).json({ code: 200 });
    } catch (e) {
        console.error("twilioSmsEventWebhook error", e);
        return res
            .status(500)
            .json(getResponseJSON("Internal Server Error!", 500));
    }
};

const handleReceivedSendGridEvent = async (req, res) => {
    try {
        const client = new SecretManagerServiceClient();
        const [version] = await client.accessSecretVersion({
            name: process.env.GCLOUD_SENDGRID_EVENT_WEBHOOKSECRET,
        });
        const publicKey = version.payload.data.toString();
        const eventWebhook = new EventWebhook();
        const ecPublicKey = eventWebhook.convertPublicKeyToECDSA(publicKey);
        const payload = req.rawBody;
        const signature = req.get(EventWebhookHeader.SIGNATURE());
        const timestamp = req.get(EventWebhookHeader.TIMESTAMP());

        const verification = eventWebhook.verifySignature(
            ecPublicKey,
            payload,
            signature,
            timestamp
        );

        if (!verification) {
            return res.status(403).send("Forbidden");
        }

        const events = req.body;
        for (let event of events) {
            await processSendGridEvent(event);
        }
        return res.status(200).json({ code: 200 });
    } catch (e) {
        console.error(e);
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
      return await handleIncomingSms(req, res);
    } else if (query.api === "twilio-message-status") {
        return await handleReceivedTwilioEvent(req, res);
    } else if (query.api === "sendgrid-email-status") {
        return await handleReceivedSendGridEvent(req, res);
    }
    
    return res.status(400).json(getResponseJSON("Bad request!", 400));
};

module.exports = {
    webhook,
};
