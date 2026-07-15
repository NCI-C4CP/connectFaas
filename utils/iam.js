const { google } = require('googleapis');

const LEGACY_KEY_THRESHOLD_YEARS = 1;
const TWO_WEEKS_MS = 14 * 24 * 60 * 60 * 1000;

const getIamClient = async () => {
    const auth = new google.auth.GoogleAuth({
        scopes: ['https://www.googleapis.com/auth/cloud-platform'],
    });
    const authClient = await auth.getClient();
    return google.iam({ version: 'v1', auth: authClient });
};

const isLegacyKey = (validBeforeTime) => {
    if (!validBeforeTime) return true;
    const expiry = new Date(validBeforeTime);
    const threshold = new Date();
    threshold.setFullYear(threshold.getFullYear() + LEGACY_KEY_THRESHOLD_YEARS);
    return expiry > threshold;
};

/**
 * Lists active USER_MANAGED keys for a service account with expiration info.
 * @param {string} saEmail - The service account email address
 * @returns {Promise<Array<object>>} Array of key info objects
 */
const listServiceAccountKeys = async (saEmail) => {
    const iam = await getIamClient();

    const response = await iam.projects.serviceAccounts.keys.list({
        name: `projects/-/serviceAccounts/${saEmail}`,
        keyTypes: ['USER_MANAGED'],
    });

    const keys = response.data.keys || [];
    const now = new Date();

    return keys
        .filter(key => {
            if (!key.validBeforeTime) return true;
            return new Date(key.validBeforeTime) > now;
        })
        .map(key => {
            const legacy = isLegacyKey(key.validBeforeTime);
            const keyId = key.name.split('/').pop();
            return {
                keyId,
                name: key.name,
                createdAt: key.validAfterTime || null,
                expiresAt: legacy ? null : key.validBeforeTime,
                isLegacy: legacy,
            };
        });
};

/**
 * Validates whether a new key can be created based on active key count and expiration.
 * @param {Array<object>} activeKeys - Result from listServiceAccountKeys
 * @returns {{ allowed: boolean, reason: string|null }}
 */
const validateKeyCreation = (activeKeys) => {
    if (activeKeys.length >= 2) {
        return { allowed: false, reason: 'Maximum of 2 active keys reached. Delete an existing key before creating a new one.' };
    }

    const nonLegacyKeys = activeKeys.filter(k => !k.isLegacy);
    for (const key of nonLegacyKeys) {
        const timeRemaining = new Date(key.expiresAt) - new Date();
        if (timeRemaining > TWO_WEEKS_MS) {
            const expiresDate = new Date(key.expiresAt).toISOString().split('T')[0];
            return { allowed: false, reason: `An active key still has more than 2 weeks before expiration (expires ${expiresDate}). New keys can only be created within 2 weeks of an existing non-legacy key expiring.` };
        }
    }

    return { allowed: true, reason: null };
};

/**
 * Creates a new service account key and returns the parsed JSON key file.
 * @param {string} saEmail - The service account email address from siteDetails.saEmail
 * @returns {Promise<object>} The parsed JSON key file object
 */
const createServiceAccountKey = async (saEmail) => {
    const iam = await getIamClient();

    const response = await iam.projects.serviceAccounts.keys.create({
        name: `projects/-/serviceAccounts/${saEmail}`,
        requestBody: {
            privateKeyType: 'TYPE_GOOGLE_CREDENTIALS_FILE',
        },
    });

    const { privateKeyData, name } = response.data || {};
    if (!privateKeyData) {
        throw new Error(`IAM key creation did not return privateKeyData for ${saEmail}`);
    }

    const keyJson = JSON.parse(Buffer.from(privateKeyData, 'base64').toString('utf8'));

    console.log(`Service account key created — saEmail: ${saEmail}, keyName: ${name}, time: ${new Date().toISOString()}`);

    return keyJson;
};

module.exports = {
    createServiceAccountKey,
    listServiceAccountKeys,
    validateKeyCreation,
};
