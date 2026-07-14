const { google } = require('googleapis');

/**
 * Creates a new service account key and returns the parsed JSON key file.
 * @param {string} saEmail - The service account email address from siteDetails.saEmail
 * @returns {Promise<object>} The parsed JSON key file object
 */
const createServiceAccountKey = async (saEmail) => {
    const auth = new google.auth.GoogleAuth({
        scopes: ['https://www.googleapis.com/auth/cloud-platform'],
    });

    const authClient = await auth.getClient();

    const iam = google.iam({
        version: 'v1',
        auth: authClient,
    });

    const response = await iam.projects.serviceAccounts.keys.create({
        name: `projects/-/serviceAccounts/${saEmail}`,
        requestBody: {
            privateKeyType: 'TYPE_GOOGLE_CREDENTIALS_FILE',
        },
    });

    const privateKeyData = Buffer.from(response.data.privateKeyData, 'base64').toString();
    const keyJson = JSON.parse(privateKeyData);

    console.log(`Service account key created — saEmail: ${saEmail}, keyName: ${response.data.name}, time: ${new Date().toISOString()}`);

    return keyJson;
};

module.exports = {
    createServiceAccountKey,
};
