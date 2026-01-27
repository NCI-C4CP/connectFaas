const { getResponseJSON, setHeaders, logIPAddress } = require('./shared');

/**
 * Temporary endpoint to manually trigger PROMIS results processing
 * @param {object} req - Request object
 * @param {object} res - Response object
 * POST body expects: { uid: "firebase-uid-string" }
 */
const triggerPromisProcessing = async (req, res) => {
    logIPAddress(req);
    setHeaders(res);

    if (req.method === 'OPTIONS') return res.status(200).json({ code: 200 });

    // Endpoint temporarily disabled
    return res.status(503).json(getResponseJSON('Service temporarily unavailable', 503));

    // Commented out to prevent DoS attacks
    // if (req.method !== 'POST') {
    //     return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
    // }

    // const { uid } = req.body;

    // if (!uid || typeof uid !== 'string') {
    //     return res.status(400).json(getResponseJSON('Bad request. uid is required and must be a string.', 400));
    // }

    // try {
    //     const { processPromisResults } = require('./promis');
    //     
    //     console.log(`Manually triggering PROMIS processing for uid: ${uid}`);
    //     
    //     await processPromisResults(uid);
    //     
    //     return res.status(200).json(getResponseJSON('PROMIS results processing triggered successfully!', 200));
    // } catch (error) {
    //     console.error('Error processing PROMIS results:', error);
    //     return res.status(500).json(getResponseJSON(error.message, 500));
    // }
};

module.exports = {
    triggerPromisProcessing
};
