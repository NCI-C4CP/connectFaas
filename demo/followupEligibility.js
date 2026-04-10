const { setHeaders, getResponseJSON, logIPAddress, APIAuthorization, isParentEntity } = require('../utils/shared');
const { isDev } = require('./shared');

const followupEligibility = async (req, res) => {
    logIPAddress(req);
    setHeaders(res);

    console.log(`[followupEligibility] ${req.method} request received`);
    console.log(`[followupEligibility] Headers present: ${Object.keys(req.headers).join(', ')}`);
    console.log(`[followupEligibility] Authorization header: ${req.headers.authorization ? req.headers.authorization.substring(0, 20) + '...' : 'MISSING'}`);

    if (req.method === 'OPTIONS') return res.status(200).json({ code: 200 });

    if (!isDev(req, res)) {
        console.log('[followupEligibility] Blocked by dev-only guard');
        return;
    }

    if (req.method !== 'GET') {
        console.log(`[followupEligibility] Rejected method: ${req.method}`);
        return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
    }

    console.log('[followupEligibility] Calling APIAuthorization...');
    const authorized = await APIAuthorization(req);

    if (authorized instanceof Error) {
        console.error('[followupEligibility] APIAuthorization returned error:', authorized.message);
        return res.status(500).json(getResponseJSON(authorized.message, 500));
    }

    if (!authorized) {
        console.log('[followupEligibility] APIAuthorization returned false — token invalid or email not in siteDetails');
        return res.status(401).json(getResponseJSON('Authorization failed!', 401));
    }

    console.log(`[followupEligibility] Authorized: ${authorized.saEmail}, siteCode: ${authorized.siteCode}`);
    console.log(`Demo API: Followup Eligibility, accessed by: ${authorized.saEmail}`);

    const { isParent, siteCodes } = await isParentEntity(authorized);

    console.log(`[followupEligibility] isParent: ${isParent}, siteCodes: ${JSON.stringify(siteCodes)}, type: ${typeof siteCodes === 'object' ? siteCodes.map(s => typeof s) : typeof siteCodes}`);

    try {
        const admin = require('firebase-admin');
        const db = admin.firestore();

        const token = req.query.token;

        let snapshot;
        if (token) {
            const { getParticipantData } = require('../utils/firestore');
            const participant = await getParticipantData(token, siteCodes, isParent);

            if (!participant) {
                return res.status(404).json(getResponseJSON('Participant not found or does not belong to your site.', 404));
            }

            snapshot = await db.collection('activities')
                .where('token', '==', token)
                .get();
        } else {
            const operator = isParent ? 'in' : '==';
            console.log(`[followupEligibility] Querying activities: siteCode ${operator} ${JSON.stringify(siteCodes)}`);
            snapshot = await db.collection('activities')
                .where('siteCode', operator, siteCodes)
                .get();
            console.log(`[followupEligibility] Activities query returned ${snapshot.size} documents`);
        }

        const activities = [];
        snapshot.forEach(doc => {
            const data = doc.data();
            activities.push({
                connectId: data.connectId,
                token: data.token,
                round: data.round,
                activityType: data.activityType,
                activities: data.activities,
                creationDate: data.creationDate,
            });
        });

        return res.status(200).json({ data: activities, code: 200 });
    } catch (error) {
        console.error('Error in followupEligibility:', error);
        return res.status(500).json(getResponseJSON('An error occurred. Please try again later.', 500));
    }
};

module.exports = { followupEligibility };
