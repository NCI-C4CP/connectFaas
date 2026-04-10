const { setHeaders, getResponseJSON, logIPAddress, APIAuthorization, isParentEntity } = require('../utils/shared');
const { validRounds, validCollectionTypes, validStatuses, isDev } = require('./shared');
const { validateIso8601Timestamp } = require('../utils/validation');

const followupCollections = async (req, res) => {
    logIPAddress(req);
    setHeaders(res);

    if (req.method === 'OPTIONS') return res.status(200).json({ code: 200 });

    if (!isDev(req, res)) return;

    if (req.method !== 'POST') {
        return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
    }

    const authorized = await APIAuthorization(req);

    if (authorized instanceof Error) {
        return res.status(500).json(getResponseJSON(authorized.message, 500));
    }

    if (!authorized) {
        return res.status(401).json(getResponseJSON('Authorization failed!', 401));
    }

    console.log(`Demo API: Followup Collections, accessed by: ${authorized.saEmail}`);

    const { isParent, siteCodes } = await isParentEntity(authorized);

    if (req.body.data === undefined) {
        return res.status(400).json(getResponseJSON('Bad request. data is not defined in request body.', 400));
    }

    if (!Array.isArray(req.body.data)) {
        return res.status(400).json(getResponseJSON('Bad request. data must be an array.', 400));
    }

    if (req.body.data.length === 0) {
        return res.status(400).json(getResponseJSON('Bad request. data array does not have any elements.', 400));
    }

    if (req.body.data.length > 499) {
        return res.status(400).json(getResponseJSON('Bad request. data contains more than acceptable limit of 500 records.', 400));
    }

    const { getParticipantData } = require('../utils/firestore');
    const admin = require('firebase-admin');
    const db = admin.firestore();

    const dataArray = req.body.data;
    const responseArray = [];
    let hasErrors = false;

    for (const item of dataArray) {
        const errors = [];

        if (item.token === undefined) {
            hasErrors = true;
            responseArray.push({ 'Invalid Request': { Token: 'UNDEFINED', Errors: 'token not defined in data object.' } });
            continue;
        }

        const token = item.token;

        if (!item.round) {
            errors.push('round is required.');
        } else if (!validRounds.includes(item.round)) {
            errors.push(`Invalid round "${item.round}". Must be one of: ${validRounds.join(', ')}.`);
        }

        if (!item.type) {
            errors.push('type is required.');
        } else if (!validCollectionTypes.includes(item.type)) {
            errors.push(`Invalid type "${item.type}". Must be one of: ${validCollectionTypes.join(', ')}.`);
        }

        if (!item.status) {
            errors.push('status is required.');
        } else if (!validStatuses.includes(item.status)) {
            errors.push(`Invalid status "${item.status}". Must be one of: ${validStatuses.join(', ')}.`);
        }

        if (item.date && !validateIso8601Timestamp(item.date)) {
            errors.push('date must be a valid ISO 8601 timestamp.');
        }

        if (errors.length > 0) {
            hasErrors = true;
            responseArray.push({ 'Invalid Request': { Token: token, Errors: errors.join(' ') } });
            continue;
        }

        const participant = await getParticipantData(token, siteCodes, isParent);

        if (!participant) {
            hasErrors = true;
            responseArray.push({ 'Invalid Request': { Token: token, Errors: 'Token does not exist or does not belong to your site.' } });
            continue;
        }

        const connectId = participant.data.Connect_ID;

        try {
            const collectionDoc = {
                connectId,
                token,
                round: item.round,
                type: item.type,
                status: item.status,
                siteCode: authorized.siteCode,
                createdAt: new Date().toISOString(),
            };

            if (item.date) collectionDoc.date = item.date;
            if (item.location) collectionDoc.location = item.location;

            await db.collection('siteCollections').add(collectionDoc);

            responseArray.push({ Success: { Token: token, Round: item.round, Type: item.type } });
        } catch (error) {
            console.error(`Error writing collection for token ${token}:`, error);
            hasErrors = true;
            responseArray.push({ 'Server Error': { Token: token, Errors: 'Failed to save collection data.' } });
        }
    }

    const statusCode = hasErrors ? 206 : 200;
    return res.status(statusCode).json({ data: responseArray, code: statusCode });
};

module.exports = { followupCollections };
