const { setHeaders, getResponseJSON, logIPAddress, APIAuthorization, isParentEntity, isDateTimeFormat } = require('./shared');

const getCgrPackagesInTransit = async (req, res) => {
    logIPAddress(req);
    setHeaders(res);

    if (req.method !== 'GET') {
        return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
    }

    const authorized = await APIAuthorization(req);

    if(authorized instanceof Error) return res.status(500).json(getResponseJSON(authorized.message, 500));
    if(!authorized) return res.status(401).json(getResponseJSON('Authorization failed!', 401));

    try {
        const { cgrPackagesInTransit } = require('./firestore');
        const response = await cgrPackagesInTransit(req.query.startDate, req.query.endDate, 'exclude');
        return res.status(200).json({code: 200, ...response});
    } catch(err) {
        console.error('Error in getCgrPackagesInTransit', err);
        return res.status(500).json({error: err.message || err, code: 500});
    }
    
}

const getCgrPackagesLost = async (req, res) => {
    logIPAddress(req);
    setHeaders(res);

    if (req.method !== 'GET') {
        return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
    }

    const authorized = await APIAuthorization(req);

    if(authorized instanceof Error) return res.status(500).json(getResponseJSON(authorized.message, 500));
    if(!authorized) return res.status(401).json(getResponseJSON('Authorization failed!', 401));

    try {
        const { cgrPackagesInTransit } = require('./firestore');
        const response = await cgrPackagesInTransit(req.query.startDate, req.query.endDate, 'only');
        return res.status(200).json({code: 200, ...response});
    } catch(err) {
        console.error('Error in getCgrPackagesInTransit', err);
        return res.status(500).json({error: err.message || err, code: 500});
    }
}

module.exports = {
    getCgrPackagesInTransit,
    getCgrPackagesLost
};