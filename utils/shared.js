const getResponseJSON = (message, code) => {
    return { message, code };
};

const setHeaders = (res) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers','Accept,Content-Type,Content-Length,Accept-Encoding,X-CSRF-Token,Authorization');
    res.header('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS');
}

const generateConnectID = () => {
    return Math.floor(Math.random() * (9999999999 - 1000000000)) + 1000000000;
}

const generatePIN = () => {
    return Math.floor(Math.random() * (999999 - 100000)) + 100000;
}

const randomString = () => {
    const length = 6;
    return (Math.round((Math.pow(36, length + 1) - Math.random() * Math.pow(36, length))).toString(36).slice(1)).toUpperCase();
}

const deleteDocuments = (req, res) => {
    setHeaders(res);
    
    if(req.method === 'OPTIONS') return res.status(200).json({code: 200});

    if(req.method !== 'GET') {
        res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
    }

    const siteCode = 8;
    const { deleteFirestoreDocuments } = require('./firestore')
    deleteFirestoreDocuments(siteCode)
    res.status(200).json(getResponseJSON('Success!', 200))
}

const lockedAttributes = () => { // Read only access after initialization
    return ["RcrtV_Verification_v1r0", "RcrtSI_Account_v1r0", "RcrtUP_Submitted_v1r0", ""]
}

module.exports = {
    getResponseJSON,
    setHeaders,
    generateConnectID,
    generatePIN,
    randomString,
    deleteDocuments
}