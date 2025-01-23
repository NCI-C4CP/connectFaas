const { getBigQueryData } = require('./bigquery');
const { retrieveConnectID } = require('./firestore');
const { getResponseJSON } = require('./shared');

const retrievePhysicalActivityReport = async (req, res, uid, connectId) => {
    if (req.method !== "GET") {
        return res.status(405).json(getResponseJSON("Only GET requests are accepted!", 405));
    }

    //If there is a uid passed in lookup the connect id from the participant
    if (uid) {
        connectId = await retrieveConnectID(uid);
    }

    //If the connectId  an object (error from retrieveConnectID)
    if (typeof connectId === 'object') {
        return res.status(500).json(getResponseJSON("Error Finding Connect ID", 500));
    }
    //If the connectId is missing then we can not continue
    else if (!connectId) {
        return res.status(400).json(getResponseJSON("Connect ID Not Passed", 400));
    }

    connectId = '6759772253';

    try {
        let filters = [
            {
                'column': 'Connect_ID',
                'operator': '=',
                'value': ''+connectId
            }
        ];
        const reportData = await getBigQueryData('ROI', 'physical_activity', filters);
        return res.status(200).json({ data: reportData, message: "Success", code: 200 });
    } catch (error) {
        console.error("Error when retrieving physical activity report.", error);
        return res.status(500).json(getResponseJSON("Internal Server Error", 500));
    }
};

module.exports = {
    retrievePhysicalActivityReport
};