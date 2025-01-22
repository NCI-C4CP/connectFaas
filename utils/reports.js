const { processPhysicalActivity } = require('./firestore');
const { logIPAddress, setHeaders } = require('./shared');

const physicalActivity = async (req, res) => {
    logIPAddress(req);
    setHeaders(res);

    if(req.method === 'OPTIONS') {
        return res.status(200).json({code: 200});
    }

    if(req.method !== 'GET') {
        return res.status(405).json({ code: 405, data: 'Only GET requests are accepted!'});
    }

    try {
        let dateExpression = 'CURRENT_DATE()';
        const { year, month, day } = req.query;

        if (year && month && day) dateExpression = `'${year}-${month}-${day}'`;

        return await processPhysicalActivity(dateExpression);

    } catch (error) {
        console.error(error);
    
        return res.status(500).json({ message: error.toString(), error });
    }
}

module.exports = {
    physicalActivity
}