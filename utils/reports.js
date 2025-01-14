const { logIPAddress, setHeaders } = require('./shared');
const fieldMapping = require('./fieldToConceptIdMapping');
const {BigQuery} = require('@google-cloud/bigquery');

const bigquery = new BigQuery();

const physicalActivity = async (req, res) => {
    logIPAddress(req);
    setHeaders(res);

    if(req.method === 'OPTIONS') {
        return res.status(200).json({code: 200});
    }

    if(req.method !== 'GET') {
        return res.status(405).json({ code: 405, data: 'Only GET requests are accepted!'});
    }

    let dateExpression = 'CURRENT_DATE()';
    const { year, month, day } = req.query;

    if (year && month && day) dateExpression = `'${year}-${month}-${day}'`;

    const query = `
        SELECT *
        FROM \`${process.env.GCLOUD_PROJECT}.ROI.physical_activity\`
        WHERE DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', d_416831581)) = ${dateExpression}
    `;

    try {
        const [rows] = await bigquery.query(query);

        const updatePromises = rows.map(async (row) => {
            const { Connect_ID } = row;

            if (!Connect_ID) {
                console.warn('BQ Row missing Connect_ID:', row);
                return; 
            }
      
            const firestoreQuery = firestore.collection('participants').where('Connect_ID', '==', parseInt(Connect_ID));
            const snapshot = await firestoreQuery.get();

            if (!snapshot.size) {
                console.warn(`Firestore Doc not found for Connect_ID: ${Connect_ID}`);
                return;
            }

            const doc = snapshot.docs[0];
            const data = doc.data();
            const reportStatus = data[fieldMapping.physicalActivity]?.[fieldMapping.physicalActivityStatus];

            if (reportStatus == null) { 
                const payload = { [`${fieldMapping.physicalActivity}.${fieldMapping.physicalActivityStatus}`]: fieldMapping.reportStatus.unread };
                await firestore.collection('participants').doc(doc.id).update(payload);
            }
        });

        await Promise.all(updatePromises);

        return res.status(200).json({ message: 'good', code: 200});
    } catch (error) {
        console.error(error);
        return res.status(500).json({ message: error.toString(), error });
    }
}

module.exports = {
    physicalActivity
}