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

    try {
        let dateExpression = 'CURRENT_DATE()';
        const { year, month, day } = req.query;

        if (year && month && day) dateExpression = `'${year}-${month}-${day}'`;

        const query = `
            SELECT *
            FROM \`${process.env.GCLOUD_PROJECT}.ROI.physical_activity\`
            WHERE DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', d_416831581)) = ${dateExpression}
        `;

        const [rows] = await bigquery.query(query);
        const CHUNK_SIZE = 500;

        let updates = 0;

        for (let i = 0; i < rows.length; i+= CHUNK_SIZE) {
            const chunk = rows.slice(i, i + CHUNK_SIZE);
            const batch = firestore.batch();
            
            updates = 0;

            const promises = chunk.map(async (row) => {
                const { Connect_ID } = row;

                if (!Connect_ID) {
                    console.error('Row missing Connect_ID:', row);
                    return; 
                }

                const snapshot = await firestore
                    .collection('participants')
                    .where('Connect_ID', '==', parseInt(Connect_ID))
                    .select(  
                        'Connect_ID',  
                        fieldMapping.physicalActivity
                    )
                    .get();

                if (snapshot.empty) {
                    console.error(`Firestore doc not found for Connect_ID: ${Connect_ID}`);
                    return;
                }

                const doc = snapshot.docs[0];
                const data = doc.data();
                const reportStatus = data[fieldMapping.physicalActivity]?.[fieldMapping.physicalActivityStatus];

                if (reportStatus == null) {
                    batch.update(doc.ref, { [`${fieldMapping.physicalActivity}.${fieldMapping.physicalActivityStatus}`]: fieldMapping.reportStatus.unread });
                    
                    updates += 1;
                    console.log(`Updating ${Connect_ID} 686238347.446235715 = 702641611`);
                }
            });

            const results = await Promise.allSettled(promises);

            for (const r of results) {
                if (r.status === 'rejected') {
                    console.error('Error while processing a row:', r.reason);
                }
            }

            if (updates > 0) {
                await batch.commit();
            }
        }

        return res.status(200);
    } catch (error) {
        console.error(error);
    
        return res.status(500).json({ message: error.toString(), error });
    }
}

module.exports = {
    physicalActivity
}