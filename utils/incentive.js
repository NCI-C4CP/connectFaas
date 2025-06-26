const { setHeaders, getResponseJSON, logIPAddress, APIAuthorization, isParentEntity, isDateTimeFormat } = require('./shared');

const incentiveCompleted = async (req, res) => {
    logIPAddress(req);
    setHeaders(res);

    if(req.method === 'OPTIONS') return res.status(200).json({code: 200});
    if(req.method !== 'POST') return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));

    const authorized = await APIAuthorization(req);

    if(authorized instanceof Error) return res.status(500).json(getResponseJSON(authorized.message, 500));
    if(!authorized) return res.status(401).json(getResponseJSON('Authorization failed!', 401));

    console.log(`Incentives API: Incentive Completed, accessed by: ${authorized.saEmail}`);
    
    const {isParent, siteCodes} = await isParentEntity(authorized);

    let responseArray = [];
    let error = false;

    const { getParticipantData } = require('./firestore');
    const { incentiveConcepts } = require('./shared');

    const data = req.body.data;

    for(let i = 0; i < data.length; i++) {

        const { token, round, incentiveIssued, incentiveIssuedAt, incentiveRefused, incentiveRefusedAt, incentiveChosen, caseNumber } = data[i];
        
        if(token === undefined) {
            error = true;
            responseArray.push({'Invalid Request': {'Token': 'UNDEFINED', 'Errors': 'Token not defined in data object.'}});
            continue;
        }

        const participantExists = await getParticipantData(token, siteCodes, isParent);

        if(!participantExists) {
            error = true;
            responseArray.push({'Invalid Request': {'Token': token, 'Errors': 'Token does not exist.'}});
            continue;
        }

        if(round === undefined) {
            error = true;
            responseArray.push({'Invalid Request': {'Token': token, 'Errors': '"Round" not defined in data object.'}});
            continue;
        }

        if(round !== 'baseline' && round !== 'followup1' && round !== 'followup2' && round !== 'followup3') {
            error = true;
            responseArray.push({'Invalid Request': {'Token': token, 'Errors': 'Invalid value for "Round".'}});
            continue;
        }

        const roundConcept = incentiveConcepts[round];

        let incentiveUpdates = {};
        if(incentiveIssued || incentiveRefused) {

            if(incentiveIssued) {
                incentiveUpdates[`${roundConcept}.${incentiveConcepts['incentiveIssued']}`] = 353358909;

                if(incentiveIssuedAt) {
                    if(isDateTimeFormat(incentiveIssuedAt)){
                        incentiveUpdates[`${roundConcept}.${incentiveConcepts['incentiveIssuedAt']}`] = incentiveIssuedAt;
                    }
                    else {
                        error = true;
                        responseArray.push({'Invalid Request': {'Token': token, 'Errors': 'Invalid datetime format for "Incentive Issued At".'}});
                        continue;
                    }
                }
                else {
                    incentiveUpdates[`${roundConcept}.${incentiveConcepts['incentiveIssuedAt']}`] = new Date().toISOString();
                }

                if(incentiveChosen) {
                    incentiveUpdates[`${roundConcept}.${incentiveConcepts['incentiveChosen']}`] = incentiveChosen;
                }
                else {
                    error = true;
                    responseArray.push({'Invalid Request': {'Token': token, 'Errors': 'Data not provided for "Incentive Chosen".'}});
                    continue;
                }
            }
            else if(incentiveRefused) {
                incentiveUpdates[`${roundConcept}.${incentiveConcepts['incentiveRefused']}`] = 353358909;

                if(incentiveRefusedAt) {
                    if(isDateTimeFormat(incentiveRefusedAt)){
                        incentiveUpdates[`${roundConcept}.${incentiveConcepts['incentiveRefusedAt']}`] = incentiveRefusedAt;
                    }
                    else {
                        error = true;
                        responseArray.push({'Invalid Request': {'Token': token, 'Errors': 'Invalid datetime format for "Incentive Refused At".'}});
                        continue;
                    }
                }
                else {
                    incentiveUpdates[`${roundConcept}.${incentiveConcepts['incentiveRefusedAt']}`] = new Date().toISOString();
                }
            }

            if(caseNumber) {
                incentiveUpdates[`${roundConcept}.${incentiveConcepts['caseNumber']}`] = caseNumber;
            }

            const { updateParticipantRecord } = require(`./firestore`);
            let response = await updateParticipantRecord('token', token, siteCodes, isParent, incentiveUpdates);

            if(response instanceof Error) {
                error = true;
                responseArray.push({'Invalid Request': {'Token': token, 'Errors': 'There was a problem updating participant document.'}});
                continue;
            }

            responseArray.push({'Success': {'Token': token, 'Errors': 'None'}});
        }
    }

    return res.status(error ? 206 : 200).json({code: error ? 206 : 200, results: responseArray});
}

const eligibleForIncentive = async (req, res) => {
    logIPAddress(req);
    setHeaders(res);

    if(req.method === 'OPTIONS') return res.status(200).json({code: 200});
    if(req.method !== 'GET') return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));

    const authorized = await APIAuthorization(req);

    if(authorized instanceof Error) return res.status(500).json(getResponseJSON(authorized.message, 500));
    if(!authorized) return res.status(401).json(getResponseJSON('Authorization failed!', 401));

    console.log(`Incentives API: Get Eligible Participants, accessed by: ${authorized.saEmail}`);

    let { round, limit, page } = req.query;

    if(!round) return res.status(400).json(getResponseJSON('Round query parameter missing!', 400));
    if(round !== 'baseline' && round !== 'followup1' && round !== 'followup2' && round !== 'followup3') return res.status(400).json(getResponseJSON('Invalid round!', 400));
    if(limit && parseInt(limit) > 1000) return res.status(400).json(getResponseJSON('Bad request, the limit cannot exceed more than 1000 records!', 400));

    limit = limit ? parseInt(limit) : 100;
    page = page ? parseInt(page) : 1;

    const {isParent, siteCodes} = await isParentEntity(authorized);
    
    
    const { retrieveParticipantsEligibleForIncentives } = require('./firestore');
    const data = await retrieveParticipantsEligibleForIncentives(siteCodes, round, isParent, limit, page);

    if(data instanceof Error){
        return res.status(500).json(getResponseJSON(data.message, 500));
    }

    return res.status(200).json({data, code:200, limit, dataSize: data.length})
}

module.exports = {
    incentiveCompleted,
    eligibleForIncentive
}