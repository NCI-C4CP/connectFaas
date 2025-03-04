const { getResponseJSON, setHeaders, logIPAddress } = require('./shared');

const dashboard = async (req, res) => {
    logIPAddress(req);
    setHeaders(res);
    if (req.method === 'OPTIONS') return res.status(200).json({code: 200});
    if (!req.headers.authorization || req.headers.authorization.trim() === "") {
        return res.status(401).json(getResponseJSON('Authorization failed!', 401));
    }
    if (!req.query.api) {
      return res.status(400).json(getResponseJSON('Bad request!', 400));
    }

    const accessToken = req.headers.authorization.replace('Bearer ','').trim();

    const { SSOValidation, decodingJWT } = require('./shared');
    let dashboardType = 'siteManagerUser';
    if (accessToken.includes('.')) {
        const decodedJWT = decodingJWT(accessToken);
        dashboardType = ['saml.connect-norc', 'saml.connect-norc-prod'].includes(decodedJWT.firebase.sign_in_provider) ? 'helpDeskUser' : 'siteManagerUser';
    }
    const SSOObject = await SSOValidation(dashboardType, accessToken);

    if (!SSOObject) {
        return res.status(401).json(getResponseJSON('Authorization failed!', 401));
    }

    let userEmail = SSOObject.email;
    let siteDetails = SSOObject.siteDetails;

    const { isParentEntity } = require('./shared');
    const authObj = await isParentEntity(siteDetails);
    if (userEmail) authObj['userEmail'] = userEmail;
    const isParent = authObj.isParent;
    const siteCodes = authObj.siteCodes;
    const isCoordinatingCenter = authObj.coordinatingCenter;
    const isHelpDesk = authObj.helpDesk;
    const api = req.query.api;
    console.log(`SMDB API: ${api}, accessed by: ${userEmail}`);

    if (api === 'validateSiteUsers') {
        if (req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }
        return res.status(200).json({message: 'Ok', code: 200, isParent, coordinatingCenter: isCoordinatingCenter, helpDesk: isHelpDesk});
    } else if (api === 'getParticipants') {
        const { getParticipants } = require('./submission');
        return await getParticipants(req, res, authObj);
    } else if (api === 'retrievePhysicalActivityReport') {
        const { retrievePhysicalActivityReport } = require('./reports');
        let uid = req.query.uid;
        return await retrievePhysicalActivityReport(req, res, uid);
    } else if (api === 'getFilteredParticipants') {
        if (req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }
        
        // req.query includes 'api' key plus query params from the participant search form.
        if(Object.keys(req.query).length < 2) {
            return res.status(400).json(getResponseJSON('Please include at least one search parameter.', 400));
        }
        
        try {
            req.query.source = 'dashboard';
            const { getFilteredParticipants } = require('./submission');
            return await getFilteredParticipants(req, res, authObj);
        } catch (error) {
            console.error('Error in getFilteredParticipants.', error);
            return res.status(500).json(getResponseJSON('An error occurred while searching for this participant. Please try again later.', 500));
        }
    } else if (api === 'identifyParticipant' && isParent === false) {
        const { identifyParticipant } = require('./submission');
        return await identifyParticipant(req, res, siteCodes);
    } else if (api === 'submitParticipantsData') {
        const { submitParticipantsData } = require('./sites');
        return await submitParticipantsData(req, res, siteCodes);
    } else if (api === 'updateParticipantData') {
        const { updateParticipantData } = require('./sites');
        return await updateParticipantData(req, res, authObj);
    } else if (api === 'updateParticipantDataNotSite') {
        if (req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }   
        const {submit} = require('./submission');
        let body = req.body;
        if (!body.uid) {
            return res.status(405).json(getResponseJSON('Missing UID!', 405));
        }
        let uid = body.uid;
        delete body['uid'];
        return submit(res, body, uid);
    } else if (api === 'updateUserAuthentication') {
        const { updateUserAuthentication } = require('./sites');
        return await updateUserAuthentication(req, res, authObj);
    } else if (api === 'stats') {
        const { stats } = require('./stats');
        return await stats(req, res, authObj);
    } else if (api === 'getStatsForDashboard') {
        const { getStatsForDashboard } = require('./stats');
        return await getStatsForDashboard(req, res, authObj);
    } else if (api === 'getParticipantNotification') {
        const { getParticipantNotification } = require('./notifications');
        return await getParticipantNotification(req, res, authObj);
    } else if (api === 'storeNotificationSchema' && isParent && isCoordinatingCenter) {
        const { storeNotificationSchema } = require('./notifications');
        return await storeNotificationSchema(req, res, authObj);
    } else if (api === 'retrieveNotificationSchema' && isParent && isCoordinatingCenter) {
        const { retrieveNotificationSchema } = require('./notifications');
        return await retrieveNotificationSchema(req, res, authObj);
    } else if (api === 'getSiteNotification' && isHelpDesk === false) { // Everyone except HelpDesk
        const { getSiteNotification } = require('./notifications');
        return await getSiteNotification(req, res, authObj);
    } else if (api === 'participantDataCorrection') {
        const { participantDataCorrection } = require('./sites');
        return await participantDataCorrection(req, res);
    } else if (api === "dryRunNotificationSchema") {
        const { dryRunNotificationSchema } = require('./notifications');
        return await dryRunNotificationSchema(req, res);
    } else if (api === 'resetUser') {
        if (req.method !== 'POST') {
          return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }
        // Only permit for dev apps
        if (process.env.GCLOUD_PROJECT === 'nih-nci-dceg-connect-dev') {
          let body = req.body;
          if (!body.uid) {
              return res.status(405).json(getResponseJSON('Missing UID!', 405));
          }
          let uidToReset = body.uid;
          let saveToDb = body.saveToDb === 'true';
          const { resetParticipantHelper } = require('./firestore');
          try {
            const { data, deleted } = await resetParticipantHelper(uidToReset, saveToDb);
            if (!data) {
                return res.status(404).json(getResponseJSON('Participant not found', 404));
            }
            return res.status(200).json({data: {data, deleted}, code: 200});
          }
          catch(err) {
            console.error('error', err);
            return res.status(500).json({data: 'Error: ' + (err && err.toString ? err.toString() : err), code: 500});
          }
          
        }
        else {
          return res.status(403).json(getResponseJSON('Operation only permitted on dev environment', 403));
        }
    } else if (api === 'resetParticipantSurvey') {
        if (req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }
        let body = req.body;
        const { connectId, survey } = body;

        if (!connectId) return res.status(405).json(getResponseJSON('Missing participant\'s Connect ID!', 405));
        if (!body.survey) return res.status(405).json(getResponseJSON('Missing survey name to be reset!', 405));

        try {
            const { resetParticipantSurvey } = require('./firestore');            
            const data = await resetParticipantSurvey(connectId, survey);
            return res.status(200).json({data: data, message: 'The participant\'s survey was sucessfully reset', code: 200});
        } catch (err) {
            console.error('error', err);
            if (err.code) {
                return res.status(err.code).json(getResponseJSON(err.message, err.code));
            }
            return res.status(500).getResponseJSON.json(err.message, code);
        }
    } else if (api === `updateParticipantIncentiveEligibility`) {
        if (req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }
        let body = req.body;
        const { connectId, currentPaymentRound, dateOfEligibilityInput } = body;

        if (!connectId) return res.status(405).json(getResponseJSON('Missing participant\'s Connect ID!', 405));
        if (!currentPaymentRound) return res.status(405).json(getResponseJSON('Missing current payment round information!', 405));
        if (!dateOfEligibilityInput) return res.status(405).json(getResponseJSON('Missing date of eligibility!', 405));

        try {
            const { updateParticipantIncentiveEligibility } = require('./firestore');
            const data = await updateParticipantIncentiveEligibility(connectId, currentPaymentRound, dateOfEligibilityInput);
            return res.status(200).json({data: data, message:"Participant Eligibility Sucessfully Updated!" ,code: 200});
        } catch (err) {
            console.error('error', err);
            if (err.code) {
                return res.status(err.code).json(getResponseJSON(err.message, err.code));
            }
            return res.status(500).json(getResponseJSON(err.message, 500));
        }
    } else if (api === 'requestHomeMWReplacementKit') {
        // @TODO
        if (req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }
        const connectId = req.query.connect_id;

        if(!connectId) {
            return res.status(405).json(getResponseJSON('Missing connect ID!', 405));
        }
        const {requestHomeMWReplacementKit} = require('./firestore');
        try {
            await requestHomeMWReplacementKit(connectId);
            return res.status(200).json(getResponseJSON('Success!', 200));
        } catch(err) {
            return res.status(500).json(getResponseJSON(err && err.message ? err.message : err, 500));
        }
    } else {
        return res.status(404).json(getResponseJSON('API not found!', 404));
    }
};

module.exports = {
    dashboard
};
