const { getResponseJSON, setHeaders, logIPAdddress } = require('./shared');
const { validateSiteUser } = require('./firestore');

const dashboard = async (req, res) => {
    logIPAdddress(req);
    setHeaders(res);
    if(req.method === 'OPTIONS') return res.status(200).json({code: 200});
    if(!req.headers.authorization || req.headers.authorization.trim() === ""){
        return res.status(401).json(getResponseJSON('Authorization failed!', 401));
    }
    if (!req.query.api) {
      return res.status(400).json(getResponseJSON('Bad request!', 400));
    }

    const access_token = req.headers.authorization.replace('Bearer ','').trim();
    let siteDetails = '';
    let userEmail = '';

    const { SSOValidation, decodingJWT } = require('./shared');
    let dashboardType = 'siteManagerUser';
    if(access_token.includes('.')) {
        const decodedJWT = decodingJWT(access_token);
        dashboardType = ['saml.connect-norc', 'saml.connect-norc-prod'].includes(decodedJWT.firebase.sign_in_provider) ? 'helpDeskUser' : 'siteManagerUser';
    }
    const SSOObject = await SSOValidation(dashboardType, access_token);
    const isSsoAuthSuccess = SSOObject !== false && !!SSOObject.siteDetails;

    if (isSsoAuthSuccess) {
        userEmail = SSOObject.email;
        siteDetails = SSOObject.siteDetails;
    } else { // Check siteKey if SSO fails
        const { APIAuthorization } = require('./shared');
        const isApiAuthSuccess = await APIAuthorization(req);

        if (isApiAuthSuccess instanceof Error) {
          return res.status(500).json(getResponseJSON(isApiAuthSuccess.message, 500));
        }
        // Unauthorized if both SSO and siteKey auth fail
        if (!isApiAuthSuccess) {
          return res.status(401).json(getResponseJSON('Authorization failed!', 401));
        }
        siteDetails = await validateSiteUser(access_token);
    }

    // Auth success, and proceed below steps:
    const { isParentEntity } = require('./shared');
    const authObj = await isParentEntity(siteDetails);
    if(userEmail) authObj['userEmail'] = userEmail;
    const isParent = authObj.isParent;
    const siteCodes = authObj.siteCodes;
    const isCoordinatingCenter = authObj.coordinatingCenter;
    const isHelpDesk = authObj.helpDesk;
    const api = req.query.api;
    console.log(`SMDB API: ${api}, accessed by: ${userEmail}`);

    if(api === 'validateSiteUsers') {
        const { validateSiteUsers } = require('./validation');
        return await validateSiteUsers(req, res, authObj);
    }
    else if (api === 'getParticipants') {
        const { getParticipants } = require('./submission');
        return await getParticipants(req, res, authObj);
    }
    else if (api === 'identifyParticipant' && isParent === false) {
        const { identifyParticipant } = require('./submission');
        return await identifyParticipant(req, res, siteCodes);
    }
    else if (api === 'submitParticipantsData') {
        const { submitParticipantsData } = require('./sites');
        return await submitParticipantsData(req, res, siteCodes);
    }
    else if (api === 'updateParticipantData') {
        const { updateParticipantData } = require('./sites');
        return await updateParticipantData(req, res, authObj);
    }
    else if (api === 'updateParticipantDataNotSite') {
        if(req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }   
        const {submit} = require('./submission');
        let body = req.body;
        if(!body.uid) {
            return res.status(500).json(getResponseJSON('Missing UID!', 405));
        }
        let uid = body.uid;
        delete body['uid']
        return submit(res, body, uid)
    }
    else if (api === 'updateUserAuthentication') {
        const { updateUserAuthentication } = require('./sites');
        return await updateUserAuthentication(req, res, authObj);
    }
    else if (api === 'stats') {
        const { stats } = require('./stats');
        return await stats(req, res, authObj);
    }
    else if (api === 'getParticipantNotification') {
        const { getParticipantNotification } = require('./notifications');
        return await getParticipantNotification(req, res, authObj);
    }
    else if (api === 'storeNotificationSchema' && isParent && isCoordinatingCenter) {
        const { storeNotificationSchema } = require('./notifications');
        return await storeNotificationSchema(req, res, authObj);
    }
    else if (api === 'retrieveNotificationSchema' && isParent && isCoordinatingCenter) {
        const { retrieveNotificationSchema } = require('./notifications');
        return await retrieveNotificationSchema(req, res, authObj);
    }
    else if (api === 'getSiteNotification' && isHelpDesk === false) { // Everyone except HelpDesk
        const { getSiteNotification } = require('./notifications');
        return await getSiteNotification(req, res, authObj);
    }
    else return res.status(404).json(getResponseJSON('API not found!', 404));
}

module.exports = {
    dashboard
}