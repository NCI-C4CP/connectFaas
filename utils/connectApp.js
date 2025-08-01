const { getResponseJSON, setHeadersDomainRestricted, getUserProfile, safeJSONParse } = require('./shared');
const { submit, submitSocial, getUserSurveys, getUserCollections } = require('./submission');
const { retrieveNotifications, sendEmailLink } = require('./notifications');
const { retrievePhysicalActivityReport } = require('./reports');
const { validatePin, createParticipantRecord, updateParticipantFirebaseAuthentication, validateUsersEmailPhone, emailAddressValidation, addressValidation, validateToken } = require('./validation');

const connectApp = async (req, res) => {
    setHeadersDomainRestricted(req, res);

    if(req.method === 'OPTIONS') return res.status(200).json({code: 200});

    if (req.query.api === 'sendEmailLink') return await sendEmailLink(req, res);

    if(!req.headers.authorization || req.headers.authorization.trim() === ""){
        return res.status(401).json(getResponseJSON('Authorization failed!', 401));
    }

    const idToken = req.headers.authorization.replace('Bearer','').trim();
    const { validateIDToken } = require('./firestore');
    const decodedToken = await validateIDToken(idToken);

    if(decodedToken instanceof Error){
        return res.status(401).json(getResponseJSON(decodedToken.message, 401));
    }

    if(!decodedToken){
        return res.status(401).json(getResponseJSON('Authorization failed!', 401));
    }

    const uid = decodedToken.uid;

    const query = req.query;
    
    if(!query.api) return res.status(400).json(getResponseJSON('Bad request!', 400));
    const api = query.api;

    console.log(`PWA API: ${api}, called from uid: ${uid}`);

  try {
    if (api === 'submit') {

      if (req.method !== 'POST') {
        return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
      }

      const body = req.body;

      if (!body || Object.keys(body).length === 0) {
        return res.status(400).json(getResponseJSON('Bad request!', 400));
      }

      // all 'submit' paths return res.status(code).json({ message, code });
      return await submit(res, body, uid);
    }

    else if (api === 'submitSocial') return await submitSocial(req, res, uid);

    else if (api === 'getUserProfile') return await getUserProfile(req, res, uid);

    else if (api === 'getUserSurveys') return await getUserSurveys(req, res, uid);

    else if (api === 'getUserCollections') {
      if (req.method !== 'GET') {
        return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
      }

      // all 'getUserCollections' paths return res.status(code).json({ message, code });
      return await getUserCollections(req, res, uid);
    }

    else if (api === 'retrieveNotifications') return await retrieveNotifications(req, res, uid);
    
    else if (api === 'validatePin') {
      if (req.method !== 'POST') {
        return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
      }

      const body = safeJSONParse(req.body);
      if (!body || Object.keys(body).length === 0) {
        return res.status(400).json(getResponseJSON('Bad request: empty submission', 400));
      }

      // all 'validatePin' paths return res.status(code).json({ message, code });
      return await validatePin(res, body, uid);
    }

    else if (api === 'validateToken') {
      if (req.method !== 'POST') {
        return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
      }

      const body = safeJSONParse(req.body);
      if (!body || Object.keys(body).length === 0) {
        return res.status(400).json(getResponseJSON('Bad request: empty submission', 400));
      }

      return validateToken(res, body, uid);
    }

    else if (api === 'createParticipantRecord') {
      if (req.method !== 'POST') {
        return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
      }

      const body = safeJSONParse(req.body);
      if (!body || Object.keys(body).length === 0) {
        return res.status(400).json(getResponseJSON('Bad request: empty submission', 400));
      }

      // all 'createParticipantRecord' paths return res.status(code).json({ message, code });
      return await createParticipantRecord(res, body, uid);
    }

    else if (api === 'updateParticipantFirebaseAuthentication') return await updateParticipantFirebaseAuthentication(req, res);

    else if (api === 'validateEmailOrPhone') return await validateUsersEmailPhone(req, res);

    else if (api === 'emailAddressValidation') return await emailAddressValidation(req, res);
    
    else if (api === 'addressValidation') return await addressValidation(req, res);

    else if (api === 'retrievePhysicalActivityReport') return await retrievePhysicalActivityReport(req, res, uid);

    else if (api === 'getModuleSHA') {
      if (req.method !== 'GET') {
        return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
      }

      if (!req.query.path) {
        return res.status(400).json(getResponseJSON('Path parameter is required!', 400));
      }

      const path = req.query.path;

      const { getModuleSHA } = require('./submission');
      const shaResult = await getModuleSHA(path);
      
      return res.status(200).json({data: shaResult, code: 200});
    }

    else if (api === 'getQuestSurveyFromGitHub') {
      if (req.method !== 'GET') {
        return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
      }

      const { sha, path } = req.query;

      if (!sha) {
        return res.status(400).json(getResponseJSON('Sha parameter is required!', 400));
      }

      if (!path) {
        return res.status(400).json(getResponseJSON('Path parameter is required!', 400));
      }

      const { getQuestSurveyFromGitHub } = require('./submission');
      const moduleTextAndVersionResult = await getQuestSurveyFromGitHub(sha, path);
      
      return res.status(200).json({data: moduleTextAndVersionResult, code: 200});
    }

    else if (api === 'getSHAFromGitHubCommitData') {
      if (req.method !== 'GET') {
        return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
      }

      if (!req.query.path) {
        return res.status(400).json(getResponseJSON('Path parameter is required!', 400));
      }

      const surveyStartTimestamp = req.query.surveyStartTimestamp || '';
      const path = req.query.path;

      const { getSHAFromGitHubCommitData } = require('./submission');
      const shaResult = await getSHAFromGitHubCommitData(surveyStartTimestamp, path);
      
      return res.status(200).json({data: shaResult, code: 200});
    }

    else if (api === 'syncDHQ3RespondentInfo') {
      if (req.method !== 'POST') { // NOTE: DHQ respondent info is fetched via POST request
        return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
      }

      const { studyID, respondentUsername, dhqSurveyStatus, dhqSurveyStatusExternal } = safeJSONParse(req.body);      
      if (!respondentUsername || !studyID || !dhqSurveyStatus) {
        return res.status(400).json(getResponseJSON('DHQ respondent username and study ID parameters are required!', 400));
      }

      const { syncDHQ3RespondentInfo } = require('./dhq');
      const respondentInfo = await syncDHQ3RespondentInfo(studyID, respondentUsername, dhqSurveyStatus, dhqSurveyStatusExternal, uid);

      return res.status(200).json({ data: respondentInfo, code: 200 });
    }

    else if (api === 'retrieveDHQHEIReport') {
      if (req.method !== 'POST') {
        return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
      }

      const dhqData = req.body;

      if (!dhqData || !dhqData.studyID || !dhqData.respondentUsername) {
        return res.status(400).json(getResponseJSON('Bad request: missing required body parameters studyID and/or respondentUsername', 400));
      }

      const { retrieveDHQHEIReport } = require('./dhq');
      const studyID = dhqData.studyID;
      const respondentUsername = dhqData.respondentUsername;

      const reportData = await retrieveDHQHEIReport(studyID, respondentUsername);

      return res.status(200).json({ data: reportData.data, code: 200 });
    }

    else if (api === 'updateDHQReportViewedStatus') {
      if (req.method !== 'POST') {
        return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
      }

      const { studyID, respondentUsername, isDeclined } = req.body;

      if (!studyID || !respondentUsername) {
        return res.status(400).json(getResponseJSON('Missing required body parameters: studyID, and/or respondentUsername.', 400));
      }

      const { updateDHQReportViewedStatus } = require('./dhq');
      await updateDHQReportViewedStatus(uid, studyID, respondentUsername, isDeclined);

      return res.status(200).json(getResponseJSON('DHQ report viewed status updated successfully.', 200));
    }

    else if (api === 'allocateDHQ3Credential') {
      if (req.method !== 'POST') {
        return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
      }

      const availableCredentialPools = safeJSONParse(req.body);
      if (!availableCredentialPools || !Array.isArray(availableCredentialPools) || availableCredentialPools.length === 0) {
        return res.status(400).json(getResponseJSON('Error: The availableCredentialPools array is required.', 400));
      }

      const { allocateDHQ3Credential } = require('./dhq');
      const allocationResult = await allocateDHQ3Credential(availableCredentialPools, uid);
      
      return res.status(200).json({ data: allocationResult, code: 200 });
    }

    else if (api === 'getAppSettings') {
      if (req.method !== 'GET') {
        return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
      }

      const selectedParamsArray = req.query.selectedParamsArray?.split(',');
      if (!selectedParamsArray || !Array.isArray(selectedParamsArray) || selectedParamsArray.length === 0) {
        return res.status(400).json(getResponseJSON("Error: selectedParamsArray is required. Please specify parameters to return.", 400));
      }

      const { getAppSettings } = require('./firestore');
      const appSettings = await getAppSettings('connectApp', selectedParamsArray);
      
      return res.status(200).json({data: appSettings, code: 200});
    }

    else return res.status(400).json(getResponseJSON('Bad request!', 400));

  } catch (error) {
    console.error(error);
    return res.status(500).json(getResponseJSON(error.message, 500));
  }
}

module.exports = {
    connectApp
}