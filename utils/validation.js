const { getResponseJSON, setHeaders, logIPAddress, getSecret } = require('./shared');
const conceptIds = require('./fieldToConceptIdMapping');


const generateToken = async (req, res, uid) => {

    if(req.method !== 'GET') {
        return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
    }

    const { participantExists } = require('./firestore')
    const userAlreadyExists = await participantExists(uid);

    if(userAlreadyExists){
        return res.status(401).json(getResponseJSON('Account already exists', 401));
    }
    const { v4: uuid } = require('uuid');
    const { defaultFlags, defaultStateFlags } = require('./shared');
    const obj = {
        state: { 
            uid, 
            ...defaultStateFlags
        },
        230663853: 353358909,
        token: uuid(),
        512820379: 854703046, // defaulting it as passive
        471593703: (new Date()).toISOString(),
        ...defaultFlags
    }
    console.log('Token of new record:', obj.token);
    const { createRecord } = require('./firestore');
    createRecord(obj);
    return res.status(200).json(getResponseJSON('Ok', 200));
}

const validateToken = async (req, res, uid) => {

    if(req.method !== 'GET') {
        return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
    }

    console.log('uid:', uid, ' req.query:', JSON.stringify(req.query));
    const { participantExists } = require('./firestore')
    const userAlreadyExists = await participantExists(uid);

    if(userAlreadyExists){
        return res.status(401).json(getResponseJSON('Account already exists', 401));
    }

    const pin = req.query.pin?.trim();
    const token = req.query.token?.trim();
    if (!pin && !token) {
        return res.status(400).json(getResponseJSON('Bad request: token/pin required', 400));
    }

    if (token) {
      const {
        verifyTokenOrPin,
        linkParticipanttoFirebaseUID,
      } = require('./firestore');

      const {
        isDuplicateAccount,
        isValid: isValidToken,
        docId,
      } = await verifyTokenOrPin({ token });

      if (isDuplicateAccount) {
        return res.status(202).json(getResponseJSON('Duplicate account', 202));
      }

      if (isValidToken) {
        await linkParticipanttoFirebaseUID(docId, uid);
        return res.status(200).json(getResponseJSON('Ok', 200));
      }

      return res.status(401).json(getResponseJSON('Invalid token', 401));
    }

    if (pin) {
      const {
        verifyTokenOrPin,
        linkParticipanttoFirebaseUID,
        updateResponse,
      } = require('./firestore');

      const {
        isDuplicateAccount,
        isValid: isValidPin,
        docId,
      } = await verifyTokenOrPin({ pin });

      if (isDuplicateAccount) {
        return res.status(202).json(getResponseJSON('Duplicate account', 202));
      }

      if (isValidPin) {
        await linkParticipanttoFirebaseUID(docId, uid);
        await updateResponse({ [conceptIds.pinMatch]: conceptIds.yes }, uid);
        return res.status(200).json(getResponseJSON('Ok', 200));
      }

      return res.status(401).json(getResponseJSON('Invalid pin', 401));
    }
};

const getToken = async (req, res) => {
    logIPAddress(req);
    setHeaders(res);
    if(req.method === 'OPTIONS') return res.status(200).json({code: 200});

    if(req.method !== 'POST') {
        return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
    }

    const { APIAuthorization } = require('./shared');
    const authorized = await APIAuthorization(req);
    if(authorized instanceof Error){
        return res.status(500).json(getResponseJSON(authorized.message, 500));
    }

    if(!authorized){
        return res.status(401).json(getResponseJSON('Authorization failed!', 401));
    }

    const siteCode = authorized.siteCode;

    if(req.body.data === undefined) return res.status(400).json(getResponseJSON('Bad request!', 400));
    if(req.body.data.length > 0){
        const { v4: uuid } = require('uuid');
        let responseArray = [];
        if(req.body.data.length > 999) return res.status(400).json(getResponseJSON('Bad request!', 400));
        const data = req.body.data;
        console.log('Data in request body:', data);
        for(let dt in data){
            if(data[dt].studyId && data[dt].studyId.trim() !== ""){
                const studyId = data[dt].studyId
                const { recordExists } = require('./firestore');
                console.log('studyId', studyId);
                console.log('siteCode', siteCode);
                const response = await recordExists(studyId, siteCode);

                if(response === false){
                    const { randomString } = require('./shared');
                    
                    let boo = false;
                    let PIN;
                    while(boo === false){
                        const tempPIN = `${randomString()}`
                        const { sanityCheckPIN } = require('./firestore');
                        const response = await sanityCheckPIN(tempPIN);
                        if(response === true) {
                            PIN = tempPIN;
                            boo = true;
                        }
                    }
                    const { defaultFlags, defaultStateFlags } = require('./shared');
                    const obj = {
                        state: { studyId, 
                            ...defaultStateFlags,
                            521025370: new Date().toISOString()
                        },
                        827220437: siteCode,
                        512820379: 180583933, // default recruit type not-active
                        230663853: 104430631,
                        pin: PIN,
                        token: uuid(),
                        ...defaultFlags
                    }
                    const { createRecord } = require('./firestore');
                    await createRecord(obj);
                    responseArray.push({studyId: studyId, token: obj.token, pin: obj.pin});
                } else {
                    response.pin ? responseArray.push({studyId: studyId, token: response.token, pin: response.pin}) : responseArray.push({studyId: studyId, token: response.token});
                }
            } else {
                // Return error?
            }
        }
        return res.status(200).json({data: responseArray, code: 200});
    }
    else {
        return res.status(400).json(getResponseJSON('Bad request!', 400));
    }
}

const processMouthwashEligibility = (data) => {
    // Conditions for initialized: baselineMouthwashSample is no, bloodOrUrineCollected is yes, 
    // kitStatus does not yet have a value, processParticipantHomeMouthwashKitData passes
    const updates = {};
    const {processParticipantHomeMouthwashKitData} = require('./firestore');
    if(
        data[conceptIds.withdrawConsent] == conceptIds.no &&
        data[conceptIds.participantDeceasedNORC] == conceptIds.no &&
        data[conceptIds.activityParticipantRefusal] && data[conceptIds.activityParticipantRefusal][conceptIds.baselineMouthwashSample] == conceptIds.no &&
        data[conceptIds.collectionDetails] && data[conceptIds.collectionDetails][conceptIds.baseline] &&
        data[conceptIds.collectionDetails][conceptIds.baseline][conceptIds.bloodOrUrineCollected] == conceptIds.yes &&
        data[conceptIds.collectionDetails][conceptIds.baseline][conceptIds.bloodOrUrineCollectedTimestamp] >= '2024-04-01T00:00:00.000Z' &&
        (
            !data[conceptIds.collectionDetails][conceptIds.baseline][conceptIds.bioKitMouthwash] ||
            data[conceptIds.collectionDetails][conceptIds.baseline][conceptIds.bioKitMouthwash][conceptIds.kitStatus] !== conceptIds.initialized
        )
    ) {
        const isEligible = !!processParticipantHomeMouthwashKitData(data, true);
        if(isEligible) {
            updates[`${conceptIds.collectionDetails}.${conceptIds.baseline}.${conceptIds.bioKitMouthwash}.${conceptIds.kitStatus}`] = conceptIds.initialized;
        }
    } else if(
        data[conceptIds.collectionDetails] &&
        data[conceptIds.collectionDetails][conceptIds.baseline] &&
        data[conceptIds.collectionDetails][conceptIds.baseline][conceptIds.bioKitMouthwash] &&
        data[conceptIds.collectionDetails][conceptIds.baseline][conceptIds.bioKitMouthwash][conceptIds.kitStatus] == conceptIds.initialized
    ) {
        // Conditions to remove initialized: status is initialized and processParticipantHomeMouthwashKitData fails
        const isEligible = !!processParticipantHomeMouthwashKitData(data, true);
        if(!isEligible) {
            updates[`${conceptIds.collectionDetails}.${conceptIds.baseline}.${conceptIds.bioKitMouthwash}.${conceptIds.kitStatus}`] = undefined;
        }
    }
    return updates;
}

const checkDerivedVariables = async (token, siteCode) => {
    
    const { getParticipantData, getSpecimenCollections, retrieveUserSurveys } = require('./firestore');

    const response = await getParticipantData(token, siteCode);
    if(!response) {
        return;
    }
    const specimenArray = await getSpecimenCollections(token, siteCode);

    
    
    const data = response.data;
    const doc = response.id;

    const uid = data.state.uid

    if(!uid) return;

    const surveys = await retrieveUserSurveys(uid, ["D_299215535", "D_826163434"]);

    let updates = {};

    let incentiveEligible = false;
    let menstrualCycleSurveyEligible = false;
    let allBaselineComplete = false;
    let bloodUrineNotRefused = false;
    let calculateBaselineOrderPlaced = false;
    let clinicalSampleDonated = false;
    let anyRefusalWithdrawal = false;

    // incentiveEligible
    if(data['130371375']['266600170']['731498909'] === 104430631) {

        const module1 = (data['949302066'] === 231311385);
        const module2 = (data['536735468'] === 231311385);
        const module3 = (data['976570371'] === 231311385);
        const module4 = (data['663265240'] === 231311385);
        const bloodCollected = (data['878865966'] === 353358909) || (data['173836415']?.['266600170']?.['693370086'] === 353358909);    
    
        if(module1 && module2 && module3 && module4) {
            if(bloodCollected) {
                incentiveEligible = true;
            }    
            else {
                if (specimenArray.length > 0) {
                    const baselineResearchCollections = specimenArray.filter(collection => collection['331584571'] === 266600170 && collection['650516960'] === 534621077);
                
                    if(baselineResearchCollections.length != 0) {
                        baselineResearchCollections.forEach(collection => {
                            
                            const researchBloodTubes = ['299553921', '703954371', '838567176', '454453939', '652357376', '505347689'];
                            
                            researchBloodTubes.forEach(tube => {
                                if(collection[tube] && collection[tube][883732523] && collection[tube][883732523] != 681745422) {
                                    incentiveEligible = true;
                                }
                            });
                        });
                    }
                }
            }
        }
    }

    // menstrualCycleSurveyEligible
    if(data['289750687'] != 353358909) {
        if(data['265193023'] === 231311385) {
            menstrualCycleSurveyEligible = (surveys['D_299215535']?.['D_112151599'] == 353358909);
        }
        else if(data['253883960'] === 231311385) {
            menstrualCycleSurveyEligible = (surveys['D_826163434']?.['D_112151599'] == 353358909);
        }
    }

    // allBaselineComplete
    if(data['100767870'] === 104430631) {
        if (data['949302066'] === 231311385 && data['536735468'] === 231311385 && data['976570371'] === 231311385 && data['663265240'] === 231311385) {
            allBaselineComplete = true;
        }
    }

    // bloodUrineNotRefused
    if(data['526455436']) {
        if(data['526455436'] === 104430631) {
            bloodUrineNotRefused = (data['685002411']['194410742'] === 353358909 && data['685002411']['949501163'] === 353358909);
        }
    }
    else {
        bloodUrineNotRefused = true;
    }

    // calculateBaselineOrderPlaced
    if(data['173836415']?.['266600170']) {

        // calculateBaselineOrderPlaced
        if(data['173836415']['266600170']['880794013']) {
            if(data['173836415']['266600170']['880794013'] === 104430631) {
                calculateBaselineOrderPlaced = (data['173836415']['266600170']['530173840'] === 353358909 || data['173836415']['266600170']['860477844'] === 353358909);
            }
            else if(data['173836415']['266600170']['880794013'] === 353358909) {
                
                let scenario1 = (data['173836415']['266600170']['530173840'] === 104430631 && data['173836415']['266600170']['860477844'] === 104430631);
                let scenario2 = (data['173836415']['266600170']['530173840'] === 104430631 && typeof data['173836415']['266600170']['860477844'] === 'undefined');
                let scenario3 = (typeof data['173836415']['266600170']['530173840'] === 'undefined' && data['173836415']['266600170']['860477844'] === 104430631);
                
                calculateBaselineOrderPlaced = scenario1 || scenario2 || scenario3;
            }
        }
        else {
            calculateBaselineOrderPlaced = true;
        }

        // clinicalSampleDonated
        if(data['173836415']['266600170']['156605577']) {
            if(data['173836415']['266600170']['156605577'] === 104430631) {
                clinicalSampleDonated = checkSamplesDonated(data);
            }
        }
        else {
            clinicalSampleDonated = true;
        }
    }

    // anyRefusalWithdrawal
    if(typeof data['451953807'] === 'undefined') {
        anyRefusalWithdrawal = true;
    }
    else if(data['451953807'] === 104430631) {
        anyRefusalWithdrawal = checkRefusalWithdrawals(data);
    }

    if(incentiveEligible) {

        const incentiveUpdates = {
            '130371375.266600170.731498909': 353358909,
            '130371375.266600170.222373868': 353358909,
            '130371375.266600170.787567527': new Date().toISOString()
        };

        updates = { ...updates, ...incentiveUpdates};
    } 

    if(menstrualCycleSurveyEligible) {
        
        const menstrualUpdates = {
            '289750687': 353358909
        }

        updates = { ...updates, ...menstrualUpdates};
    }

    if(allBaselineComplete) {
        
        const baselineUpdates = {
            '100767870': 353358909
        }

        updates = { ...updates, ...baselineUpdates};
    }

    if(bloodUrineNotRefused) {

        const bloodRefused = data['685002411']['194410742'] === 353358909;
        const urineRefused = data['685002411']['949501163'] === 353358909;
        
        const refusalUpdates = {
            '526455436': (bloodRefused && urineRefused) ? 353358909 : 104430631
        }

        updates = { ...updates, ...refusalUpdates};
    }

    if(calculateBaselineOrderPlaced) {

        const bloodOrder = data['173836415']['266600170']['530173840'] === 353358909;
        const urineOrder = data['173836415']['266600170']['860477844'] === 353358909;

        const bloodTime = data['173836415']['266600170']['769615780'];
        const urineTime = data['173836415']['266600170']['939818935'];
        let earliestTime = false;

        if(bloodTime) {
            if(urineTime) {
                if(bloodTime < urineTime) {
                    earliestTime = bloodTime;
                }
                else {
                    earliestTime = urineTime;
                }
            }
            else {
                earliestTime = bloodTime;
            }
        }
        else {
            if(urineTime) {
                earliestTime = urineTime;
            }
        }

        let orderUpdates = {};

        if(earliestTime) {
            orderUpdates = {
                '173836415.266600170.880794013': bloodOrder || urineOrder ? 353358909 : 104430631,
                '173836415.266600170.184451682': earliestTime
            }
        }
        else {
            orderUpdates = {
                '173836415.266600170.880794013': bloodOrder || urineOrder ? 353358909 : 104430631
            }
        }

        updates = { ...updates, ...orderUpdates};
    }

    if(clinicalSampleDonated) {

        const sampleUpdates = {
            '173836415.266600170.156605577': checkSamplesDonated(data) ? 353358909 : 104430631
        }

        updates = { ...updates, ...sampleUpdates};
    }

    if(anyRefusalWithdrawal) {

        const refusalUpdates = {
            '451953807': checkRefusalWithdrawals(data) ? 353358909 : 104430631
        }

        updates = { ...updates, ...refusalUpdates};
    }

    if(Object.keys(updates).length > 0) {
        const { updateParticipantData } = require('./firestore');
        await updateParticipantData(doc, updates);
    }
}

const checkSamplesDonated = (data) => {
    
    const samplesDonated = (
        data['173836415']['266600170']['693370086'] === 353358909 || 
        data['173836415']['266600170']['786930107'] === 353358909 ||
        data['173836415']['266600170']['728696253'] === 353358909 ||
        data['173836415']['266600170']['453452655'] === 353358909 ||
        data['173836415']['266600170']['534041351'] === 353358909 ||
        data['173836415']['266600170']['210921343'] === 353358909
    ); 

    return samplesDonated;
}

const checkRefusalWithdrawals = (data) => {
    const anyRefusalWithdrawal = (
        data['685002411']['194410742'] === 353358909 ||
        data['685002411']['217367618'] === 353358909 ||
        data['685002411']['277479354'] === 353358909 ||
        data['685002411']['352996056'] === 353358909 ||
        data['685002411']['867203506'] === 353358909 ||
        data['685002411']['949501163'] === 353358909 ||
        data['685002411']['994064239'] === 353358909 ||
        data['685002411']['101763809'] === 353358909 ||
        data['685002411']['525277409'] === 353358909 ||
        data['685002411']['936015433'] === 353358909 ||
        data['685002411']['688142378'] === 353358909 ||
        data['685002411']['671903816'] === 353358909 ||
        data['726389747'] === 353358909 ||
        data['747006172'] === 353358909 ||
        data['773707518'] === 353358909 ||
        data['831041022'] === 353358909 ||
        data['906417725'] === 353358909 ||
        data['987563196'] === 353358909
    ); 

    return anyRefusalWithdrawal;
};

const validateUsersEmailPhone = async (req, res) => {
    logIPAddress(req);
    setHeaders(res);
    if(req.method !== 'GET') {
        return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
    }
    if(!req.query) return res.status(404).json(getResponseJSON('Not valid', 404));
    const { verifyUsersEmailOrPhone } = require('./firestore');
    let result = await verifyUsersEmailOrPhone(req)
    if (result) return res.status(200).json({data: {accountExists: true}, code: 200})
    else return res.status(200).json({data: {accountExists: false}, code: 200})
}

const emailAddressValidation = async (req, res) => {
    logIPAddress(req);
    setHeaders(res);

    if (req.method !== "POST") {
        return res
            .status(405)
            .json(getResponseJSON("Only POST requests are accepted!", 405));
    }

    if (!req.body) {
        return res.status(400).json(getResponseJSON("Bad Request", 400));
    }
    
    const secret = await getSecret(process.env.GCLOUD_SENDGRID_EMAIL_VALIDATION_SECRET);
    const sgClient = require('@sendgrid/client');
    sgClient.setApiKey(secret);

    const { emails } = JSON.parse(req.body);
    const result = {};
    try {
        // Prepare the list of validation requests
        const validationPromises = Object.keys(emails).map(async (key) => {
            const email = emails[key];
            if (!email) return { key, result: null }; // Skip if email is empty

            const [_, body] = await sgClient.request({
                url: "/v3/validations/email",
                method: "POST",
                body: { email },
            });
            return { key, result: body.result };
        });

        // Await all validation promises
        const validationResults = await Promise.all(validationPromises);

        // Populate the result object with the outcomes
        validationResults.forEach(({ key, result: validationResult }) => {
            if (validationResult) result[key] = validationResult;
        });

        return res.status(200).json(result);
    } catch (error) {
        console.error("Unexpected error:", error);
        return res.status(500).json({ error: "Internal Server Error" });
    }
};

const updateParticipantFirebaseAuthentication = async (req, res) => {
    if(req.method !== 'POST') {
        return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
    }
    const data = req.body.data;
    const flag = data.flag;
    const uid =  data.uid;

    if(data === undefined) {
        return res.status(400).json(getResponseJSON('Bad request. Data is not defined in request body.', 400));
    }

    let status = '';
    const { updateUserPhoneSigninMethod, updateUserEmailSigninMethod, updateUsersCurrentLogin } = require('./firestore');
    
    if (flag === 'replaceSignin' && data['phone']) status = await updateUserPhoneSigninMethod(data.phone, uid);
    else if (flag === 'replaceSignin' && data['email']) status = await updateUserEmailSigninMethod(data.email, uid);
    else return res.status(403).json(getResponseJSON('Invalid Request. Phone or email data not defined in request.', 403));
    
    if (flag === `updateEmail` || flag === `updatePhone`) status = await updateUsersCurrentLogin(data, uid);

    if (status === true) return res.status(200).json({code: 200});
    else if (status === `auth/phone-number-already-exists`) return res.status(409).json(getResponseJSON('The user with provided phone number already exists.', 409));
    else if (status === `auth/email-already-exists`) return res.status(409).json(getResponseJSON('The user with the provided email already exists.', 409));
    else if (status === `auth/invalid-phone-number`) return res.status(403).json(getResponseJSON('Invalid Phone number', 403));
    else if (status === `auth/invalid-email`) return res.status(403).json(getResponseJSON('Invalid Email', 403));
    else return res.status(400).json(getResponseJSON('Operation Unsuccessful', 400));
}

const isIsoDate = (str) => {
    if (!/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z/.test(str)) return false;
    const d = new Date(str);
    return d instanceof Date && !isNaN(d) && d.toISOString() === str; // valid date 
}

/**
 * Validate an ISO 8601 timestamp. Use capturing groups to extract year, month, day, hour, minute, and second.
 * Millisecond validation (range 000-999) is handled by the regex.
 * Ensure all values are within the acceptable range for each field.
 * @param {string} timestamp - the ISO 8601 timestamp to validate.
 * @returns {object} - an object with an error flag and message.
 */

const iso8601Regex = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\.(\d{3})Z$/;

const validateIso8601Timestamp = (timestamp) => {
    const match = typeof timestamp === 'string' && timestamp.match(iso8601Regex);
    if (!match) {
        return { error: true, message: "Invalid ISO 8601 format. ISO 8601 string required. Example: '2023-12-15T12:45:52.123Z'" };
    }

    const [, yearStr, monthStr, dayStr, hourStr, minuteStr, secondStr] = match;
    const year = parseInt(yearStr, 10);
    const month = parseInt(monthStr, 10);
    const day = parseInt(dayStr, 10);
    const hour = parseInt(hourStr, 10);
    const minute = parseInt(minuteStr, 10);
    const second = parseInt(secondStr, 10);

    const currentYear = new Date().getUTCFullYear();

    if (year < 1900 || year > currentYear) {
        return { error: true, message: `Year must be between 1900 and ${currentYear}` };
    }
    if (month < 1 || month > 12) {
        return { error: true, message: "Month must be between 1 and 12" };
    }

    const monthLengths = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if (year % 400 === 0 || (year % 100 !== 0 && year % 4 === 0)) {
        monthLengths[1] = 29;
    }

    if (day < 1 || day > monthLengths[month - 1]) {
        return { error: true, message: `Day must be between 1 and ${monthLengths[month - 1]} for ${monthStr}/${yearStr}` };
    }

    if (hour < 0 || hour > 23) {
        return { error: true, message: "Hour must be between 0 and 23" };
    }

    if (minute < 0 || minute > 59) {
        return { error: true, message: "Minute must be between 0 and 59" };
    }

    if (second < 0 || second > 59) {
        return { error: true, message: "Second must be between 0 and 59" };
    }

    // success case
    return { error: false, message: "" };
};

module.exports = {
    generateToken,
    validateToken,
    getToken,
    processMouthwashEligibility,
    checkDerivedVariables,
    validateUsersEmailPhone,
    emailAddressValidation,
    updateParticipantFirebaseAuthentication,
    isIsoDate,
    validateIso8601Timestamp,
}
