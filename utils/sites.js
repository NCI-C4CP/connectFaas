const rules = require("../updateParticipantData.json");
const submitRules = require("../submitParticipantData.json");
const { getResponseJSON, setHeaders, logIPAddress, validPhoneFormat, validEmailFormat, refusalWithdrawalConcepts } = require('./shared');
const { validateIso8601Timestamp } = require('./validation');
const fieldMapping = require('./fieldToConceptIdMapping');

const submitParticipantsData = async (req, res, site) => {
    logIPAddress(req);
    setHeaders(res);

    if(req.method === 'OPTIONS') return res.status(200).json({code: 200});
        
    if(req.method !== 'POST') {
        return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
    }
    let siteCode = '';
    if(site) siteCode = site;
    else {
        const { APIAuthorization } = require('./shared');
        const authorized = await APIAuthorization(req);
        if(authorized instanceof Error){
            return res.status(500).json(getResponseJSON(authorized.message, 500));
        }
    
        if(!authorized){
            return res.status(401).json(getResponseJSON('Authorization failed!', 401));
        }

        siteCode = authorized.siteCode;
    }

    if(req.body.data === undefined) return res.status(400).json(getResponseJSON('Bad request. Data is not defined in request body.', 400));
    if(!Array.isArray(req.body.data)) return res.status(400).json(getResponseJSON('Bad request. Data must be an array.', 400));
    if(req.body.data.length === 0) return res.status(400).json(getResponseJSON('Bad request. Data array does not have any elements.', 400));
    if(req.body.data.length > 499) return res.status(400).json(getResponseJSON('Bad request. Data contains more than acceptable limit of 500 records.', 400));

    console.log(req.body.data);
    
    const dataArray = req.body.data;

    let responseArray = [];
    let error = false;

    for(let dataObj of dataArray){
        if(dataObj.token === undefined) {
            error = true;
            responseArray.push({'Invalid Request': {'Token': 'UNDEFINED', 'Errors': 'Token not defined in data object.'}});
            continue;
        }
        
        const participantToken = dataObj.token;
        delete dataObj.token;

        const { getParticipantData } = require('./firestore');
        const record = await getParticipantData(participantToken, siteCode);

        const flat = (obj, att, attribute) => {
            for(let k in obj) {
                if(typeof(obj[k]) === 'object') flat(obj[k], att, attribute ? `${attribute}.${k}`: k)
                else flattened[att][attribute ? `${attribute}.${k}`: k] = obj[k]
            }
        }

        if(!record) {
            error = true;
            responseArray.push({'Invalid Request': {'Token': participantToken, 'Errors': 'Token does not exist.'}});
            continue;
        }

        const docID = record.id;
        const docData = record.data;

        const dataHasBeenDestroyed =
            fieldMapping.participantMap.dataHasBeenDestroyed.toString();
        if (docData[dataHasBeenDestroyed] === fieldMapping.yes) {
            error = true;
            responseArray.push({'Invalid Request': {'Token': participantToken, 'Errors': 'Data Destroyed'}});
            continue;
        }

        let flattened = {
            docData: {}
        };

        flat(docData, 'docData');

        let errors = [];
        
        for(let key in dataObj) {

            if(submitRules[key] || submitRules['state.' + key]) {

                if(!submitRules[key]) {
                    let oldKey = key;
                    let newKey = 'state.' + key;

                    dataObj[newKey] = dataObj[oldKey];
                    delete dataObj[oldKey];

                    key = 'state.' + key;
                }

                if(flattened.docData[key]) {
                    errors.push(" Key (" + key + ") cannot exist before updating");
                    continue;
                }

                if(submitRules[key].dataType) {
                    if(submitRules[key].dataType == 'ISO') {
                        if(typeof dataObj[key] !== "string" || !(/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z/.test(dataObj[key]))) {
                            errors.push(" Invalid data type / format for Key (" + key + ")");
                        }
                    }
                    else {
                        if(submitRules[key].dataType !== typeof dataObj[key]) {
                            errors.push(" Invalid data type for Key (" + key + ")");
                        }
                        else {
                            if(submitRules[key].values) {
                                if(submitRules[key].values.filter(value => value.toString() === dataObj[key].toString()).length == 0) {
                                    errors.push(" Invalid value for Key (" + key + ")");
                                }
                            }
                        }
                    }
                }
            }
            else {
                errors.push(" Key (" + key + ") not found");
            }
        }

        if(errors.length !== 0) {
            error = true;
            responseArray.push({'Invalid Request': {'Token': participantToken, 'Errors': errors}});
            continue;
        }

        // TODO - "condition stacking" logic

        // If age deidentified data is provided and participant is not passive then make this participant Active
        if(dataObj['state.934298480'] && record.data['512820379'] !== 854703046) { 
            dataObj['512820379'] = 486306141;
            dataObj['471593703'] = new Date().toISOString();
        }

        // If Update recruit type is non-zero
        // Passive to Active
        if(dataObj['state.793822265'] && dataObj['state.793822265'] === 854903954 && record.data['512820379'] === 854703046) dataObj['512820379'] = 486306141;
        // Active to Passive
        if(dataObj['state.793822265'] && dataObj['state.793822265'] === 965707001 && record.data['512820379'] === 486306141) dataObj['512820379'] = 854703046;

        if(Object.keys(dataObj).length > 0) {

            console.log("SUBMITTED DATA");
            console.log(dataObj);

            const { updateParticipantData } = require('./firestore');
            await updateParticipantData(docID, dataObj);
        }

        responseArray.push({'Success': {'Token': participantToken, 'Errors': 'None'}});
    }

    return res.status(error ? 206 : 200).json({code: error ? 206 : 200, results: responseArray});
}

const siteNotificationsHandler = async (Connect_ID, concept, siteCode, obj) => {
    const { handleSiteNotifications } = require('./siteNotifications');
    const { getSiteEmail } = require('./firestore');
    const siteEmail = await getSiteEmail(siteCode);
    await handleSiteNotifications(Connect_ID, concept, siteEmail, obj.id, obj.acronym, siteCode);
}

const updateParticipantData = async (req, res, authObj) => {
    const { getParticipantData, updateParticipantData: updateParticipantDataFirestore, writeBirthdayCard, writeCancerOccurrences } = require('./firestore');
    const { checkForQueryFields, flattenObject, initializeTimestamps, userProfileHistoryKeys } = require('./shared');
    const { checkDerivedVariables } = require('./validation');

    logIPAddress(req);
    setHeaders(res);
    
    if(req.method === 'OPTIONS') return res.status(200).json({code: 200});

    if(req.method !== 'POST') {
        return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
    }
    let obj = {};
    let internalCall = false;
    if (authObj) {
        obj = authObj;
        internalCall = true;
    } else {
        const { APIAuthorization } = require('./shared');
        const authorized = await APIAuthorization(req);
        if(authorized instanceof Error){
            return res.status(500).json(getResponseJSON(authorized.message, 500));
        }

        if(!authorized){
            return res.status(401).json(getResponseJSON('Authorization failed!', 401));
        }
    
        const { isParentEntity } = require('./shared');
        obj = await isParentEntity(authorized);
    }
    const isParent = obj.isParent;
    const siteCodes = obj.siteCodes;

    if(req.body.data === undefined) return res.status(400).json(getResponseJSON('Bad request. Data is not defined in request body.', 400));
    if(!Array.isArray(req.body.data)) return res.status(400).json(getResponseJSON('Bad request. Data must be an array.', 400));
    if(req.body.data.length === undefined || req.body.data.length < 1) return res.status(400).json(getResponseJSON('Bad request. Data array does not have any elements.', 400));
    if(req.body.data.length > 100) return res.status(400).json(getResponseJSON('Bad request. Data contains more than acceptable limit of 100 records.', 400));

    const dataArray = req.body.data;
    let responseArray = [];

    // Track errors across all participants in request (multi-participant requests sent through the API)
    let batchError = false;

    let docCount = 0;

    for(let dataObj of dataArray) {

        // Track errors for each dataObj in the batch.
        let dataObjError = false;

        if(dataObj.token === undefined) {
            dataObjError = true;
            batchError = true;
            responseArray.push({'Invalid Request': {'Token': 'UNDEFINED', 'Errors': 'Token not defined in data object.'}});
            continue;
        }

        const participantToken = dataObj.token;
        const record = await getParticipantData(participantToken, siteCodes, isParent);

        if(!record) {
            dataObjError = true;
            batchError = true;
            responseArray.push({'Invalid Request': {'Token': participantToken, 'Errors': 'Token does not exist.'}});
            continue;
        }

        const docID = record.id;
        const docData = record.data;

        // Reject if participant has opted for data descruction.
        const dataHasBeenDestroyed = fieldMapping.participantMap.dataHasBeenDestroyed.toString();
        if (docData[dataHasBeenDestroyed] === fieldMapping.yes) {
            dataObjError = true;
            batchError = true;
            responseArray.push({'Invalid Request': {'Token': participantToken, 'Errors': 'Data Destroyed'}});
            continue;
        }

        // Refect if the participant has withdrawn consent unless the update is
        // for data distruction or hippa withdrawal 
        const withdrawConsent = refusalWithdrawalConcepts.withdrewConsent.toString();
        const revokeHIPAA = refusalWithdrawalConcepts.revokeHIPAA.toString();
        if (!internalCall && docData[withdrawConsent] === fieldMapping.yes && docData[revokeHIPAA] === fieldMapping.yes) {       
            dataObjError = true;
            batchError = true;
            responseArray.push({'Invalid Request': {'Token': participantToken, 'Errors': 'Particpant Withdrawn'}});
            continue;
        }

        // Reject to update the uninvited flag if the participant is verified or The PIN was used to sign in
        const uninvitedRecruitsId = fieldMapping.participantMap.uninvitedRecruits.toString()
        if (dataObj[uninvitedRecruitsId] && dataObj[uninvitedRecruitsId] === fieldMapping.yes &&
            (
                docData[fieldMapping.participantMap.signedInFlag.toString()] === fieldMapping.yes
                || docData[fieldMapping.participantMap.consentFormSubmitted.toString()] === fieldMapping.yes
                || docData[fieldMapping.verificationStatus.toString()] === fieldMapping.verified

            )
        ) {
            dataObjError = true;
            batchError = true;
            responseArray.push({ 'Invalid Request': { 'Token': participantToken, 'Errors': 'The participant is verified or has used a pin to sign in' }});
            continue;
        }

        // Reject if query key is included. Those values are derived.
        if (dataObj['query']) {
            dataObjError = true;
            batchError = true;
            responseArray.push({'Invalid Request': {'Token': participantToken, 'Errors': '\'Query\' variables not accepted. The expected values will be derived automatically.'}});
            continue;
        }

        // Handle site notifications
        if(dataObj['831041022'] && dataObj['747006172'] && dataObj['773707518'] && dataObj['831041022'] === 353358909 && dataObj['747006172'] === 353358909 && dataObj['773707518'] === 353358909){ // Data Destruction
            await siteNotificationsHandler(docData['Connect_ID'], '831041022', docData['827220437'], obj);
        }
        else if (dataObj['747006172'] && dataObj['773707518'] && dataObj['747006172'] === 353358909 && dataObj['773707518'] === 353358909) { // Withdraw Consent
            await siteNotificationsHandler(docData['Connect_ID'], '747006172', docData['827220437'], obj);
        }
        else if(dataObj['773707518'] && dataObj['773707518'] === 353358909) { // Revocation only email
            await siteNotificationsHandler(docData['Connect_ID'], '773707518', docData['827220437'], obj);
        }
        else if (dataObj['987563196'] && dataObj['987563196'] === 353358909) {
            await siteNotificationsHandler(docData['Connect_ID'], '987563196', docData['827220437'], obj);
        }
        
        // Flatten dataObj and docData for comparison & validation with JSON file.
        const flatUpdateObj = flattenObject(dataObj);
        const flatDocData = flattenObject(docData);

        // Delete primary identifiers from flatUpdateObj if they exist. There is no reason to update these through this API.
        const primaryIdentifiers = ['token', 'pin', 'Connect_ID', 'state.uid'];
        for (const identifier of primaryIdentifiers) {
            delete flatUpdateObj[identifier];
        }
        
        // Note: Data is validated in this function. Anything beyond this can be treated as valid.
        // Validate incoming flatDataObj data. flatDocData is only used to check the 'mustExist' property in some rules.
        if(!authObj) {
            const errors = flatValidationHandler(flatUpdateObj, flatDocData, rules, validateUpdateParticipantData);
            if(errors.length !== 0) {
                dataObjError = true;
                batchError = true;
                responseArray.push({'Invalid Request': {'Token': participantToken, 'Errors': errors}});
                continue;
            }
        }
        
        // Check initializeTimestamps and init on match. Currently, only one key exists in initializeTimestamps.
        const keysForTimestampGeneration = Object.keys(initializeTimestamps);
        for (const key of keysForTimestampGeneration) {
            if (flatUpdateObj[key] != null) {
                if (initializeTimestamps[key].value && initializeTimestamps[key].value === flatUpdateObj[key]) {
                    Object.assign(flatUpdateObj, initializeTimestamps[key].initialize);
                }
            }
        }

        // Handle deceased data. If participantDeceased === yes, derive participantDeceasedNORC === fieldMapping.yes.
        // Ignore and delete deceased data if participantDeceased === no. There is no error case (data already validated).
        if (flatUpdateObj[fieldMapping.participantDeceased] === fieldMapping.yes) {
            flatUpdateObj[fieldMapping.participantDeceasedNORC] = fieldMapping.yes;
            flatUpdateObj[fieldMapping.participationStatus] = fieldMapping.participationStatusDeceased;
        } else if (flatUpdateObj[fieldMapping.participantDeceased] === fieldMapping.no) {
            delete flatUpdateObj[fieldMapping.participantDeceased];
            delete flatUpdateObj[fieldMapping.participantDeceasedTimestamp];
        }

        // Handle destroyed data. If destroyData = yes then participation status = destroyData
        if (flatUpdateObj[fieldMapping.participantMap.destroyData] === fieldMapping.yes) {
            flatUpdateObj[fieldMapping.participationStatus] = fieldMapping.participantMap.dataDestructionRequested;
        }

        // Handle cancer occurrence data. This gets validated and directed to the Firestore cancerOccurrence collection. One occurrence per doc.
        const incomingCancerOccurrenceArray = dataObj[fieldMapping.cancerOccurrence] || [];
        let finalizedCancerOccurrenceArray = [];
        if (incomingCancerOccurrenceArray.length > 0) { 
            // delete flatUpdateObj keys that start with '637153953' (cancerOccurrence). These are handled separately.
            Object.keys(flatUpdateObj)
                .filter(key => key.startsWith(fieldMapping.cancerOccurrence.toString()))
                .forEach(key => delete flatUpdateObj[key]);

            const { handleCancerOccurrences } = require('./shared');
            const requiredOccurrenceRules = Object.keys(rules).filter(key => rules[key].required && key.startsWith(fieldMapping.cancerOccurrence.toString()));
            const participantConnectId = docData['Connect_ID'];
            const occurrenceResult = await handleCancerOccurrences(incomingCancerOccurrenceArray, requiredOccurrenceRules, participantToken, participantConnectId);
            if (occurrenceResult.error) {
                dataObjError = true;
                batchError = true;
                responseArray.push({'Invalid Request': {'Token': participantToken, 'Errors': occurrenceResult.message}});
                continue;
            }
            finalizedCancerOccurrenceArray = occurrenceResult.data;
        }

        // Handle NORC Birthday Card data: birthday card data + optional participant update from birthday card return data. 
        let finalizedBirthdayCardData = {};
        let norcParticipantUpdateData = {};
        let birthdayCardWriteDetails = {};
        const norcBirthdayCardData = dataObj[fieldMapping.norcBirthdayCard];
        if (norcBirthdayCardData && Object.keys(norcBirthdayCardData).length > 0) {
            Object.keys(flatUpdateObj)
                .filter(key => key.startsWith(fieldMapping.norcBirthdayCard.toString()))
                .forEach(key => delete flatUpdateObj[key]);
            
            const { handleNorcBirthdayCard } = require('./shared');
            const requiredNorcBirthdayCardRules = Object.keys(rules).filter(key => rules[key].required && key.startsWith(fieldMapping.norcBirthdayCard.toString()));
            const participantConnectId = docData['Connect_ID'];
            const participantProfileHistory = docData[fieldMapping.userProfileHistory];
            const birthdayCardResult = await handleNorcBirthdayCard(norcBirthdayCardData, requiredNorcBirthdayCardRules, participantToken, participantConnectId, participantProfileHistory);

            if (birthdayCardResult.error) {
                dataObjError = true;
                batchError = true;
                responseArray.push({'Invalid Request': {'Token': participantToken, 'Errors': birthdayCardResult.message}});
                continue;
            } else {
                [finalizedBirthdayCardData, norcParticipantUpdateData, birthdayCardWriteDetails] = birthdayCardResult.data;                
                if (Object.keys(norcParticipantUpdateData).length > 0) {
                    Object.assign(dataObj, norcParticipantUpdateData); // query field check
                    Object.assign(flatUpdateObj, norcParticipantUpdateData); // profile history check
                }
            }
        }

        // Handle reinvitation updates. This is a second round of invitations for participants who did not respond to the initial invitation.
        const reinvitationCampaignType = flatUpdateObj[fieldMapping.reinvitationCampaignType];
        if (reinvitationCampaignType) {
            // Confirm there's no ConnectID and the participant status is 'active', which is set on the initial invitation.
            // Enforce that only the reinvitationCampaignType is being updated. If other fields are present, reject the request.
            if (!docData['Connect_ID'] && docData[fieldMapping.autogeneratedRecruitmentType] === fieldMapping.recruitActive && !docData[fieldMapping.reinvitationTimestamp] && Object.keys(flatUpdateObj).length === 1) {
                flatUpdateObj[fieldMapping.reinvitationTimestamp] = new Date().toISOString();
            } else {
                const connectIDMessage = docData['Connect_ID'] ? ' Connect ID already exists.' : '';
                const participantStatusMessage = docData[fieldMapping.autogeneratedRecruitmentType] !== fieldMapping.recruitActive ? ' Participant is not active.' : '';
                const alreadyReinvitedMessage = docData[fieldMapping.reinvitationTimestamp] ? ' Participant has already been reinvited.' : '';
                dataObjError = true;
                batchError = true;
                responseArray.push({ 'Invalid Request': { 'Token': participantToken, 'Errors': `Participant not eligible for reinvitation.${connectIDMessage}${participantStatusMessage}${alreadyReinvitedMessage}` } });
                continue;
            }
        }

        // Handle updates to query.firstName, query.lastName, query.allPhoneNo, and query.allEmails arrays (these are used for participant search). Derive and add the updated query array to flatDataObj.
        const shouldUpdateQueryFields = checkForQueryFields(dataObj);
        if (shouldUpdateQueryFields) {
            const { updateQueryListFields } = require('./shared');
            if (!flatUpdateObj['query']) flatUpdateObj['query'] = {};
            flatUpdateObj['query'] = updateQueryListFields(dataObj, docData);
        }

        // Handle updates to user profile history. userProfileHistory is an array of objects. Each object has a timestamp and a userProfile object.
        const shouldUpdateUserProfileHistory = userProfileHistoryKeys.some(key => key in flatUpdateObj);
        if (shouldUpdateUserProfileHistory) {
            const { updateUserProfileHistory } = require('./shared');
            flatUpdateObj[fieldMapping.userProfileHistory] = updateUserProfileHistory(dataObj, docData, siteCodes);
        }
        
        try {
            if (!dataObjError) {
                const promises = [];
                if (Object.keys(flatUpdateObj).length > 0) promises.push(updateParticipantDataFirestore(docID, flatUpdateObj));
                if (finalizedCancerOccurrenceArray.length > 0) promises.push(writeCancerOccurrences(finalizedCancerOccurrenceArray));
                if (Object.keys(finalizedBirthdayCardData).length > 0) promises.push(writeBirthdayCard(finalizedBirthdayCardData, birthdayCardWriteDetails));
                await Promise.all(promises);
                await checkDerivedVariables(participantToken, docData['827220437']);
                
                responseArray.push({'Success': {'Token': participantToken, 'Errors': 'None'}});
            
                docCount++;
            }
        } catch (e) {
            // Alert the user about the error for this participant but continue to process the rest of the participants.
            console.error(`Server error updating participant at updateParticipantData & checkDerivedVariables. ${e}`);
            dataObjError = true;
            batchError = true;
            responseArray.push({'Server Error': {'Token': participantToken, 'Errors': `Please retry this participant. Error: ${e}`}});
            continue;
        }
    }

    console.log(`Updated ${docCount} participant records.`);
    return res.status(batchError ? 206 : 200).json({code: batchError ? 206 : 200, results: responseArray});
}

/**
 * Delegate validation and return errors.
 * @param {object} newData - the new data object to validate.
 * @param {object} existingData - the existing data object to validate against.
 * @param {object} rules - the validation rules object (imported from JSON file).
 * @param {function} validationFunction - the validation function to use.
 * @returns {array} - an array of errors. Success if empty.
 */
const flatValidationHandler = (newData, existingData, rules, validationFunction) => {
    let errors = [];
    for (const [path, value] of Object.entries(newData)) {
        const validationPath = path.replace(/\[\d+\]/g, '[]'); // Handling array rules
        const rule = rules[validationPath];

        // If a rule exists for the data point, validate.
        if (rule) {
            const error = validationFunction(value, existingData[path], validationPath, rule);
            if (error) errors.push(error);
        } else {
            errors.push(`Error: No validation rule exists for "${path}".`); // Reject POST request if no rule is established.
            return errors;
        }
    }
    return errors;
}

/**
 * Validate data submitted to the updateParticipantData endpoint.
 * @param {string|number|array|object} value - The value to validate. From a key:value pair submitted in the POST request.
 * @param {string|number|array|object} existingValue - The existing value to validate against. From the existing participant data in the database.
 * @param {string} path - The flattened path to the value in the data object. Example: 'state.123456789' or '637153953[].149205077' <- where [] is an array with any index value.
 * @param {object} rule - The validation rule to use from updateParticipantData.json. Example: { "dataType": "string", "maxLength": 100 }
 * @returns null for success, or an error message for failure.
 */
const validateUpdateParticipantData = (value, existingValue, path, rule) => {
    if (rule.mustExist && (existingValue === undefined || existingValue === null)) {
        return `Key (${path}) must exist before updating.`;
    }

    switch (rule.dataType) {
        case 'string':
            if (typeof value !== 'string') {
                return `Data mismatch: ${path} must be a string.`;
            } else if (rule.maxLength && value.length > rule.maxLength) {
                return `Data mismatch: ${path} must be less than ${rule.maxLength} characters. It is currently ${value.length} characters.`;
            }

            if (rule.values && !rule.values.includes(value)) {
                return `Data mismatch: ${path} must be one of the following values: ${rule.values.join(', ')}.`;
            }
            break;

        case 'number':
            if (typeof value !== 'number') {
                return `Data mismatch: ${path} must be a number.`;
            }
            if (rule.values && !rule.values.includes(value)) {
                return `Data mismatch: ${path} must be one of the following values: ${rule.values.join(', ')}.`;
            }
            break;

        case 'ISO':
            const validationResponse = validateIso8601Timestamp(value);
            if (validationResponse.error === true) {
                return `Data mismatch: ${path} ${validationResponse.message}`;
            }

            break;

        case 'phone':
            if (typeof value !== 'string' || !validPhoneFormat.test(value)) {
                return `Data mismatch: ${path} must be a valid phone number. 10 character string, no spaces, no dashes. Example: '1234567890'`;
            }
            break;

        case 'email':
            if (typeof value !== 'string' || !validEmailFormat.test(value)) {
                return `Data mismatch: ${path} must be a valid email address. Example: abc@xyz.com`;
            }
            break;

        case 'zipCode':
            if (typeof value !== 'string' || value.length !== 5) {
                return `Data mismatch: ${path} zip code must be a 5 character string.`;
            }
            break;

        case 'array':
            if (!Array.isArray(value)) {
                return `Data mismatch: ${path} must be an array.`;
            }
            if (rule.innerElementType) {
                for (let i = 0; i < value.length; i++) {
                    if (typeof value[i] !== rule.innerElementType) {
                        return `Data mismatch: Element at index ${i} of ${path} must be a ${rule.innerElementType}.`;
                    }
                }
            }
            break;

        case 'object':
            if (typeof value !== 'object' || Array.isArray(value)) {
                return `Data mismatch: ${path} must be an object.`;
            }
            break;

        default:
            if (typeof value !== rule.dataType) {
                return `Data mismatch: ${path} must be a ${rule.dataType}.`;
            }
            break;
    }

    return null;
}

const updateUserAuthentication = async (req, res, authObj) => {
    if(req.method !== 'POST') {
        return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
    }

    if(req.body.data === undefined) {
        return res.status(400).json(getResponseJSON('Bad request. Data is not defined in request body.', 400));
    }

    const permSiteArray = ['NIH', 'NORC'];
    if (!permSiteArray.includes(authObj.acronym)) {
        return res.status(403).json(getResponseJSON('You are not authorized!', 403));
    }

    const { updateUserPhoneSigninMethod, updateUserEmailSigninMethod, updateUsersCurrentLogin } = require('./firestore');
    let status = ``
    if (req.body.data['phone'] && req.body.data.flag === `replaceSignin`) status = await updateUserPhoneSigninMethod(req.body.data.phone, req.body.data.uid);
    if (req.body.data['email'] && req.body.data.flag === `replaceSignin`) status = await updateUserEmailSigninMethod(req.body.data.email, req.body.data.uid);
    if (req.body.data.flag === `updateEmail` || req.body.data.flag === `updatePhone`) status = await updateUsersCurrentLogin(req.body.data, req.body.data.uid);
    if (status === true) return res.status(200).json({code: 200});
    else if (status === `auth/phone-number-already-exists`) return res.status(409).json(getResponseJSON('The user with provided phone number already exists.', 409));
    else if (status === `auth/email-already-exists`) return res.status(409).json(getResponseJSON('The user with the provided email already exists.', 409));
    else if (status === `auth/invalid-phone-number`) return res.status(403).json(getResponseJSON('Invalid Phone number', 403));
    else if (status === `auth/invalid-email`) return res.status(403).json(getResponseJSON('Invalid Email', 403));
    else return res.status(400).json(getResponseJSON('Operation Unsuccessful', 400));
}

const participantDataCorrection = async (req, res) => {
    logIPAddress(req);
    setHeaders(res);

    if (req.method === 'OPTIONS') return res.status(200).json({ code: 200 });

    if (req.method !== 'POST') {
        return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
    }
    if(req.body.data.length === 0) return res.status(400).json(getResponseJSON('Bad request. Empty array.', 400));

    try {
        const { updateParticipantCorrection } = require('./firestore');
        if (req.body.data) {
            const status = await updateParticipantCorrection(req.body.data[0]);
            return status === true
                ? res.status(200).json({ code: 200 })
                : res.status(400).json(getResponseJSON('Operation Unsuccessful', 400));
        } else {
            return res.status(400).json(getResponseJSON('Invalid request format', 400));
        }
    } catch (error) {
        console.error(error);
        return res.status(500).json(getResponseJSON('Internal Server Error', 500));
    }
};

const getBigQueryData = async (req, res) => {
    const { validateTableAccess, validateFilters, validateFields, getBigQueryData } = require('./bigquery');

    logIPAddress(req);
    setHeaders(res);
    
    if (req.method === 'OPTIONS') return res.status(200).json({ code: 200 });

    if (req.method !== 'GET') {
        return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
    }
    const { APIAuthorization } = require('./shared');
    const authorized = await APIAuthorization(req);
    if (authorized instanceof Error) {
        return res.status(500).json(getResponseJSON(authorized.message, 500));
    }

    if (!authorized) {
        return res.status(401).json(getResponseJSON('Authorization failed!', 401));
    }

    if (req.query.table === undefined || !req.query.table) return res.status(400).json(getResponseJSON('Bad request. Table is not defined in query', 400));
    if (req.query.dataset === undefined || !req.query.dataset) return res.status(400).json(getResponseJSON('Bad request. Dataset is not defined in query', 400));

    //Validate the caller has access to the table and dataset
    const dataset = req.query.dataset;
    const table = req.query.table;
    let allowAccess = await validateTableAccess(authorized, dataset, table)
    if (!allowAccess) {
        return res.status(401).json(getResponseJSON('Forbidden', 403));
    }

    let filters = req.query.filters;
    if (Array.isArray(filters)) {
        try {
            filters.forEach((filter, index) => {
                filters[index] = JSON.parse(filter);
            });
        } catch (e) {
            return res.status(400).json(getResponseJSON('Bad request. '+e.toString(), 400));
        }   

    } else if (typeof filters === "string") {
        try {
            filters = [JSON.parse(filters)];
        } catch (e) {
            return res.status(400).json(getResponseJSON('Bad request. Filters is not an array or JSON object', 400));
        }
    }

    if (Array.isArray(filters)) {
        let validFilters = await validateFilters(dataset, table, filters);
        if (!validFilters) {
            return res.status(400).json(getResponseJSON('Bad request. Filters are invalid', 400));
        }
    }

    let fields = req.query.fields;
    if (Array.isArray(fields) && fields.length > 0) {
        let validFields = await validateFields(dataset, table, fields);
        if (!validFields) {
            return res.status(400).json(getResponseJSON('Bad request. Fields are invalid', 400));
        }
    }

    let error;
    let responseArray = [];
    try {
        responseArray = await getBigQueryData(dataset, table, filters, fields);
    } catch (e) {
        error = e.toString();
        console.log(e);
    }

    return res.status(error ? 500 : 200).json(error ? getResponseJSON(error, 500) : {code: 200, results: responseArray});
}


module.exports = {
    getBigQueryData,
    submitParticipantsData,
    updateParticipantData,
    updateUserAuthentication,
    participantDataCorrection
}