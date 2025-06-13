const { getResponseJSON, setHeaders, logIPAddress, SSOValidation, convertSiteLoginToNumber } = require('./shared');
const fieldMapping = require('./fieldToConceptIdMapping');
const { sendInstantNotification } = require("./notifications");

const biospecimenAPIs = async (req, res) => {
    logIPAddress(req);
    setHeaders(res);

    if(req.method === 'OPTIONS') return res.status(200).json({code: 200});

    if(!req.headers.authorization || req.headers.authorization.trim() === ""){
        return res.status(401).json(getResponseJSON('Authorization failed!', 401));
    }

    const query = req.query;
    if(!query.api) return res.status(400).json(getResponseJSON('Bad request!', 400));

    const api = query.api;
    console.log("API Accessed: Biospecimen - " + api);

    const idToken = req.headers.authorization.replace('Bearer','').trim();
    const { validateIDToken } = require('./firestore');
    let decodedToken = await SSOValidation('biospecimenUser', idToken) || await validateIDToken(idToken);

    if(decodedToken instanceof Error){
        return res.status(401).json(getResponseJSON(decodedToken.message, 401));
    }
    
    if(!decodedToken){
        return res.status(401).json(getResponseJSON('Authorization failed!', 401));
    }
    
    const email = decodedToken.email;
    console.log("Accessed By: " + email);

    const isBPTLUser = decodedToken.isBPTLUser !== undefined ? decodedToken.isBPTLUser : false;
    const isBiospecimenUser = decodedToken.isBiospecimenUser !== undefined ? decodedToken.isBiospecimenUser : false;

    let obj = {};
    let role = "";
    let siteAcronym = "";
    let siteCode = undefined;

    if(!isBPTLUser && !isBiospecimenUser) {
        const { validateBiospecimenUser } = require('./firestore');
        const isValidUser = await validateBiospecimenUser(email);
        if(!isValidUser) return res.status(401).json(getResponseJSON('Authorization failed!', 401));
        role = isValidUser.role;
        siteCode = isValidUser.siteCode;
        siteAcronym = isValidUser.siteAcronym;
        obj = { role, siteCode,siteAcronym, isBPTLUser, isBiospecimenUser, email };
    }
    else {
        role = 'user';
        siteCode = decodedToken.siteDetails.siteCode ? decodedToken.siteDetails.siteCode : 13;
        siteAcronym = decodedToken.siteDetails.acronym;
        obj = { role, siteAcronym, isBPTLUser, isBiospecimenUser, email };
        if(siteCode !== 0) obj['siteCode'] = siteCode;
    }

    if(api === 'getFilteredParticipants') {
        if(req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }

        // req.query includes 'api' key plus query params from the participant search form.
        if(Object.keys(req.query).length < 2) {
            return res.status(400).json(getResponseJSON('Please include parameters to filter data.', 400));
        }

        try {
            req.query.source = 'biospecimen';
            const { getFilteredParticipants } = require('./submission');
            
            return await getFilteredParticipants(req, res, obj);
        } catch (error) {
            console.error('Error in getFilteredParticipants.', error);
            return res.status(500).json(getResponseJSON('An error occurred while searching for this participant. Please try again later.', 500));
        }
    }

    if(api === 'getDailyReportParticipants') {
        if(req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }
        const { queryDailyReportParticipants } = require('./firestore');
        const result = await queryDailyReportParticipants(siteCode);
        if(result instanceof Error){
            return res.status(500).json(getResponseJSON(result.message, 500));
        }

        return res.status(200).json({data: result, code: 200})
    }

    else if(api === 'validateUsers') {
        if(req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }
        return res.status(200).json({data: obj, code:200});
    }
    else if(api === 'users' && (role === 'admin' || role === 'manager')) {
        if(req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }
        const { biospecimenUserList } = require('./firestore');
        const usersList = role === 'admin' ? await biospecimenUserList(siteCode) : await biospecimenUserList(siteCode, email);
        return res.status(200).json({data: {users: usersList}, code: 200})
    }
    else if(api === 'addUsers' && (role === 'admin' || role === 'manager')) {
        if(req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }
        const requestData = req.body;
        console.log(requestData);
        if(requestData.length === 0 ) return res.status(400).json(getResponseJSON('Request body is empty!', 400));
        for(let person of requestData) {
            if(person.name && person.email && person.role){
                if(role === 'admin' && ( person.role === 'manager' || person.role === 'user')){
                    const response = await addNewUser(person, email, siteCode);
                    if(response instanceof Error){
                        return res.status(400).json(getResponseJSON(response.message, 400));
                    }
                }
                if(role === 'manager' && person.role === 'user'){
                    const response = await addNewUser(person, email, siteCode);
                    if(response instanceof Error){
                        return res.status(400).json(getResponseJSON(response.message, 400));
                    }
                }
            }
        }
        return res.status(200).json({message: 'Success!', code: 200})
    }
    else if (api === 'removeUser'  && (role === 'admin' || role === 'manager')) {
        if(req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }
        const emailId = req.query.email;
        if(!emailId) return res.status(400).json(getResponseJSON('Query parameter email is missing.', 400));
        if(emailId === email) return res.status(400).json(getResponseJSON('Can not remove yourself.', 400));
        const { removeUser } = require('./firestore');
        let response = '';
        if(role === 'admin') response = await removeUser(emailId, siteCode, email);
        else if(role === 'manager') response = await removeUser(emailId, siteCode, email, true);
        if(!response) return res.status(404).json(getResponseJSON('User not found.', 404));
        return res.status(200).json({message: 'Success!', code:200})
    }
    else if (api === 'addSpecimen') {
        if(req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }
        const requestData = req.body;
        if(requestData.length === 0 ) return res.status(400).json(getResponseJSON('Request body is empty!', 400));
        for(let specimen of requestData) {
            if(specimen['820476880']){
                const masterSpecimenId = specimen['820476880'];
                const { specimenExists } = require('./firestore');
                const exists = await specimenExists(masterSpecimenId, specimen)
                if(exists === true) return res.status(400).json(getResponseJSON('Specimen already exists!', 400));
                if(exists === false){
                    const { v4: uuid } = require('uuid');
                    specimen['id'] = uuid();
                    const { storeSpecimen } = require('./firestore');
                    await storeSpecimen(specimen);

                }
            }
            return res.status(200).json({message: 'Success!', code:200})
        }
    }
    else if(api === 'accessionIdExists'){
        if(req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }
        const requestData = req.body;
        if(Object.keys(requestData).length === 0 ) return res.status(400).json(getResponseJSON('Request body is empty!', 400));
        if(requestData['accessionId']){
            const accessionId = requestData['accessionId'];
            const accessionIdType = requestData['accessionIdType'];
            const { accessionIdExists } = require('./firestore');
            const existingData = await accessionIdExists(accessionId, accessionIdType, siteCode);
            if (!!existingData) return res.status(200).json({message: 'AccessionId exists!', data: existingData, code:200})
        }
        return res.status(200).json(getResponseJSON('AccessionId doesn\'t exist!', 200));
    }
    else if (api === 'updateSpecimen') {
        if(req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }
        const requestData = req.body;
        if(requestData.length === 0 ) return res.status(400).json(getResponseJSON('Request body is empty!', 400));
        for(let specimen of requestData) {
            if(specimen['820476880']){
                const masterSpecimenId = specimen['820476880'];
                const { specimenExists } = require('./firestore');
                const exists = await specimenExists(masterSpecimenId, specimen)
                if(exists === false) return res.status(400).json(getResponseJSON('Specimen does not exist!', 400));
                if(exists === true){
                    const { updateSpecimen } = require('./firestore');
                    await updateSpecimen(masterSpecimenId, specimen);
                    
                    return res.status(200).json({message: 'Success!', code:200});
                }
            }
            else {
                return res.status(400).json({message: 'Collection ID does not exist in the request body!', code:400});
            }
        }
    }
    else if (api === 'submitSpecimen') {
        if(req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }
        const {submitSpecimen} = require('./firestore');
        const responseJson = await submitSpecimen(req.body.biospecimenData, req.body.participantData, req.body.siteTubesList);
        return res.status(responseJson.code).json(responseJson);
    }
    else if (api === 'checkDerivedVariables') {
        if(req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }

        const requestData = req.body;
        if(requestData.length === 0 ) {
            return res.status(400).json(getResponseJSON('Request body is empty!', 400));
        }

        const token = requestData.token;
        if(!token) {
            return res.status(400).json(getResponseJSON('Request body does not include token!', 400));
        }

        const { checkDerivedVariables } = require ('./validation');
        await checkDerivedVariables(token, siteCode);

        return res.status(200).json({message: 'Success!', code:200});
    }
    else if (api === 'getSpecimenAndParticipant') {
        if (req.method !== 'GET') return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        if (!req.query.collectionId) return res.status(400).json(getResponseJSON('Collection ID is missing.', 400));

        const collectionId = req.query.collectionId;
        const isBPTL = req.query.isBPTL === 'true';

        try {
            const { getSpecimenAndParticipant } = require('./firestore');
            const { specimenData, participantData } = await getSpecimenAndParticipant(collectionId, siteCode, isBPTL);
            return res.status(200).json({ data: [specimenData, participantData], message: 'Success!', code: 200 });
        } catch (error) {
            console.error(`Error in getSpecimenAndParticipant(). ${error.message}`);
            return res.status(500).json({ data: [], message: `Error in getSpecimenAndParticipant(). ${error.message}`, code: 500 });
        }
    }
    else if (api === 'searchSpecimen') {
        if(req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }
        if(req.query.masterSpecimenId) {
            const { searchSpecimen } = require('./firestore');
            const masterSpecimenId = req.query.masterSpecimenId;
            const allSitesFlag = (req.query.allSitesFlag) ? true : false;

            try {
              const biospecimenData = await searchSpecimen(masterSpecimenId, siteCode, allSitesFlag);
              return res.status(200).json({ data: biospecimenData, message: 'Success!', code: 200 });
            } catch (error) {
              console.error('Error occurred when running searchSpecimen:', error);
              return res.status(500).json({ data: {}, message: 'Error occurred when running searchSpecimen.', code: 500 });
            }
        } else if (req.query.requestedSite && req.query.boxId) {
            const { searchSpecimenBySiteAndBoxId } = require('./firestore');
            const requestedSite = convertSiteLoginToNumber(req.query.requestedSite);
            const boxId = req.query.boxId;

            try {
                const biospecimenData = await searchSpecimenBySiteAndBoxId(requestedSite, boxId);
                return res.status(200).json({ data: biospecimenData, message: 'Success!', code: 200 });
                
            } catch (error) {
                console.error('Error occurred when running searchSpecimenBySiteAndBoxId:', error);
                return res.status(500).json({ data: [], message: error, code: 500 });
            }
        } else {
            const { searchShipments } = require('./firestore');
            const requestedSite = convertSiteLoginToNumber(req.query.requestedSite);
            const response = requestedSite ? await searchShipments(requestedSite) : await searchShipments(siteCode);
            return res.status(200).json({data: response, code:200});
        }
    }
    else if(api === 'getParticipantCollections') {
        if(req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }
        if(req.query.token) {
            const { getSpecimenCollections } = require('./firestore');
            const token = req.query.token;

            try {
              const specimenArray = await getSpecimenCollections(token, siteCode);
              return res.status(200).json({ data: specimenArray, message: 'Success!', code: 200 });
            } catch (error) {
              console.error('Error occurred when running getSpecimenCollections:', error);
              return res.status(500).json({ data: [], message: 'Error occurred when running getSpecimenCollections.', code: 500 });
            }
        }

        return res.status(400).json(getResponseJSON('Bad request!', 400));
    }
    else if(api === 'addBoxAndUpdateSiteDetails'){
        try {
            if(req.method !== 'POST') {
                return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
            }
    
            const requestData = req.body;
            if(Object.keys(requestData).length === 0 ) return res.status(400).json(getResponseJSON('Request body is empty!', 400));

            const boxId = requestData['132929440'];
            const loginSite = requestData['789843387'];
    
            if (!boxId || !loginSite) {
                return res.status(400).json(getResponseJSON('Required fields are missing!', 400));
            }
    
            const { boxExists, addBoxAndUpdateSiteDetails } = require('./firestore');
            const exists = await boxExists(boxId, loginSite);
    
            if (exists === true) {
                return res.status(409).json(getResponseJSON('Conflict: Box already exists!', 409));
            } else if (exists === false) {
                await addBoxAndUpdateSiteDetails(requestData);
                return res.status(200).json({message: 'Success!', code:200});
            }
        } catch (error) {
            console.error("Error in addBoxAndUpdateSiteDetails endpoint:", error.message);
            return res.status(500).json(getResponseJSON('Internal Server Error', 500));
        }
    }
    else if(api == 'updateBox'){
        if(req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }
        
        const requestData = req.body;
        if(Object.keys(requestData).length === 0 ) return res.status(400).json(getResponseJSON('Request body is empty!', 400));
        
        try {
            const { boxExists, updateBox } = require('./firestore');
            
            if (requestData[fieldMapping.shippingBoxId]){
                const boxId = requestData[fieldMapping.shippingBoxId];
                const loginSite = requestData[fieldMapping.loginSite];
                const addedTubes = requestData['addedTubes'];
        
                if (!addedTubes || addedTubes.length === 0) return res.status(400).json(getResponseJSON('Missing added tubes', 400));

                const exists = await boxExists(boxId, loginSite, requestData);
                if (exists !== true) return res.status(404).json(getResponseJSON('Box does not exist!', 404));
                
                const updatedSpecimenData = await updateBox(boxId, requestData, addedTubes, loginSite);
                if (!updatedSpecimenData) {
                    return res.status(500).json(getResponseJSON('Failed to update box!', 500));
                }
                return res.status(200).json({data: updatedSpecimenData, message: 'Success!', code: 200});
            } else {
                return res.status(400).json(getResponseJSON('Error: missing boxId', 400));
            } 
        } catch (error) {
                console.error('Error updating box:', error);
                return res.status(500).json(getResponseJSON(`Error updating box. ${error}`, 500));
        }
    }
    else if (api === 'searchBoxes') {
        if(req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }
        const { searchBoxes } = require('./firestore');
        let querySource = '';
        if (req.query.source === `bptl` || req.query.source === `bptlPackagesInTransit`) { 
            querySource = req.query.source;
        }
        const response = await searchBoxes(siteCode, querySource);
        return res.status(200).json({data: response, code: 200});
    }
    else if (api === 'searchBoxesByLocation'){
        if(req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }
        let location = req.body.location;
        console.log(location);
        const { searchBoxesByLocation } = require('./firestore');
        const response = await searchBoxesByLocation(siteCode, location);
        return res.status(200).json({data: response, code:200});
    }
    else if (api === 'getBoxesById') {
        if(req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }

        const boxIdQuery = req.query.boxIdArray;
        if (!boxIdQuery) return res.status(400).json(getResponseJSON('BoxIdArray is missing.', 400));
        
        const boxIdArray = boxIdQuery.split(',');

        try {
            const { getBoxesByBoxId } = require('./firestore');
            const boxesList = await getBoxesByBoxId(boxIdArray, siteCode);
            return res.status(200).json({data: boxesList, code:200});
        } catch (error) {
            return res.status(500).json({ message: `Internal Server Error running getBoxesById(), ${error}`, code: 500 });
        }
    }
    else if(api === 'ship'){
        if(req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }

        // Confirm request data
        const boxIdToTrackingNumberMap = req.body.boxIdToTrackingNumberMap;
        const shippingData = req.body.shippingData;

        if (!boxIdToTrackingNumberMap || !shippingData) {
            return res.status(400).json(getResponseJSON('Request data is incomplete!', 400));
        }

        // Confirm shipmentCourier & shipperEmail are in shippingData
        const requiredKeysInShippingdData = [`${fieldMapping.shipmentCourier}`, `${fieldMapping.shipperEmail}`];
        for (const key of requiredKeysInShippingdData) {
            if (!shippingData[key]) {
                return res.status(400).json(getResponseJSON(`Required key ${key} is missing in shipping data!`,400));
            }
        }

        // Confirm Box Ids Exist
        const boxIdArray = Object.keys(boxIdToTrackingNumberMap);
        if (boxIdArray.length === 0 ) {
            return res.status(400).json(getResponseJSON('No box data found in request!', 400));
        }
        
        let {boxWithTempMonitor, ...sharedShipmentData} = shippingData;
        
        sharedShipmentData = {
            ...sharedShipmentData,
            [fieldMapping.submitShipmentTimestamp]: new Date().toISOString(),
            [fieldMapping.submitShipmentFlag]: fieldMapping.yes,
            [fieldMapping.temperatureProbeInBox]: fieldMapping.no,
        };

        let boxIdAndShipmentDataArray = [];

        for (const boxId of boxIdArray) {
            let shipmentData = {
                ...sharedShipmentData,
                [fieldMapping.boxTrackingNumberScan]: boxIdToTrackingNumberMap[boxId],
            };

            if (boxWithTempMonitor === boxId) {
                shipmentData[fieldMapping.temperatureProbeInBox] = fieldMapping.yes;
            }

            boxIdAndShipmentDataArray.push({boxId, shipmentData});
        }

        try {
            const { shipBatchBoxes} = require('./firestore');
            
            const isShipmentSuccessful = await shipBatchBoxes(boxIdAndShipmentDataArray, siteCode);
            if (!isShipmentSuccessful) {
                return res.status(500).json({ message: 'Failed to save box data', code: 500 });
            }
            
            return res.status(200).json({ message: 'Success!', code: 200 });
        } catch (error) {
            console.error(`Error occurred when running shipBatchBoxes(). ${error}`);
            return res.status(500).json({ message: `Internal Server Error: ${error}`, code: 500 });
        }    
    }
    else if (api === 'updateParticipantData') {
        const { updateParticipantData } = require('./sites');
        return updateParticipantData(req, res, siteCode)
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
    else if (api === 'getUserProfile') {

        const { getUserProfile } = require('./shared');

        if(query.uid) {
            return getUserProfile(req, res, query.uid);
        }
        else return res.status(400).json(getResponseJSON('Bad request!', 400));
    }
    else if (api === 'removeBag') {
        if(req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }

        const requestData = req.body;
        if (Object.keys(requestData).length === 0) return res.status(400).json(getResponseJSON('Request body is empty!', 400));
        
        try {
            const {removeBag} = require('./firestore');
            const updatedSpecimenDataArray = await removeBag(siteCode, requestData);
            if (!updatedSpecimenDataArray) {
                return res.status(500).json(getResponseJSON('Failure. Could not remove bag.', 500));
            }
            return res.status(200).json({data: updatedSpecimenDataArray, message: 'Success!', code: 200});
        } catch (error) {
            console.error('Error removing bag:', error);
            return res.status(500).json(getResponseJSON('Error removing bag', 500));
        }
    }
    else if (api === 'getUnshippedBoxes') {
        if (req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }

        const isBPTL = req.query.isBPTL === 'true';

        try {
            const { getUnshippedBoxes } = require('./firestore');
            const unshippedBoxes = await getUnshippedBoxes(siteCode, isBPTL);
            return res.status(200).json({data: unshippedBoxes, code:200});
        } catch (error) {
            console.error("Error in getUnshippedBoxes():", error.message);
            return res.status(500).json({ data: [], message: `Error running getUnshippedBoxes(). ${error}`, code: 500 });
        }
    }
    else if (api === 'getSpecimensByBoxedStatus'){
        if(req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }

        const boxedStatusArray = [fieldMapping.notBoxed, fieldMapping.partiallyBoxed, fieldMapping.boxed];
        const boxedStatus = req.query.boxedStatus;
        const parsedBoxStatus = parseInt(boxedStatus, 10);
        const isBPTL = req.query.isBPTL === 'true';

        if (!boxedStatus || isNaN(parsedBoxStatus) || !boxedStatusArray.includes(parsedBoxStatus)) {
            return res.status(400).json(getResponseJSON('Boxed status is invalid or missing.', 400));
        }

        try {
            const { getSpecimensByBoxedStatus } = require('./firestore');
            const specimens = await getSpecimensByBoxedStatus(siteCode, parsedBoxStatus, isBPTL);
            return res.status(200).json({data: specimens, code:200});
        } catch (error) {
            console.error("Error in getSpecimensByBoxedStatus():", error.message);
            return res.status(500).json({ data: [], message: `Error running getSpecimensByBoxedStatus(). ${error}`, code: 500 });
        }
    }
    else if (api === 'getSpecimensByCollectionIds'){
        if(req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }

        if (!req.query.collectionIdsArray) return res.status(400).json(getResponseJSON('collectionIdsArray is missing.', 400));

        const collectionIdArray = req.query.collectionIdsArray.split(',');
        const isBPTL = req.query.isBPTL ?? false;

        try {
            const { getSpecimensByCollectionIds } = require('./firestore');
            const specimensList = await getSpecimensByCollectionIds(collectionIdArray, siteCode, isBPTL);
            return res.status(200).json({data: specimensList, code:200});
        } catch (error) {
            return res.status(500).json({ message: `Internal Server Error running getSpecimensByCollectionIds(), ${error}`, code: 500 });
        }
    }
    else if (api === 'getSpecimensByReceivedDate'){
        if(req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }

        if (!req.query.receivedTimestamp) return res.status(400).json(getResponseJSON('Timestamp is missing.', 400));
        
        const receivedTimestamp = req.query.receivedTimestamp;

        try {
            const { getSpecimensByReceivedDate } = require('./firestore');
            const boxesByReceivedDateBPTL = await getSpecimensByReceivedDate(receivedTimestamp);
            return res.status(200).json({data: boxesByReceivedDateBPTL, code:200});
        } catch (error) {
            return res.status(500).json({ message: `Internal Server Error running getSpecimensByReceivedDate(), ${error}`, code: 500 });
        }
    }
    else if (api === 'reportMissingSpecimen'){
        if(req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }
        const {reportMissingSpecimen} = require('./firestore');
        const requestData = req.body;
        if(Object.keys(requestData).length === 0 ) return res.status(400).json(getResponseJSON('Request body is empty!', 400));

        await reportMissingSpecimen(siteAcronym, requestData);
        return res.status(200).json({message: 'Success!', code:200});
    }
    else if (api === 'getLocations'){
        if(req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }
        const {getLocations} = require('./firestore');
        const response = await getLocations(siteAcronym);
        return res.status(200).json({message: 'Success!', response, code:200});
    }
    else if (api === 'updateTempCheckDate'){
        if(req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }
        const {updateTempCheckDate} = require('./firestore');
        await updateTempCheckDate(siteAcronym);
        return res.status(200).json({message: 'Success!', code:200});
    }
    else if (api === 'getBoxesPagination'){
        if(req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }
        const {getBoxesPagination} = require('./firestore');
        const requestData = req.body;
        let toReturn = await getBoxesPagination(siteCode, requestData);
        return res.status(200).json({data: toReturn, code:200});
    }
    else if(api === 'getNumBoxesShipped'){
        if(req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }
        const {getNumBoxesShipped} = require('./firestore');
        const requestData = req.body;
        let response = await getNumBoxesShipped(siteCode, requestData);
        return res.status(200).json({data:response, code:200});
    }

    else if (api === 'addKitData'){
        if (req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }
        const requestData = req.body;
        if(Object.keys(requestData).length === 0 ) return res.status(400).json(getResponseJSON('Request body is empty!', 400));
        try {
            const { addKitAssemblyData } = require('./firestore');
            const response = await addKitAssemblyData(requestData);
            return res.status(200).json({ response, code:200 });
        }
        catch (error) {
            console.error(error);
            return res.status(500).json({response: error.message || `${error}`, code: 500});
        }
    }

    else if (api === 'updateKitData'){
        if (req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }
        const requestData = req.body;
        if(Object.keys(requestData).length === 0 ) return res.status(400).json(getResponseJSON('Request body is empty!', 400));
        try {
            const { updateKitAssemblyData } = require('./firestore');
            const response = await updateKitAssemblyData(requestData);
            return res.status(200).json({ response, code:200 });
        }
        catch (error) {
            console.error(error);
            return res.status(500).json({response: error.message || `${error}`, code: 500});
        }
    }
    else if (api === 'collectionUniqueness'){
        if( req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }
        const supplyKitId = req.query.supplyKitId;
        const collectionIdSuffix = (req.query.collectionId?.slice(0, -4) || "") + " " + (req.query.collectionId?.slice(-4) || ""); // add space to collection
        const returnKitTrackingNumberNumber = req.query.returnKitTrackingNumber;
        if (Object.keys(query).length === 0) return res.status(404).json(getResponseJSON('Please include id to check uniqueness.', 400));
        if (collectionIdSuffix.length < 14) return res.status(200).json({data: 'Check Collection ID', code:200});
        try {
            const { checkCollectionUniqueness } = require('./firestore');
            const response = await checkCollectionUniqueness(supplyKitId, collectionIdSuffix, returnKitTrackingNumberNumber);
            return res.status(200).json({data: response, code:200});
        }
        catch (error) {
            console.error(error);
            return res.status(500).json(getResponseJSON(error.message, 500));
        }
    }

    else if(api == 'assignKit'){
        if(req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }
        let requestData = req.body;
        if(Object.keys(requestData).length === 0 ) return res.status(400).json(getResponseJSON('Request body is empty!', 400));
        try {
            const { assignKitToParticipant } = require('./firestore');
            const response = await assignKitToParticipant(requestData);
            return res.status(200).json({ ...response, code:200 });
        }
        catch (error) {
            console.error(error);
            return res.status(500).json(getResponseJSON(error.message, 500));
        }
    }

    else if(api == 'verifyScannedCode'){
        if(req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }
        const query = req.query.id
        if(Object.keys(query).length === 0) return res.status(404).json(getResponseJSON('Please include id to verify scanned code.', 400));
        try {
            const { processVerifyScannedCode } = require('./firestore');
            const response = await processVerifyScannedCode(query);
            return res.status(200).json({data: response, code:200});
        }
        catch (error) {
            console.error(error);
            return res.status(500).json(getResponseJSON(error.message, 500));
        }
    }

    else if(api === 'confirmShipment'){
        if(req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }
        let requestData = req.body;
        if(Object.keys(requestData).length === 0 ) return res.status(400).json(getResponseJSON('Request body is empty!', 400));
        try {
            const { confirmShipmentKit } = require('./firestore');
            const response = await confirmShipmentKit(requestData);
            return res.status(200).json({response, code:200});
        }
        catch (error) {
            console.error(error);
            return res.status(500).json(getResponseJSON(error.message, 500));
        }
    }

    else if(api === 'kitReceipt') {
        if(req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }
        let requestData = req.body;
        if(Object.keys(requestData).length === 0 ) return res.status(400).json(getResponseJSON('Request body is empty!', 400));
        try {
            const { storeKitReceipt } = require('./firestore');
            const response = await storeKitReceipt(requestData);
            return res.status(200).json({response, code:200});
        }
        catch (error) {
            console.error(error);
            return res.status(500).json(getResponseJSON(error.message, 500));
        }
    }

    else if(api == 'totalAddressesToPrint'){
        if(req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }
        
        try {
            const { queryHomeCollectionAddressesToPrint } = require('./firestore');
            const response = await queryHomeCollectionAddressesToPrint(req.query ? req.query.limit : undefined);
            return res.status(200).json({data: response, code:200});
        } catch (error) {
            console.error(error);
            return res.status(500).json(getResponseJSON(error.message, 500));
        }
    }

    else if(api == 'totalAddressesToPrintCount'){
        if(req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }

        try {
            const { queryCountHomeCollectionAddressesToPrint } = require('./firestore');
            const response = await queryCountHomeCollectionAddressesToPrint();
            return res.status(200).json({data: response, code:200});
        } catch (error) {
            console.error(error);
        }
    }

    else if(api == 'totalReplacementAddressesToPrint'){
        if(req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }
        
        try {
            const { queryReplacementHomeCollectionAddressesToPrint } = require('./firestore');
            const response = await queryReplacementHomeCollectionAddressesToPrint(req.query ? req.query.limit : undefined);
            return res.status(200).json({data: response, code:200});
        } catch (error) {
            console.error(error);
            return res.status(500).json(getResponseJSON(error.message, 500));
        }
    }

    else if(api == 'totalReplacementAddressesToPrintCount'){
        if(req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }

        try {
            const { queryCountReplacementHomeCollectionAddressesToPrint } = require('./firestore');
            const response = await queryCountReplacementHomeCollectionAddressesToPrint();
            return res.status(200).json({data: response, code:200});
        } catch (error) {
            console.error(error);
            return res.status(500).json(getResponseJSON(err && err.message ? err.message : err, 500));
        }
    }

    else if(api === 'getKitsByReceivedDate') {
        if(req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }
        try {
            const queryReceivedDateTimestamp = req.query.receivedDateTimestamp;
            if(queryReceivedDateTimestamp.length === 0) return res.status(404).json(getResponseJSON('Please include parameter to filter data.', 400));
            const { queryKitsByReceivedDate } = require('./firestore');
            const response = await queryKitsByReceivedDate(queryReceivedDateTimestamp);
            return res.status(200).json({data: response, code:200});
        }
        catch(error) {
            console.error('Error querying kits', error);
            return res.status(500).json(getResponseJSON(error.message, 500));
        }
    }

    else if(api === 'kitStatusToParticipant') {
        if(req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }
        const requestData = req.body;
        if(Object.keys(requestData).length === 0 ) return res.status(400).json(getResponseJSON('Request body is empty!', 400));
        try {
            const { addKitStatusToParticipant } = require('./firestore');
            const response = await addKitStatusToParticipant(requestData);
            return res.status(200).json({data: response, code:200});
        }
        catch (error) {
            console.error(error);
            return res.status(500).json(getResponseJSON(error.message, 500));
        }
    }

    else if(api === 'kitStatusToParticipantV2') {
        if(req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }
        const requestData = req.body;
        if(Object.keys(requestData).length === 0 ) return res.status(400).json(getResponseJSON('Request body is empty!', 400));
        try {
            const { addKitStatusToParticipantV2 } = require('./firestore');
            const response = await addKitStatusToParticipantV2(requestData);
            return res.status(200).json({data: response, code:200});
        }
        catch (error) {
            console.error(error);
            return res.status(500).json(getResponseJSON(error.message, 500));
        }
    }

    else if (api === 'markParticipantAddressUndeliverable') {
        if(req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }
        const participantCID = req.body?.Connect_ID;
        if(!participantCID) {
            return res.status(400).json(getResponseJSON('No participant connect ID', 400));
        }

        try {
            const { markParticipantAddressUndeliverable } = require('./firestore');
            const data = await markParticipantAddressUndeliverable(participantCID);
            return res.status(200).json({code: 200, data});
        } catch(error) {
            console.error(error);
            return res.status(500).json(getResponseJSON(error.message, 500));
        }

    }

    else if(api === 'getElgiblePtsForAssignment') {
        if(req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }
        
        try {
            const { eligibleParticipantsForKitAssignment } = require('./firestore');
            const response = await eligibleParticipantsForKitAssignment();
            return res.status(200).json({data: response, code:200});
        } catch (error){
            console.error(error);
            return res.status(500).json(getResponseJSON(error.message, 500));
        }
    }

    else if(api == 'getKitData'){
        if(req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }
        const { getKitAssemblyData } = require('./firestore');
        const response = await getKitAssemblyData();
        if(!response) return res.status(404).json(getResponseJSON('ERROR!', 404));
        return res.status(200).json({data: response, code:200})
    }

    else if(api === 'getParticipantsByKitStatus') {
        if(req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }
        try {
            const statusType = req.query.type;
            
            const collectionId = req.query.collectionId;
            const connectId = req.query.connectId;
            const returnKitTrackingNumber = req.query.returnKitTrackingNum;
            const dateReceived = req.query.dateReceived;

            const filters = {
                collectionId: collectionId,
                connectId: connectId,
                returnKitTrackingNumber: returnKitTrackingNumber,
                receivedDateTime: dateReceived
            };

            const kitStatusOptions = [
                fieldMapping.pending.toString(),
                fieldMapping.assigned.toString(),
                fieldMapping.shipped.toString(),
                fieldMapping.received.toString()
            ];
            
            if (!statusType) return res.status(400).json(getResponseJSON('The type of kit status value is empty.', 400));
            if (!kitStatusOptions.includes(statusType)) return res.status(400).json(getResponseJSON('The type of kit status value is not one of the available options.', 400));
            
            const { getParticipantsByKitStatus } = require('./firestore');
            const response = await getParticipantsByKitStatus(statusType, filters);
            
            if (!response) return res.status(404).json(getResponseJSON('No matching document found!', 404));
            return res.status(200).json({data: response, code:200});
        } catch (error) { 
            console.error(error);
            return res.status(500).json(getResponseJSON(error.message, 500));
        }
    }

    else if(api === 'getSiteMostRecentBoxId'){
        if(req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }
        try {
            const {getSiteMostRecentBoxId} = require('./firestore');
            const response = await getSiteMostRecentBoxId(siteCode);
            if(!response) {
                return res.status(404).json(getResponseJSON('No matching document found!', 404));
            }
            return res.status(200).json({data: response, code: 200});
        } catch (error) {
            console.error("Error fetching site most recent box ID:", error);
            return res.status(500).json(getResponseJSON('Internal Server Error', 500));
        }
    }
     // Print Addresses POST- BPTL
    else if(api == 'printAddresses'){
        if(req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }
        let requestData = req.body;
        if(Object.keys(requestData).length === 0 ) return res.status(400).json(getResponseJSON('Request body is empty!', 400));
        const { addPrintAddressesParticipants } = require('./firestore');
        const response = await addPrintAddressesParticipants(requestData);
        if(!response) return res.status(404).json(getResponseJSON('ERROR!', 404));
        return res.status(200).json({message: `Success!`, code:200})
    }

        // Shipped POST- BPTL
    else if(api == 'shipped'){
        if(req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }
        let requestData = req.body;
        if(Object.keys(requestData).length === 0 ) return res.status(400).json(getResponseJSON('Request body is empty!', 400));
        const { shipKits } = require('./firestore');
        const response = await shipKits(requestData);
        if(!response) return res.status(404).json(getResponseJSON('ERROR!', 404));
        return res.status(200).json({message: `Success!`, code:200})
        }

    // Store Receipt POST- BPTL 
    else if (api === 'storeSpecimenReceipt') {
        if(req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }

        const requestData = req.body;
        
        if (!requestData || Object.keys(requestData).length === 0) {
            return res.status(400).json(getResponseJSON('Request body is empty!', 400));
        }
        
        try {
            const { storePackageReceipt } = require('./firestore');
            const response = await storePackageReceipt(requestData);

            if (!response) {
                return res.status(404).json(getResponseJSON('ERROR!', 404));
            } else if (response.message === 'Box Not Found') {
                return res.status(404).json(getResponseJSON(response.message, 404));
            } else if (response.message === 'Multiple Results' || response.message === 'Box Already Received') {
                return res.status(409).json({ message: response.message, data: response.data, code: 409 });
            } else {
                return res.status(200).json(getResponseJSON(response.message, 200));
            }
        } catch (error) {
            console.error('Error storing package receipt:', error);
            if (error.message.includes('setPackageReceiptFedex')) {
                return res.status(500).json(getResponseJSON(`FedEx processing error. ${error.message}`, 500));
            } else if (error.message.includes('setPackageReceiptUSPS')) {
                return res.status(500).json(getResponseJSON(`USPS processing error. ${error.message}`, 500));
            } else if (error.message.includes('processReceiptData')) {
                return res.status(500).json(getResponseJSON(`Data processing error. ${error.message}`, 500));
            } else {
                return res.status(500).json(getResponseJSON(`Internal server error. ${error.message}`, 500));
            }
        }
    }

    // BPTL Metrics GET- BPTL 
    else  if(api === 'bptlMetrics') {
            if(req.method !== 'GET') {
                return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
            }
            const { getBptlMetrics } = require('./firestore');
            const response = await getBptlMetrics();
            if(!response) return res.status(404).json(getResponseJSON('ERROR!', 404));
            return res.status(200).json({data: response, code:200})
        }

    // BPTL Metrics Shipped Kit GET
    else  if(api === 'bptlMetricsShipped') {
        if(req.method !== 'GET') {
            return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
        }
        const { getBptlMetricsForShipped } = require('./firestore');
        const response = await getBptlMetricsForShipped();
        if(!response) return res.status(404).json(getResponseJSON('ERROR!', 404));
        return res.status(200).json({data: response, code:200})
    }
    else if (api === 'sendClientEmail') {
        if(req.method !== 'POST') {
            return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
        }
        let requestData = req.body;
        const { sendClientEmail } = require('./firestore');
        const response = await sendClientEmail(requestData);
        if(!response) return res.status(404).json(getResponseJSON('ERROR!', 404));
        return res.status(200).json(getResponseJSON('Success!', 200));
    } else if (api === "sendInstantNotification") {
        if (req.method !== "POST") {
          return res.status(405).json(getResponseJSON("Only POST requests are accepted!", 405));
        }

        const requestData = req.body;
        try {
          await sendInstantNotification(requestData);
          return res.status(200).json(getResponseJSON("Success!", 200));
        } catch (err) {
          console.error(err);
          return res.status(500).json({ message: err.message, code: 500 });
        }
    }

    else return res.status(400).json(getResponseJSON('Bad request!', 400));
};

const addNewUser = async (person, email, siteCode) => {
    const { biospecimenUserExists } = require('./firestore');
    const exists = await biospecimenUserExists(person.email);
    if(exists === false) {
        person['addedBy'] = email;
        person['addedAt'] = new Date().toISOString();
        person['siteCode'] = siteCode;
        const { addNewBiospecimenUser } = require('./firestore');
        await addNewBiospecimenUser(person);
    }
    else return new Error('User with this email already exists');
}

module.exports = {
    biospecimenAPIs
}