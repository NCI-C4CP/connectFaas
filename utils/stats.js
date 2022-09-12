const { getResponseJSON, setHeaders, logIPAdddress } = require('./shared');


const stats = async (req, res, authObj) => {
    logIPAdddress(req);
    setHeaders(res);

    if(req.method === 'OPTIONS') return res.status(200).json({code: 200});
    
    if(req.method !== 'GET') {
        return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
    }
    let obj = {};
    if(authObj) {
        obj = authObj;
    }
    else {
        const { APIAuthorization } = require('./shared');
        const authorized = await APIAuthorization(req);
        if(authorized instanceof Error){
            return res.status(401).json(getResponseJSON(authorized.message, 500));
        }
    
        if(!authorized){
            return res.status(401).json(getResponseJSON('Authorization failed!', 401));
        }
    
        const { isParentEntity } = require('./shared');
        obj = await isParentEntity(authorized);
    }
    
    const isParent = obj.isParent;
    const siteCodes = obj.siteCodes;
    
    if(!req.query.type) return res.status(404).json(getResponseJSON('Resource not found', 404));
    const type = req.query.type;
    console.log(`Stats type: ${type}`)
    console.log(`Retrieveing data for siteCodes: ${siteCodes}`)
    const { getTable } = require('./bigquery');
    let response
    if(type === 'race') response = await getTable('participants_race_count_by_sites', isParent, siteCodes);
    if(type === 'age') response = await getTable('participant_birthYear_by_siteCode', isParent, siteCodes );
    if(type === 'sex') response = await getTable('participants_sex_count_by_sites', isParent, siteCodes);

    if(type === 'participants_verification') response = await getTable('participants_verification_status', isParent, siteCodes);
    if(type === 'participants_workflow') response = await getTable('participants_workflow_status', isParent, siteCodes);

    if(type === 'participants_recruits_count') response = await getTable('participants_recruits_count', isParent, siteCodes);

    if(type === 'participants_optOuts') response = await getTable('participants_optOuts', isParent, siteCodes);

    if(type === 'participants_allModules') response = await getTable('participants_allModules', isParent, siteCodes);
    if(type === 'participants_moduleOne') response = await getTable('participants_moduleOne', isParent, siteCodes);
    if(type === 'participants_modulesTwoThree') response = await getTable('participants_modulesTwoThree', isParent, siteCodes);
    if(type === 'participants_allModulesAllSamples') response = await getTable('participants_allModulesAllSamples', isParent, siteCodes);
    if(type === 'participants_modulesNone') response = await getTable('participants_modulesNone', isParent, siteCodes);

    if(type === 'participants_ssn') response = await getTable('participants_ssn', isParent, siteCodes);
    if(type === 'participants_biospecimen') response = await getTable('participants_biospecimen', isParent, siteCodes);

    return res.status(200).json({stats: response, code:200});
}

module.exports = {
    stats
}