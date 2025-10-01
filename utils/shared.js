const {SecretManagerServiceClient} = require('@google-cloud/secret-manager');
const { QuerySnapshot } = require('firebase-admin/firestore');
const fieldMapping = require('./fieldToConceptIdMapping');

const getResponseJSON = (message, code) => {
    return { message, code };
};

const setHeaders = (res) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers','Accept,Content-Type,Content-Length,Accept-Encoding,X-CSRF-Token,Authorization');
    res.header('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS');
}

const setHeadersDomainRestricted = (req, res) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers','Accept,Content-Type,Content-Length,Accept-Encoding,X-CSRF-Token,Authorization');
    res.header('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS');
}

const generateConnectID = () => {
    return Math.floor(Math.random() * (9999999999 - 1000000000)) + 1000000000;
}

const generatePIN = () => {
    return Math.floor(Math.random() * (999999 - 100000)) + 100000;
}

const randomString = () => {
    const length = 6;
    let pin = '';
    const chars = '123456789ABCDEFGHJKLMNPQRSTUVWXYZ'
    for (let i = length; i > 0; --i) pin += chars[Math.round(Math.random() * (chars.length - 1))];
    return pin;
}

const deleteDocuments = (siteCode) => {
    if(!siteCode) return;
    const { deleteFirestoreDocuments } = require('./firestore')
    deleteFirestoreDocuments(siteCode)
    return true;
}

const lockedAttributes = [
                        "state", 
                        "token", 
                        "pin", 
                        "Connect_ID", 
                        "821247024", 
                        "230663853", 
                        "130371375",
                        "512820379",
                        "598680838",
                        "454067894",
                        "914639140",
                        "311580100",
                        "948195369",
                        "685002411", "906417725", "773707518", "747006172", "831041022", "269050420", "659990606", "664453818", "987563196", "123868967", "764403541", // Withdrawal concepts
                        "851245875", "919699172", "141450621", "576083042", "431428747", "121430614", "523768810", "639172801", "175732191", "637147033", "150818546", "624030581", "285488731", "596510649", "866089092", "990579614", "131458944", "372303208", "777719027", "620696506", "352891568", "958588520", "875010152", "404289911", "538619788", // Refusal concepts
                        "912301837",
                        "113579866",
                        
                    ] // Read only access after initialization

const incentiveFlags = {
    130371375 : { // Payment Round
        266600170: { // Baseline
            731498909: 104430631,
            648936790: 104430631,
            648228701: 104430631,
            222373868: 104430631,
            297462035: '',
            438636757: '',
            320023644: ''
        },
        496823485: { // Follow up 1
            731498909: 104430631,
            648936790: 104430631,
            648228701: 104430631,
            222373868: 104430631,
            297462035: '',
            438636757: '',
            320023644: ''
        },
        650465111: { // Follow up 2
            731498909: 104430631,
            648936790: 104430631,
            648228701: 104430631,
            222373868: 104430631,
            297462035: '',
            438636757: '',
            320023644: ''
        },
        303552867: { // Follow up 3
            731498909: 104430631,
            648936790: 104430631,
            648228701: 104430631,
            222373868: 104430631,
            297462035: '',
            438636757: '',
            320023644: ''
        }
    }
}

const refusalConcepts = {
	919699172: 104430631,
	141450621: 104430631,
	576083042: 104430631,
	431428747: 104430631,
	121430614: 104430631,
	523768810: 104430631,
	639172801: 104430631,
	175732191: 104430631,
	637147033: 104430631,
	150818546: 104430631,
	624030581: 104430631,
	285488731: 104430631,
	596510649: 104430631,
	866089092: 104430631,
	990579614: 104430631,
	131458944: 104430631,
	372303208: 104430631,
	777719027: 104430631,
	620696506: 104430631,
	352891568: 104430631,
	958588520: 104430631,
	875010152: 104430631,
	404289911: 104430631,
    734828170: 104430631,
	538619788: 104430631
}

const withdrawalConcepts = {
    685002411: {
        994064239: 104430631,
        194410742: 104430631,
        949501163: 104430631,
        277479354: 104430631,
        217367618: 104430631,
        867203506: 104430631,
        352996056: 104430631,
        941963821: 104430631,
        936015433: 104430631,
        688142378: 104430631,
        101763809: 104430631,
        525277409: 104430631,
        671903816: 104430631,
    },
    906417725: 104430631,
    773707518: 104430631,
    153713899: 104430631,
    747006172: 104430631,
    831041022: 104430631,
    359404406: 104430631,
    987563196: 104430631,
    861639549: 104430631,
    451953807: 104430631,
    123868967: '',
    113579866: '',
    659990606: '',
    269050420: '',
    664453818: '',
    ...refusalConcepts
}

const optOutReasons = {
    706283025: {
        196038514: 104430631,
        873405723: 104430631,
        517101990: 104430631,
        347614743: 104430631,
        535928798: 104430631,
        897366187: 104430631,
        415693436: '',
        719451909: 104430631,
        377633816: 104430631,
        211023960: 104430631,
        209509101: 104430631,
        363026564: 104430631,
        405352246: 104430631,
        755545718: 104430631,
        831137710: 104430631,
        496935183: 104430631,
        491099823: 104430631,
        836460125: 104430631,
        163534562: 104430631,
        331787113: 104430631,
        705732561: 104430631,
        381509125: 104430631,
        497530905: 104430631,
        627995442: 104430631,
        208102461: 104430631,
        579618065: 104430631,
        702433259: 104430631,
        771146804: 104430631,
        163284008: 104430631,
        387198193: 104430631,
        566047367: 104430631,
        400259098: 104430631,
        260703126: 104430631,
        744197145: 104430631,
        950040334: 104430631
    }
}

const defaultFlags = {
    948195369: 104430631,
    919254129: 104430631,
    821247024: 875007964,
    828729648: 104430631,
    699625233: 104430631,
    912301837: 208325815,
    253883960: 972455046,
    547363263: 972455046,
    949302066: 972455046,
    536735468: 972455046,
    976570371: 972455046,
    663265240: 972455046,
    265193023: 972455046,
    220186468: 972455046,
    320303124: 972455046,
    459098666: 972455046,
    126331570: 972455046,
    311580100: 104430631,
    914639140: 104430631,
    878865966: 104430631,
    167958071: 104430631,
    684635302: 104430631,
    100767870: 104430631,
    ...incentiveFlags,
    ...withdrawalConcepts
}

const defaultStateFlags = {
    875549268: 104430631,
    158291096: 104430631,
    ...optOutReasons
}

const moduleConceptsToCollections = {
    "D_726699695" :     "module1_v1",
    "D_726699695_V2" :  "module1_v2",
    "D_745268907" :     "module2_v1",
    "D_745268907_V2" :  "module2_v2",
    "D_965707586" :     "module3_v1",
    "D_716117817" :     "module4_v1",
    "D_299215535" :     "bioSurvey_v1",
    "D_793330426" :     "covid19Survey_v1",
    "D_912367929" :     "menstrualSurvey_v1",
    "D_826163434" :     "clinicalBioSurvey_v1",
    "D_166676176" :     "ssn",
    "D_390351864" :     "mouthwash_v1",
    "D_601305072" :     "promis_v1",
    "D_506648060" :     "experience2024",
    "D_369168474":      "cancerScreeningHistorySurvey",
    "D_497020618" :     "dhq3Survey",
};

const moduleStatusConcepts = {
    "949302066" :       "module1",
    "536735468" :       "module2",
    "976570371" :       "module3",
    "663265240" :       "module4",
    "265193023" :       "bioSurvey",
    "220186468" :       "covid19Survey",
    "459098666" :       "menstrualSurvey",
    "253883960" :       "clinicalBioSurvey",
    "126331570" :       "ssn",
    "547363263" :       "mouthwash",
    "320303124" :       "promis",
    "956490759" :       "experience2024",
    "176068627":        "cancerScreeningHistorySurvey",
    "497020618":        "dhq3Survey",
};

const listOfCollectionsRelatedToDataDestruction = [
    "bioSurvey_v1",
    "clinicalBioSurvey_v1",
    "covid19Survey_v1",
    "menstrualSurvey_v1",
    "module1_v1",
    "module1_v2",
    "module2_v1",
    "module2_v2",
    "module3_v1",
    "module4_v1",
    "notifications",
    "promis_v1",
    "mouthwash_v1",
    "ssn",
    "experience2024",
    "cancerScreeningHistorySurvey",
    "dhqAnalysisResults",
    "dhqDetailedAnalysis",
    "dhqRawAnswers",
    "cancerOccurrence",
];

const incentiveConcepts = {
    'baseline': '130371375.266600170',
    'followup1': '130371375.496823485',
    'followup2': '130371375.650465111',
    'followup3': '130371375.303552867',
    'incentiveIssued': 648936790,
    'incentiveIssuedAt': 297462035,
    'incentiveRefused': 648228701,
    'incentiveRefusedAt': 438636757,
    'caseNumber': 320023644,
    'incentiveChosen': 945795905
};

const conceptMappings = {
    'verified': 197316935,
    'cannotbeverified': 219863910,
    'duplicate': 922622075,
    'outreachtimedout': 160161595
};

const refusalWithdrawalConcepts = {
    "refusedBaselineBlood": "685002411.194410742",
    "refusedBaselineSpecimenSurvey": "685002411.217367618",
    "refusedBaselineSaliva": "685002411.277479354",
    "refusedFutureSamples": "685002411.352996056",
    "refusedFutureSurveys": "685002411.867203506",
    "refusedBaselineUrine": "685002411.949501163",
    "refusedBaselineSurveys": "685002411.994064239",
    "refusedFollowUpBloodCollection": "685002411.941963821",
    "refused2024ConnectExperienceSurveys": "685002411.101763809",
    "refusedAllFutureConnectExperienceSurveys": "685002411.525277409",
    "refusedQOL3moSurveys": "685002411.936015433",
    "refusedAllFutureQOLSurveys": "685002411.688142378",
    "refusedCanScreeningHistorySurvey": "685002411.671903816",

    "suspendedContact": "726389747",
    "withdrewConsent": "747006172",
    "revokeHIPAA": "773707518",
    "dataDestroyed": "831041022",
    "refusedFutureActivities": "906417725",
    "deceased": "987563196",

    "anyRefusalWithdrawal": "451953807"
}

const nihSSOConfig = {
    group: 'https://federation.nih.gov/person/DLGroups',
    firstName: 'https://federation.nih.gov/person/FirstName',
    lastName: 'https://federation.nih.gov/person/LastName',
    email: 'https://federation.nih.gov/person/Mail',
    siteManagerUser: 'CN=connect-study-manager-user',
    biospecimenUser: 'CN=connect-biospecimen-user',
    bptlUser: 'connect-bptl-user',
    helpDeskUser: 'CN=connect-help-desk-user',
    siteCode: 111111111,
    acronym: 'NIH'
}

const nihSSODevConfig = {
    group: 'https://federation.nih.gov/person/DLGroups',
    firstName: 'https://federation.nih.gov/person/FirstName',
    lastName: 'https://federation.nih.gov/person/LastName',
    email: 'https://federation.nih.gov/person/Mail',
    siteManagerUser: 'connect-study-manager-dev',
    biospecimenUser: 'connect-biospecimen-dev',
    bptlUser: 'connect-bptl-dev',
    helpDeskUser: 'CN=connect-help-desk-user',
    siteCode: 111111111,
    acronym: 'NIH'
}

const hpSSOConfig = {
    group: 'AD_groups',
    email: 'email',
    siteManagerUser: 'CN=connect-dshbrd-user',
    biospecimenUser: 'connect-biodshbrd-user',
    siteCode: 531629870,
    acronym: 'HP'
}

const sfhSSOConfig = {
    group: 'UserRole',
    email: 'UserEmail',
    siteManagerUser: 'Connect-Study-Manager-User',
    biospecimenUser: 'Connect-Study-Manager-User',
    siteCode: 657167265,
    acronym: 'SFH'
}

const hfhsSSOConfig = {
    group: 'http://schemas.microsoft.com/ws/2008/06/identity/claims/groups',
    firstName: 'http://schemas.xmlsoap.org/ws/2005/05/identity/claims/givenname',
    lastName: 'http://schemas.xmlsoap.org/ws/2005/05/identity/claims/surname',
    email: 'http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress',
    siteCode: 548392715,
    acronym: 'HFHS',
    siteManagerUser: 'study-manager-user',
    biospecimenUser: 'biospecimen-user'
}

const kpSSOConfig = {
    group: 'memberOf',
    firstName: 'givenName',
    email: 'userPrincipalName',
    siteManagerUser: 'CN=connect_study_manager_user',
    biospecimenUser: 'CN=connect_biospecimen_user',
    kpco: {
        name: 'CN=connect_kpco_user',
        siteCode: 125001209,
        acronym: 'KPCO'
    },
    kpnw: {
        name: 'CN=connect_kpnw_user',
        siteCode: 452412599,
        acronym: 'KPNW'
    },
    kphi: {
        name: 'CN=connect_kphi_user',
        siteCode: 300267574,
        acronym: 'KPHI'
    },
    kpga: {
        name: 'CN=connect_kpga_user',
        siteCode: 327912200,
        acronym: 'KPGA'
    }
}

const norcSSOConfig = {
    group: 'http://schemas.microsoft.com/ws/2008/06/identity/claims/groups',
    email: 'http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress',
    helpDeskUser: 'connect-help-desk-user',
    siteCode: 222222222,
    acronym: 'NORC'
}

const mfcSSOConfig = {
    siteCode: 303349821,
    acronym: 'MFC',
    firstName: 'firstName',
    lastName: 'lastName',
    email: 'http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress',
    group: 'http://schemas.microsoft.com/ws/2008/06/identity/claims/role',
    siteManagerUser: 'connect-study-manager-user',
    biospecimenUser: 'connect-biospecimen-user'
}

const ucmSSOConfig = {
    group: '1.3.6.1.4.1.9902.2.1.41',
    firstName: 'urn:oid:0.9.2342.19200300.100.1.1',
    email: 'urn:oid:1.3.6.1.4.1.5923.1.1.1.6',
    siteManagerUser: 'uc:org:bsd:applications:connect:connect-study-manager-user:authorized',
    biospecimenUser: 'uc:org:bsd:applications:connect:connect-biospecimen-user:authorized',
    siteCode: 809703864,
    acronym: 'UCM'
}

const bswhSSOConfig = {
    siteCode: 472940358,
    acronym: 'BSWH',
    firstName: 'First Name',
    lastName: 'Last Name',
    email: 'Email Address',
    group: 'Groups',
    siteManagerUser: 'Research_Connect_FC',
    biospecimenUser: 'Research_Connect_FC'
}

const SSOConfig = {
    'NIH-SSO-qfszp': nihSSODevConfig,
    'NIH-SSO-9q2ao': nihSSODevConfig,
    'NIH-SSO-wthvn': nihSSOConfig,

    'HP-SSO-wb1zb': hpSSOConfig,
    'HP-SSO-1elez': hpSSOConfig,
    'HP-SSO-252sf': hpSSOConfig,

    'SFH-SSO-cgzpj': sfhSSOConfig,
    'SFH-SSO-uetfo': sfhSSOConfig,
    'SFH-SSO-pb390': sfhSSOConfig,

    'HFHS-SSO-ay0iz': hfhsSSOConfig,
    'HFHS-SSO-eq1fj': hfhsSSOConfig,
    'HFHS-SSO-lo99j': hfhsSSOConfig,

    'KP-SSO-wulix': kpSSOConfig,
    'KP-SSO-ssj7c': kpSSOConfig,
    'KP-SSO-ii9sr': kpSSOConfig,

    'NORC-SSO-dilvf': norcSSOConfig,
    'NORC-SSO-l80az': norcSSOConfig,
    'NORC-SSO-nwvau': norcSSOConfig,

    'MFC-SSO-fljvd': mfcSSOConfig,
    'MFC-SSO-6x4zy': mfcSSOConfig,
    'MFC-SSO-tdj17': mfcSSOConfig,

    'UCM-SSO-tovai': ucmSSOConfig,
    'UCM-SSO-lrjsp': ucmSSOConfig,
    'UCM-SSO-p4f5m': ucmSSOConfig,

    'BSWH-SSO-y2jj3': bswhSSOConfig,
    'BSWH-SSO-k4cat': bswhSSOConfig,
    'BSWH-SSO-dcoos': bswhSSOConfig,
}

// https://www.twilio.com/docs/messaging/guides/debugging-tools#error-codes
const twilioErrorMessages = {
    30001: "Queue overflow. You tried to send too many messages too quickly, and your message queue overflowed. Try sending your message again after waiting for some time.",
    30002: "Account suspended. Your account was suspended between the time of message send and delivery. Please contact Twilio.",
    30003: "Unreachable destination handset. The destination handset you are trying to reach is switched off or otherwise unavailable.",
    30004: "Message blocked. The destination number you are trying to reach is blocked from receiving this message (e.g., due to blacklisting).",
    30005: "Unknown destination handset. The destination number you are trying to reach is unknown and may no longer exist.",
    30006: "Landline or unreachable carrier. The destination number is unable to receive this message. Potential reasons could include trying to reach a landline or, in the case of short codes, an unreachable carrier.",
    30007: "Carrier violation. Your message was flagged as objectionable by the carrier. To protect their subscribers, many carriers have implemented content or spam filtering.",
    30008: "Unknown error. The error does not fit into any of the above categories.",
    30009: "Missing segment. One or more segments associated with your multi-part inbound message was not received.",
    300010: "Message price exceeds max price. The price of your message exceeds the max price parameter.",
};

const decodingJWT = (token) => {
    if (token) {
        const base64String = token.split('.')[1];
        const decodedValue = JSON.parse(Buffer.from(base64String, 'base64').toString());
        return decodedValue;
    }
    return null;
};

const SSOValidation = async (dashboardType, idToken) => {
    try {
        const decodedJWT = decodingJWT(idToken);
        const tenant = decodedJWT.firebase.tenant;
        const { validateMultiTenantIDToken } = require('./firestore');
        const decodedToken = await validateMultiTenantIDToken(idToken, tenant);

        console.log("Decoded Token:", decodedToken);

        if(decodedToken instanceof Error) {
            return false;
        }

        const allGroups = decodedToken.firebase.sign_in_attributes[SSOConfig[tenant]['group']];

        console.log("All Groups:", allGroups);

        if(!allGroups) return;
        const email = decodedToken.firebase.sign_in_attributes[SSOConfig[tenant]['email']];

        console.log("Email:", email);

        console.log("Attributes:", decodedToken.firebase.sign_in_attributes);

        if(!SSOConfig[tenant][dashboardType]) return false;
        let requiredGroups = new RegExp(SSOConfig[tenant][dashboardType], 'g').test(allGroups.toString());
        let isBiospecimenUser = false;
        if(requiredGroups) isBiospecimenUser = true;
        let isBPTLUser = false;
        if(SSOConfig[tenant].acronym === 'NIH') {
            isBPTLUser = new RegExp(SSOConfig[tenant]['bptlUser'], 'g').test(allGroups.toString())
            requiredGroups = requiredGroups || isBPTLUser;
        }
        if(!requiredGroups) return false;
        let acronym = SSOConfig[tenant].acronym;
        if(tenant === 'KP-SSO-wulix' || tenant === 'KP-SSO-ssj7c' || tenant === 'KP-SSO-ii9sr') {
            const moreThanOneRegion = allGroups.toString().match(/CN=connect_kp(co|hi|nw|ga)_user/ig);
            if(moreThanOneRegion.length > 1) return false;
            if(new RegExp(SSOConfig[tenant]['kpco']['name'], 'g').test(allGroups.toString())) acronym = SSOConfig[tenant]['kpco']['acronym'];
            if(new RegExp(SSOConfig[tenant]['kpga']['name'], 'g').test(allGroups.toString())) acronym = SSOConfig[tenant]['kpga']['acronym'];
            if(new RegExp(SSOConfig[tenant]['kphi']['name'], 'g').test(allGroups.toString())) acronym = SSOConfig[tenant]['kphi']['acronym'];
            if(new RegExp(SSOConfig[tenant]['kpnw']['name'], 'g').test(allGroups.toString())) acronym = SSOConfig[tenant]['kpnw']['acronym'];
            if(!acronym) return false;
        }

        const { getSiteDetailsWithSignInProvider } = require('./firestore');
        const siteDetails = await getSiteDetailsWithSignInProvider(acronym);

        console.log("Results in SSOValidation():");
        console.log("Email: " + email);
        console.log("BPTL User: " + isBPTLUser);
        console.log("BSD User: " + isBiospecimenUser);
        return {siteDetails, email, isBPTLUser, isBiospecimenUser};
    } catch (error) {
        return false;
    }
}

const APIAuthorization = async (req) => {
    
    if(!req.headers.authorization || req.headers.authorization.trim() === "" || req.headers.authorization.replace('Bearer ','').trim() === ""){
        return false;
    }

    let authorized = false;

    try {
        const {google} = require("googleapis");

        const OAuth2 = google.auth.OAuth2;
        const oauth2Client = new OAuth2();
        const access_token = req.headers.authorization.replace('Bearer ','').trim();

        oauth2Client.setCredentials({access_token: access_token});

        const oauth2 = await google.oauth2({
            auth: oauth2Client,
            version: 'v2'
        });

        const response = await oauth2.userinfo.get();
        if(response.status === 200) {
            const saEmail = response.data.email;
            const { validateSiteSAEmail } = require(`./firestore`);

            authorized = await validateSiteSAEmail(saEmail);

            if(authorized instanceof Error) {
                return new Error(authorized)
            }

            if(authorized) {
                return authorized;
            }
        }

        return false;

    } catch (error) {
        if(error.code === 401) return false;
        else return new Error(error)
    }
}

const isParentEntity = async (siteDetails) => {
    const { getChildren } = require('./firestore');

    const id = siteDetails.id;
    let siteCodes = await getChildren(id);
    siteCodes = siteCodes.length > 0 ? siteCodes : siteDetails.siteCode;
    const isParent = siteCodes.length > 0;

    return {...siteDetails, isParent, siteCodes};
};

const logIPAddress = (req) => {
    const ipAddress = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    console.log(ipAddress)
}

const initializeTimestamps = {
    "state.158291096": {
        value: 353358909,
        initialize: {
            "state.158291096": 353358909,
            "state.697256759": new Date().toISOString()
        }
    }
}

const tubeKeytoNum = {
    299553921: '0001',
    703954371: '0002',
    838567176: '0003',
    454453939: '0004',
    652357376: '0005',
    973670172: '0006',
    143615646: '0007',
    787237543: '0008',
    223999569: '0009',
    376960806: '0011',
    232343615: '0012',
    589588440: '0021',
    958646668: '0013',
    677469051: '0014',
    683613884: '0024',
    505347689: '0060',
  };

const collectionIdConversion = {
    "0007": "143615646",
    "0009": "223999569",
    "0012": "232343615",
    "0001": "299553921",
    "0011": "376960806",
    "0004": "454453939",
    "0021": "589588440",
    "0005": "652357376",
    "0032": "654812257",
    "0014": "677469051",
    "0024": "683613884",
    "0002": "703954371",
    "0022": "746999767",
    "0008": "787237543",
    "0003": "838567176",
    "0031": "857757831",
    "0013": "958646668",
    "0006": "973670172",
    "0060": "505347689",
}

const sites = {
  HP: { siteCode: '531629870', locations: ['834825425'] },
  HFHS: {
    siteCode: '548392715',
    locations: ['752948709', '570271641', '838480167'],
  },
  KPCO: { siteCode: '125001209', locations: ['763273112'] },
  KPGA: { siteCode: '327912200', locations: ['767775934'] },
  KPHI: { siteCode: '300267574', locations: ['531313956'] },
  KPNW: { siteCode: '452412599', locations: ['715632875'] },
  MFC: { siteCode: '303349821', locations: ['692275326'] },
  SFH: { siteCode: '657167265', locations: ['589224449'] },
  UCM: { siteCode: '809703864', locations: ['333333333'] },
  NIH: { siteCode: '13', locations: ['111111111', '222222222'] },
};
  
const bagConceptIDs = [
  '650224161', // bag1
  '136341211', // bag2
  '503046679', // bag3
  '313341808', // bag4
  '668816010', // bag5
  '754614551', // bag6
  '174264982', // bag7
  '550020510', // bag8
  '673090642', // bag9
  '492881559', // bag10
  '536728814', // bag11
  '309413330', // bag12
  '357218702', // bag13
  '945294744', // bag14
  '741697447', // bag15
  '125739724', // bag16
  '989380048', // bag17
  '446995300', // bag18
  '137286816', // bag19
  '977670846', // bag20
  '563435337', // bag21
  '807530964', // bag22
  '898078094', // bag23
  '866824332', // bag24
  '456471969', // bag25
  '288387838', // bag26
  '335054951', // bag27
  '235683703', // bag28
  '390934489', // bag29
  '753716110', // bag30
  '669598671', // bag31
  '699864022', // bag32
  '986589527', // bag33
  '417623038', // bag34
  '725915890', // bag35
  '956354350', // bag36
  '925165180', // bag37
  '832614280', // bag38
  '301569492', // bag39
  '685888031', // bag40
];

const checkDefaultFlags = async (data, uid) => {
  
    if(!data) return {};
  
    let missingDefaults = {};
  
    Object.entries(defaultFlags).forEach(item => {
      if(!data[item[0]]) {
        missingDefaults[item[0]] = item[1];
      }
    });

    lockedAttributes.forEach(atr => delete missingDefaults[atr]);

    if(Object.entries(missingDefaults).length != 0) {
       
        const { updateResponse } = require('./firestore');
        const response = await updateResponse(missingDefaults, uid);
        if(response instanceof Error){
            return response;
        }

        return true;
    }
  
    return false;
}

const cleanSurveyData = (data) => {

    const admin = require('firebase-admin');
    
    Object.keys(data).forEach(key => {
        if (data[key] === null || data[key] === undefined) {
            data[key] = admin.firestore.FieldValue.delete();
        }
    });

    return data;
}

/**
 * Gets baseline data updates for participants when submitting specimens
 * @param {object} biospecimenData The biospecimen data
 * @param {object} participantData The participant data
 * @param {array} siteTubesList The array of tubes used for the site
 * @returns {object} - The participant updates.
 */

const updateBaselineData = (biospecimenData, participantData, siteTubesList) => {
    let participantUpdates = {};
    let settings = {};
    let visit = biospecimenData[fieldMapping.collectionSelectedVisit];

    // Code Scanning Alert Fix: Prototype-polluting Assignment
    // https://github.com/NCI-C4CP/connectFaas/security/code-scanning/1
    if (visit === '__proto__' || visit === 'constructor' || visit === 'prototype') {
        throw new Error('Validation to prevent prototype pollution failed.');
    }

    // Now we potentially need to updateBaselineData
    const baselineVisit = (biospecimenData[fieldMapping.collectionSelectedVisit] === fieldMapping.baseline);
    const clinicalResearchSetting = (biospecimenData[fieldMapping.collectionSetting] === fieldMapping.research || biospecimenData[fieldMapping.collectionSetting] === fieldMapping.clinical);
    if (baselineVisit && clinicalResearchSetting) {
        // Update baseline data
        const baselineCollections = [biospecimenData].filter(specimen => specimen[fieldMapping.collectionSelectedVisit] === fieldMapping.baseline);

        const bloodTubes = siteTubesList.filter(tube => tube.tubeType === "Blood tube");
        const urineTubes = siteTubesList.filter(tube => tube.tubeType === "Urine");
        const mouthwashTubes = siteTubesList.filter(tube => tube.tubeType === "Mouthwash");

        let bloodTubesLength = 0
        let urineTubesLength = 0
        let mouthwashTubesLength = 0

        const collectionSetting = biospecimenData[fieldMapping.collectionSetting];
        const isResearch = collectionSetting === fieldMapping.research;
        const isClinical = collectionSetting === fieldMapping.clinical;

        // Build the collection details
        if (participantData[fieldMapping.collectionDetails]) {
            settings = participantData[fieldMapping.collectionDetails];
            if (!settings[visit]) settings[visit] = {};

        } else {
            settings = {
                [visit]: {}
            }
        }

        if (!settings[visit][fieldMapping.bloodCollectionSetting]) {
            bloodTubes.forEach(tube => {
                const tubeIsCollected = biospecimenData[tube.concept][fieldMapping.tubeIsCollected] === fieldMapping.yes;
                if(tubeIsCollected) {
                    settings[visit][fieldMapping.bloodCollectionSetting] = collectionSetting;
                    if(isResearch) {
                        // Prevent overwriting; always use the time from the first submission
                        if(!settings[visit][fieldMapping.baselineBloodCollectedTime]) {
                            settings[visit][fieldMapping.baselineBloodCollectedTime] = biospecimenData[fieldMapping.collectionDateTimeStamp];
                        }
                    }
                    else if(isClinical) {
                        settings[visit][fieldMapping.clinicalBloodCollected] = fieldMapping.yes;
                        if(!settings[visit][fieldMapping.clinicalBloodCollectedTime]) {
                            // Prevent overwriting; always use the time from the first submission
                            settings[visit][fieldMapping.clinicalBloodCollectedTime] = biospecimenData[fieldMapping.collectionScannedTime];
                        }

                        settings[visit][fieldMapping.anySpecimenCollected] = fieldMapping.yes;

                        if(!(settings[visit][fieldMapping.anySpecimenCollectedTime])) {
                            settings[visit][fieldMapping.anySpecimenCollectedTime] = biospecimenData[fieldMapping.collectionScannedTime];
                        }
                    }
                    bloodTubesLength += 1
                }
            });
        }
        // This section has been commented out as a part of implementing 1169 and 1062
        // according to the understanding that these values should not be changed once finalized,
        // and are now set only on finalization.
        // If you need to restore this logic, please review 1169 and 1062 and find alternate implementations.
        // 1169 - Update to when derived variables are set and preventing overwriting (Biospecimen): https://github.com/episphere/connect/issues/1169
        // 1062 - Handling data from multiple baseline collections: https://github.com/episphere/connect/issues/1062
        /*
        else if (settings[visit][fieldMapping.baselineBloodCollectedTime] !== '' ||  settings[visit][fieldMapping.clinicalBloodCollectedTime] !== ''){
            const participantBloodCollected = participantData[fieldMapping.baselineBloodSampleCollected] === fieldMapping.yes;
            const totalBloodTubesAvail = bloodTubes.filter((tube) => biospecimenData[tube.concept][fieldMapping.tubeIsCollected] === fieldMapping.yes);
            if (totalBloodTubesAvail.length === 0 && participantBloodCollected) {
                delete settings[visit][fieldMapping.bloodCollectionSetting]; // derived variables & timestamp are updated only if all the blood tubes are unchecked
                if (isResearch) {
                    delete settings[visit][fieldMapping.baselineBloodCollectedTime];
                }
                else if (isClinical) {
                    settings[visit][fieldMapping.clinicalBloodCollected] = fieldMapping.no;
                    delete settings[visit][fieldMapping.clinicalBloodCollectedTime];

                    if (urineTubesLength === 0 && mouthwashTubesLength === 0) { // anySpecimenCollected variable will only be updated to NO if mouthwash & urine specimens are not present.
                        settings[visit][fieldMapping.anySpecimenCollected] = fieldMapping.no;
                        if (!(settings[visit][fieldMapping.anySpecimenCollectedTime])) {
                            delete settings[visit][fieldMapping.anySpecimenCollectedTime];
                        }
                    }
                }
                participantUpdates[fieldMapping.baselineBloodSampleCollected] = fieldMapping.no;
                bloodTubesLength = totalBloodTubesAvail.length;
            }
        }
        */

        if (!settings[visit][fieldMapping.urineCollectionSetting]) {
            urineTubes.forEach(tube => {
                const tubeIsCollected = biospecimenData[tube.concept][fieldMapping.tubeIsCollected] === fieldMapping.yes;
                if (tubeIsCollected) {
                    settings[visit][fieldMapping.urineCollectionSetting] = collectionSetting;
                    if (isResearch) {
                        // Prevent overwriting; always use the time from the first submission
                        if(!settings[visit][fieldMapping.baselineUrineCollectedTime]) {
                            settings[visit][fieldMapping.baselineUrineCollectedTime] = biospecimenData[fieldMapping.collectionDateTimeStamp];
                        }
                    }
                    else if (isClinical) {
                        settings[visit][fieldMapping.clinicalUrineCollected] = fieldMapping.yes;
                        settings[visit][fieldMapping.clinicalUrineCollectedTime] = biospecimenData[fieldMapping.collectionScannedTime];

                        settings[visit][fieldMapping.anySpecimenCollected] = fieldMapping.yes;

                        // prevent overwriting; always use the time from the first submission
                        if (!(settings[visit][fieldMapping.anySpecimenCollectedTime])) {
                            settings[visit][fieldMapping.anySpecimenCollectedTime] = biospecimenData[fieldMapping.collectionScannedTime];
                        }
                    }
                    urineTubesLength += 1
                }
            });
        }
         // This section has been commented out as a part of implementing 1169 and 1062
        // according to the understanding that these values should not be changed once finalized,
        // and are now set only on finalization.
        // If you need to restore this logic, please review 1169 and 1062 and find alternate implementations.
        // 1169 - Update to when derived variables are set and preventing overwriting (Biospecimen): https://github.com/episphere/connect/issues/1169
        // 1062 - Handling data from multiple baseline collections: https://github.com/episphere/connect/issues/1062
        /*
        else if (settings[visit][fieldMapping.baselineUrineCollectedTime] !== '' ||  settings[visit][fieldMapping.clinicalUrineCollectedTime] !== '') {
            const participantUrineCollected = participantData[fieldMapping.baselineUrineCollected] === fieldMapping.yes;
            const totalUrineTubesAvail = urineTubes.filter((tube) => biospecimenData[tube.concept][fieldMapping.tubeIsCollected] === fieldMapping.yes);
            if (totalUrineTubesAvail.length === 0 && participantUrineCollected) {
                delete settings[visit][fieldMapping.urineCollectionSetting];
                if(isResearch) {
                    delete settings[visit][fieldMapping.baselineUrineCollectedTime];
                }
                else if (isClinical) {
                    settings[visit][fieldMapping.clinicalUrineCollected] = fieldMapping.no;
                    delete settings[visit][fieldMapping.clinicalUrineCollectedTime];

                    if (bloodTubesLength === 0 && mouthwashTubesLength === 0) { // anySpecimenCollected variable will only be updated to NO if mouthwash & blood specimens are not present.
                        settings[visit][fieldMapping.anySpecimenCollected] = fieldMapping.no;
                        if (!(settings[visit][fieldMapping.anySpecimenCollectedTime])) {
                            delete settings[visit][fieldMapping.anySpecimenCollectedTime];
                        }
                    }
                }
                participantUpdates[fieldMapping.baselineUrineCollected] = fieldMapping.no;
                urineTubesLength = totalUrineTubesAvail.length;
            }  
        }
        */

        if (!settings[visit][fieldMapping.mouthwashCollectionSetting]) {
            mouthwashTubes.forEach(tube => {
                const isTubeCollected = biospecimenData[tube.concept][fieldMapping.tubeIsCollected] === fieldMapping.yes;
                if (isTubeCollected) {
                    settings[visit][fieldMapping.mouthwashCollectionSetting] = collectionSetting;
                    if (isResearch) {
                        // Prevent overwriting; always use the time from the first submission
                        if(!settings[visit][fieldMapping.baselineMouthwashCollectedTime]) {
                            settings[visit][fieldMapping.baselineMouthwashCollectedTime] = biospecimenData[fieldMapping.collectionDateTimeStamp];
                        }
                    }
                mouthwashTubesLength += 1
                }
            });
        }
        // This section has been commented out as a part of implementing 1169 and 1062
        // according to the understanding that these values should not be changed once finalized,
        // and are now set only on finalization.
        // If you need to restore this logic, please review 1169 and 1062 and find alternate implementations.
        // 1169 - Update to when derived variables are set and preventing overwriting (Biospecimen): https://github.com/episphere/connect/issues/1169
        // 1062 - Handling data from multiple baseline collections: https://github.com/episphere/connect/issues/1062
        /*
        else if (settings[visit][fieldMapping.baselineMouthwashCollectedTime] !== '' && participantData[fieldMapping.baselineMouthwashCollected] === fieldMapping.yes) {
            const isParticipantMouthwashCollected = participantData[fieldMapping.baselineMouthwashCollected] === fieldMapping.yes;
            const totalMouthwasTubesAvail = mouthwashTubes.filter((tube) => biospecimenData[tube.concept][fieldMapping.tubeIsCollected] === fieldMapping.yes);
            if (totalMouthwasTubesAvail.length === 0 &&  isParticipantMouthwashCollected) {
                delete settings[visit][fieldMapping.mouthwashCollectionSetting]
                if (isResearch) {
                    delete settings[visit][fieldMapping.baselineMouthwashCollectedTime];
                }
                participantUpdates[fieldMapping.baselineMouthwashCollected] = fieldMapping.no;
                mouthwashTubesLength = totalMouthwasTubesAvail.length;
            }
        }
        */

        participantUpdates[fieldMapping.collectionDetails] = settings;

        // Spread in the blood, urine, and mouthwash collected flags for cases where they were calculated above.
        participantData = { ...participantData, ...participantUpdates };
        let bloodCollected = (participantData[fieldMapping.baselineBloodSampleCollected] === fieldMapping.yes);
        let urineCollected = (participantData[fieldMapping.baselineUrineCollected] === fieldMapping.yes);
        let mouthwashCollected = (participantData[fieldMapping.baselineMouthwashCollected] === fieldMapping.yes);
        let allBaselineCollected = (participantData[fieldMapping.allBaselineSamplesCollected] === fieldMapping.yes);

        baselineCollections.forEach(collection => {

            if (!bloodCollected || bloodCollected === fieldMapping.no) {
                bloodTubes.forEach(tube => {
                    if (collection[tube.concept]?.[fieldMapping.tubeIsCollected] === 353358909) {
                        bloodCollected = true;
                    }
                });
            } 
            if (!urineCollected || urineCollected === fieldMapping.no) {
                urineTubes.forEach(tube => {
                    if (collection[tube.concept]?.[fieldMapping.tubeIsCollected] === 353358909) {
                        urineCollected = true;
                    }
                });
            }
            if (!mouthwashCollected || mouthwashCollected === fieldMapping.no) {
                mouthwashTubes.forEach(tube => {
                    if (collection[tube.concept]?.[fieldMapping.tubeIsCollected] === 353358909) {
                        mouthwashCollected = true;
                    }
                });
            }

        });

        if (baselineCollections.length > 0 && baselineCollections[0][fieldMapping.collectionSetting] === fieldMapping.research) {
            allBaselineCollected = bloodCollected && urineCollected && mouthwashCollected;
        }
        else if (baselineCollections.length > 0 && baselineCollections[0][fieldMapping.collectionSetting] === fieldMapping.clinical) {
            allBaselineCollected = bloodCollected && urineCollected;
        }

        participantUpdates = {
            ...participantUpdates,
            [fieldMapping.baselineBloodSampleCollected]: bloodCollected ? fieldMapping.yes : fieldMapping.no,
            [fieldMapping.baselineUrineCollected]: urineCollected ? fieldMapping.yes : fieldMapping.no,
            [fieldMapping.baselineMouthwashCollected]: mouthwashCollected ? fieldMapping.yes : fieldMapping.no,
            [fieldMapping.allBaselineSamplesCollected]: allBaselineCollected ? fieldMapping.yes : fieldMapping.no,
        };

    }
    return participantUpdates
}

/**
 * Matches function used on front end in biospecimen
 * Used to determine stray tubes being added to an already finalized collection
 * 
 * @param {object} originalSpecimenData Original biospecimen object
 * @param {object} currentSpecimenData Incoming biospecimen object updates
 * @returns {array} Array of stray tube IDs
 */
const getAddedStrayTubes = (originalSpecimenData, currentSpecimenData) => {
    const tubeCidList = [
    '299553921',
    '703954371',
    '838567176',
    '454453939',
    '652357376',
    '973670172',
    '143615646',
    // '787237543' and '223999569' are used as containers for tubes
    '376960806',
    '232343615',
    '589588440',
    '958646668',
    '677469051',
    '683613884',
    '505347689',
  ];
    const originalCollectedTubesSet = new Set(
        Object.keys(originalSpecimenData).filter(key =>
            tubeCidList.includes(key) && originalSpecimenData[key][fieldMapping.tubeIsCollected] === fieldMapping.yes
        )
    );

    return Object.keys(currentSpecimenData).filter(key =>
        tubeCidList.includes(key) &&
        currentSpecimenData[key][fieldMapping.tubeIsCollected] === fieldMapping.yes &&
        !originalCollectedTubesSet.has(key)).map(tubeKey => currentSpecimenData[tubeKey][fieldMapping.objectId]
    );
}

/**
 * Broken into a separate helper function for easier testing
 */
const getHomeMWKitData = (data) => {
    const {
        collectionDetails, baseline, bioKitMouthwash, bioKitMouthwashBL1, bioKitMouthwashBL2, 
        kitType, mouthwashKit, dateKitRequested, kitStatus,
        pending, initialized, addressUndeliverable, addressPrinted, assigned, shipped, received,
        bloodOrUrineCollectedTimestamp
    } = fieldMapping;
    let fieldPath = `${collectionDetails}.${baseline}.${bioKitMouthwash}`;
    let updatedParticipantObject = {};

    const participantIsEligible = !!processParticipantHomeMouthwashKitData(data, true);

    if(!participantIsEligible) {
        throw new Error('Participant address information is invalid.');
    }
    
    if(data?.[collectionDetails]?.[baseline]?.[bioKitMouthwashBL2]?.[kitStatus] === addressUndeliverable) {
        // If their R2 address is marked as undeliverable, change it to initialized.
        fieldPath = `${collectionDetails}.${baseline}.${bioKitMouthwashBL2}`;
    } else if(data?.[collectionDetails]?.[baseline]?.[bioKitMouthwashBL2]) {
        // If two replacements otherwise, they are out of replacement kits; error out.
        throw new Error('Participant has exceeded supported number of replacement kits.');
    } else if(data?.[collectionDetails]?.[baseline]?.[bioKitMouthwashBL1]) {
        // If one replacement, mark as eligible for second replacement
        switch(data[collectionDetails]?.[baseline]?.[bioKitMouthwashBL1][kitStatus]) {
            case undefined:
            case null:
            case pending: {
                throw new Error('This participant is not eligible for a second replacement home mouthwash kit');
            }
            case initialized:
            case addressPrinted:
            case assigned: 
            {
                throw new Error('This participant\'s first replacement home mouthwash kit has not been sent');
            }
            case addressUndeliverable:
            {
                // addressUndeliverable being passed in here means that
                // the address has been corrected and the status should be updated
                // to initialized
                fieldPath = `${collectionDetails}.${baseline}.${bioKitMouthwashBL1}`;
                break;
            }
            case shipped:
            case received:
                // Eligible for second replacement
                fieldPath = `${collectionDetails}.${baseline}.${bioKitMouthwashBL2}`;
                break;
            default:
                throw new Error('Unrecognized kit status ' + data[collectionDetails]?.[baseline]?.[bioKitMouthwashBL1][kitStatus]);
        }
    } else {
        switch(data?.[collectionDetails]?.[baseline]?.[bioKitMouthwash]?.[kitStatus]) {
            // Initial kit request
            case undefined:
            case null:
            case pending: {
                // If this is an initial kit request,
                // and if bloodOrUrineCollectedTimestamp is not present
                // it needs to be explicitly set to null
                // otherwise the == null filtering will not work on it in Firestore
                // This does not need to be set retroactively, only going forward.
                if(!data?.[collectionDetails]?.[baseline]?.[bloodOrUrineCollectedTimestamp]) {
                    updatedParticipantObject[`${collectionDetails}.${baseline}.${bloodOrUrineCollectedTimestamp}`] = null;
                }
                fieldPath = `${collectionDetails}.${baseline}.${bioKitMouthwash}`;
                break;
            }
            
            case initialized:
            case addressPrinted:
            case assigned: 
            {
                throw new Error('This participant\'s initial home mouthwash kit has not been sent');
            }
            case addressUndeliverable:
            {
                // addressUndeliverable being passed in here means that
                // the address has been corrected and the status should be updated
                // to initialized
                break;
            }
            case shipped:
            case received:
                // Eligible for first replacement
                fieldPath = `${collectionDetails}.${baseline}.${bioKitMouthwashBL1}`;
                break;
            default:
                throw new Error('Unrecognized kit status ' + data[collectionDetails]?.[baseline]?.[bioKitMouthwash][kitStatus]);
        }
    }

    const requestedDT = new Date().toISOString();

    updatedParticipantObject = {...updatedParticipantObject,
        [[fieldPath, kitType].join('.')]: mouthwashKit,
        [[fieldPath, kitStatus].join('.')]: initialized,
        [[fieldPath, dateKitRequested].join('.')]: requestedDT
    };

    return updatedParticipantObject;
}

const replacementKitSort = (aData, bData) => {
    const { bioKitMouthwashBL2, bioKitMouthwashBL1, collectionDetails, baseline, dateKitRequested } = fieldMapping;
    const aDate = aData?.[collectionDetails]?.[baseline]?.[bioKitMouthwashBL2]?.[dateKitRequested] || aData?.[collectionDetails]?.[baseline]?.[bioKitMouthwashBL1]?.[dateKitRequested];
    const bDate = bData?.[collectionDetails]?.[baseline]?.[bioKitMouthwashBL2]?.[dateKitRequested] || bData?.[collectionDetails]?.[baseline]?.[bioKitMouthwashBL1]?.[dateKitRequested];
    if(aDate === bDate) {
        return 0;
    }
    return aDate < bDate ? -1 : 1; // Oldest to newest
}

const manualRequestSort = (aData, bData) => {
    const { bioKitMouthwash, collectionDetails, baseline, dateKitRequested } = fieldMapping;
    const aDate = aData?.[collectionDetails]?.[baseline]?.[bioKitMouthwash]?.[dateKitRequested];
    const bDate = bData?.[collectionDetails]?.[baseline]?.[bioKitMouthwash]?.[dateKitRequested];
    if(aDate === bDate) {
        return 0;
    }
    return aDate < bDate ? -1 : 1; // Oldest to newest
}

const standardHomeKitSort = (aData, bData) => {
    const { collectionDetails, baseline, bloodOrUrineCollectedTimestamp } = fieldMapping;
    const aDate = aData?.[collectionDetails]?.[baseline]?.[bloodOrUrineCollectedTimestamp];
    const bDate = bData?.[collectionDetails]?.[baseline]?.[bloodOrUrineCollectedTimestamp];
    if(aDate === bDate) {
        return 0;
    }
    return aDate > bDate ? -1 : 1; // Newest to oldest
}

// Note: existing snake_casing follows through to BPTL CSV reporting. Do not update to camelCase without prior communication.
const processParticipantHomeMouthwashKitData = (record, printLabel, includePOBoxes) => {
    const { collectionDetails, baseline, bioKitMouthwash, bioKitMouthwashBL1, bioKitMouthwashBL2,
        firstName, lastName, 
        isPOBox, address1, address2, 
        physicalAddress1, physicalAddress2, city, state, zip, physicalCity, physicalState, physicalZip, 
        yes, dateKitRequested, bloodOrUrineCollectedTimestamp } = fieldMapping;

    // If you need to read additional fields off of this record, make sure that the field is added to participantHomeCollectionKitFields
    // or else it will not be selected when determining participants eligible for kit assignment

    if(!record) {
        return null;
    }

    const poBoxRegex = /^(?:P\.?\s*O\.?\s*(?:Box|B\.?)?|Post\s+Office\s+(?:Box|B\.?)?)\s*(\s*#?\s*\d*)((?:\s+(.+))?$)$/i;

    const physicalAddressLineOne = record[physicalAddress1];
    let addressObj = {};

    // If there is a physical address, default to it unless it's a PO Box
    // (Behavior clarified in the notes on [1174](https://github.com/episphere/connect/issues/1174))
    if(physicalAddressLineOne && !poBoxRegex.test(physicalAddressLineOne)) {
        addressObj = {
            address_1: record[physicalAddress1],
            address_2: record[physicalAddress2] || '',
            city: record[physicalCity],
            state: record[physicalState],
            zip_code: record[physicalZip], 
        };
    } else {
        const addressLineOne = record?.[address1];
        const isPOBoxMatch = poBoxRegex.test(addressLineOne) || record?.[isPOBox] === yes;
        if(isPOBoxMatch && !includePOBoxes) {
            return null;
        }
        addressObj = {
            address_1: record[address1],
            address_2: record[address2] || '',
            city: record[city],
            state: record[state],
            zip_code: record[zip]
        };
    }

    const hasMouthwash = record?.[collectionDetails]?.[baseline]?.[bioKitMouthwash] !== undefined;    
    // First of all, determine which visit it is
    let visit = 'BL';
    let requestDate = record?.[collectionDetails]?.[baseline]?.[bioKitMouthwash]?.[dateKitRequested] || record[collectionDetails]?.[baseline]?.[bioKitMouthwash]?.[bloodOrUrineCollectedTimestamp];
    const bl2Record = record?.[collectionDetails]?.[baseline]?.[bioKitMouthwashBL2];
    const bl1Record = record?.[collectionDetails]?.[baseline]?.[bioKitMouthwashBL1];


    if(bl2Record) {
        visit = 'BL_2';
        requestDate = bl2Record[dateKitRequested];
    } else if (bl1Record) {
        visit = 'BL_1';
        requestDate = bl1Record[dateKitRequested];
    }
    const processedRecord = {
        first_name: record[firstName],
        last_name: record[lastName],
        connect_id: record['Connect_ID'],
        visit,
        requestDate,
        ...addressObj
    };

    return printLabel || hasMouthwash
        ? processedRecord
        : [];
}

const convertSiteLoginToNumber = (siteLogin) => {
    const siteLoginNumber = parseInt(siteLogin);
    if (siteLoginNumber === NaN) return undefined;
    const siteLoginCidArray = Object.values(fieldMapping.siteLoginMap);
    const isSiteLoginCidFound = siteLoginCidArray?.includes(siteLoginNumber);
    return isSiteLoginCidFound ? siteLoginNumber : undefined;
}

const swapObjKeysAndValues = (object) => {
    const newObject = {};
    for (const key in object) {
        const value = object[key];
        newObject[value] = key;
    }
    return newObject;
}

const batchLimit = 500;

const getUserProfile = async (req, res, uid) => {

    if(req.method !== 'GET') {
        return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
    }

    const { retrieveUserProfile } = require('./firestore');
    let responseProfile = await retrieveUserProfile(uid);

    if(responseProfile instanceof Error){
        return res.status(500).json(getResponseJSON(responseProfile.message, 500));
    }

    if(!isEmpty(responseProfile)){

        let responseDefaults = await checkDefaultFlags(responseProfile, uid);
        
        if(responseDefaults instanceof Error){
            return res.status(500).json(getResponseJSON(responseDefaults.message, 500));
        }

        if(responseDefaults) {
            responseProfile = await retrieveUserProfile(uid);

            if(responseProfile instanceof Error){
                return res.status(500).json(getResponseJSON(responseProfile.message, 500));
            }
        }
    }
    
    return res.status(200).json({data: responseProfile, code:200});
}

const isEmpty = (object) => {
    for(let prop in object) {
        if(Object.prototype.hasOwnProperty.call(object, prop)) {
            return false;
        }
    }

    return true;
}

const findKeyByValue = (object, value) => {
    return Object.keys(object).find(key => object[key] === value);
}


const isDateTimeFormat = (value) => {
    return typeof value == "string" && (/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z/.test(value));
}

/**
 * Split a large array into smaller chunks for batched processing
 * @param {Array} inputArray 
 * @param {number} chunkSize 
 * @returns 
 */
const createChunkArray = (inputArray, chunkSize) => {
    let chunkArray = [];
    for (let i = 0; i < inputArray.length; i += chunkSize) {
        chunkArray.push(inputArray.slice(i, i + chunkSize));
    }

    return chunkArray;
};

const redactEmailLoginInfo = (participantEmail) => {
    const [prefix, domain] = participantEmail.split("@");
    const changedPrefix = prefix.length > 3
        ? prefix.slice(0, 2) + "*".repeat(prefix.length - 3) + prefix.slice(-1)
        : prefix.slice(0, -1) + "*";
    return changedPrefix + "@" + domain;
};

const redactPhoneLoginInfo = (participantPhone) => "***-***-" + participantPhone.slice(-4);

// Note: '223999569' is Biohazard bag (mouthwash) scan, '787237543' is Biohazard bag (blood/urine) scan, '522094118' is Orphan bag scan. These are not tubes.
const tubeConceptIds = [
    '143615646', // Mouthwash tube 1
    '232343615', // Serum separator tube 4
    '299553921', // Serum separator tube 1
    '376960806', // Serum separator tube 3
    '454453939', // EDTA tube 1
    '589588440', // Serum separator tube 5
    '652357376', // ACD tube 1
    '677469051', // EDTA tube 2
    '683613884', // EDTA tube 3
    '703954371', // Serum separator tube 2
    '838567176', // Heparin tube 1
    '958646668', // Heparin tube 2
    '973670172', // Urine tube 1
    '505347689', // Streck tube 1
];

const bagTypeConceptIds = [
    fieldMapping.tubesBagsCids.orphanScan,
    fieldMapping.tubesBagsCids.biohazardBagScan,
    fieldMapping.tubesBagsCids.biohazardMouthwashBagScan
];

/**
 * Extract collectionIds from a list of boxes
 * @param {array} boxesList - list of boxes to process
 * @returns {array} - array of unique collectionIds
 * Bag types: 787237543 (Biohazard Blood/Urine), 223999569 (Biohazard Mouthwash), 522094118 (Orphan)
 */
const extractCollectionIdsFromBoxes = (boxesList) => {
    const { bagConceptIDs } = require('./shared');
    const collectionIdSet = new Set();
    for (const box of boxesList) {
        for (const bag of bagConceptIDs) {
            if (box[bag]) {
                const bagId = box[bag]['787237543'] || box[bag]['223999569'] || box[bag]['522094118'];
                if (bagId) {
                    const collectionId = bagId.split(' ')[0];
                    collectionId && collectionIdSet.add(collectionId);
                }
            }
        }
    }
    return Array.from(collectionIdSet);
}

/**
 * process fetched specimen collections, filter out tubes that are not received on the receivedTimestamp day.
 * @param {array} specimenCollections - array of specimen collection data 
 * @param {*} receivedTimestamp - timestamp of the received date
 * @returns {array} - modified specimen collection data array
 */
const processSpecimenCollections = (specimenCollections, receivedTimestamp) => {
    const specimenDataArray = [];

    for (const specimenCollection of specimenCollections) {
        let hasSpecimens = false;
        const filteredSpecimens = tubeConceptIds.reduce((acc, key) => {
            const tube = specimenCollection['data'][key];

            if (tube && tube['926457119'] === receivedTimestamp) {
                acc[key] = tube;
                hasSpecimens = true;
            }
            return acc;
        }, {});

        if (hasSpecimens) {
            specimenDataArray.push({
                'specimens': filteredSpecimens,
                '820476880': specimenCollection['data']['820476880'],
                '926457119': receivedTimestamp, // Tube-level (not specimen-level) data required for this field. They can be different values.
                '678166505': specimenCollection['data']['678166505'],
                '827220437': specimenCollection['data']['827220437'],
                '951355211': specimenCollection['data']['951355211'],
                '915838974': specimenCollection['data']['915838974'],
                '650516960': specimenCollection['data']['650516960'],
                'Connect_ID': specimenCollection['data']['Connect_ID'],
            });
        }
    }

    return specimenDataArray;
}

/**
 * Manage the specimen's boxedStatus and strayTubesList fields when a bag is added.
 * @param {object} specimenData - the current specimen data from firestore. Only one item can be added at a time.
 * @param {array<string>} tubesAddedToBox - the tubes being added to the box.
 * @returns {object} - the specimen's new boxedStatus and strayTubesList.
 * If current status is notBoxed, compare tubesAddedToBox to all tubes in specimen.
 * If current status is partiallyBoxed, compare tubesAddedToBox to strayTubesList.
 * If current status is boxed, something has gone wrong. These tubes should not be available to add.
 *      -Log an error but continue to allow the operation using strayTubesList.
 */
const manageSpecimenBoxedStatusAddBag = (specimenData, tubesAddedToBox) => {
    const currentBoxedStatus = specimenData.data[fieldMapping.boxedStatus] || fieldMapping.notBoxed;
    const currentStrayTubesList = specimenData.data[fieldMapping.strayTubesList] || [];
    const allTubesInSpecimen = extractUsableTubesFromSpecimen(specimenData);

    const getUpdatedStatusAndTubes = (tubesList) => {
        const updatedStrayTubesList = tubesList.filter(tube => !tubesAddedToBox.includes(tube));
        const updatedBoxedStatus = updatedStrayTubesList.length === 0 ? fieldMapping.boxed : fieldMapping.partiallyBoxed;

        return {
            ref: specimenData.ref,
            [fieldMapping.collectionId]: specimenData.data[fieldMapping.collectionId],
            [fieldMapping.boxedStatus]: updatedBoxedStatus,
            [fieldMapping.strayTubesList]: updatedStrayTubesList,
        }
    };

    const boxedStatusMapping = {
        [fieldMapping.notBoxed]: () => getUpdatedStatusAndTubes(allTubesInSpecimen.data),
        [fieldMapping.partiallyBoxed]: () => getUpdatedStatusAndTubes(currentStrayTubesList),
        [fieldMapping.boxed]: () => {
            console.error('Error in specimen management: Specimen is boxed but tubes are being added to a new box.');
            return getUpdatedStatusAndTubes(currentStrayTubesList);
        },
    };

    return boxedStatusMapping[currentBoxedStatus]();
}

/**
 * Iterate through specimen collections and extract the usable tubes. Filter out tubes that are not usable (missing or discard deviations).
 * @param {object} specimenData - specimen data object from firestore where .data is the specimen data and .ref is the firestore document reference.
 * @returns {object} usableSpecimensDataList - specimen data object where .data is an array of usable tubes and .ref is the firestore document reference.
 */
const extractUsableTubesFromSpecimen = (specimenData) => {
    const tubeDeviationFlags = [
        fieldMapping.tubeBrokenDeviation,
        fieldMapping.tubeDiscardDeviation,
        fieldMapping.tubeInsufficientVolumeDeviation,
        fieldMapping.tubeMislabelledDeviation,
        fieldMapping.tubeNotFoundDeviation,
    ];

    const isTubeUsable = (tube) => {
        if (!tube || !tube[fieldMapping.objectId]) {
            return false;
        }

        if (tube[fieldMapping.tubeDiscardFlag] === fieldMapping.yes) {
            return false;
        }

        if (tube[fieldMapping.tubeIsMissing] === fieldMapping.yes) {
            return false;
        }

        const tubeDeviation = tube[fieldMapping.tubeDeviationObject];

        for (const deviationFlag of tubeDeviationFlags) {
            if (tubeDeviation?.[deviationFlag] === fieldMapping.yes) {
                return false;
            }
        }

        return true;
    };

    const specimenCollectionData = specimenData.data;
    const usableTubesArray = tubeConceptIds
        .map(tubeKey => specimenCollectionData[tubeKey])
        .filter(isTubeUsable)
        .map(tube => tube[fieldMapping.objectId]);

    return {
        data: usableTubesArray,
        ref: specimenData.ref,
    };
}

/**
 * Update the specimen's boxedStatus and strayTubesList fields when a bag is removed from a box.
 * @param {array<object>} specimenDataArray - array of specimen data objects from firestore where .data is the specimen data and .ref is the firestore document reference.
 * @param {object} samplesWithinBag - object where key is collectionId and value is an array of specimens being removed. { collectionId: [specimen1, specimen2, ...]}.
 * @returns {array<object>} - array of specimen data objects with updated boxedStatus and strayTubesList fields, and a firestore doc ref.
 */
const manageSpecimenBoxedStatusRemoveBag = (specimenDataArray, samplesWithinBag) => {
    const updatedSpecimenDataArray = [];

    // If tubes and allTubesInSpecimen are identical, return true. Else return false.
    // True means a collection is not boxed. False means a collection is partially boxed.
    const getIsCollectionNotBoxed = (tubes, allTubesInSpecimen) => {
        return tubes.length === allTubesInSpecimen.length &&
        allTubesInSpecimen.every(tube => tubes.includes(tube)) &&
        tubes.every(tube => allTubesInSpecimen.includes(tube));
    }

    // Update the boxedStatus and strayTubeList for the specimen
    // Unboxed specimens have the notBoxed status and an empty strayTubesList.
    const updateBoxedStatusAndStrayTubesArray = (isAllUsableTubes, tubes) => {
        return {
            updatedBoxedStatus: isAllUsableTubes ? fieldMapping.notBoxed : fieldMapping.partiallyBoxed,
            updatedStrayTubesList: isAllUsableTubes ? [] : tubes,
        };
    };

    for (const specimenDataDoc of specimenDataArray) {
        const specimenData = specimenDataDoc.data;
        const specimenCollectionId = specimenData[fieldMapping.collectionId];
        const currentBoxedStatus = specimenData[fieldMapping.boxedStatus] || fieldMapping.partiallyBoxed;
        const currentStrayTubesList = specimenData[fieldMapping.strayTubesList] || [];
        const allTubesInSpecimenObj = extractUsableTubesFromSpecimen(specimenDataDoc);
        const tubesRemovedFromBox = samplesWithinBag[specimenCollectionId] ?? [];
    
        // currentStrayTubeList will be populated when partiallyBoxed, and empty when boxed.
        const tubesInBagAndStrays = [...tubesRemovedFromBox, ...currentStrayTubesList];
        const isAllUsableTubes = getIsCollectionNotBoxed(tubesInBagAndStrays, allTubesInSpecimenObj.data);
        const {updatedBoxedStatus, updatedStrayTubesList} = updateBoxedStatusAndStrayTubesArray(isAllUsableTubes, tubesInBagAndStrays);

        if (currentBoxedStatus === fieldMapping.notBoxed) {
            console.error('Error in specimen management: Specimen is not boxed but tubes are being removed from a bag.');
        }
    
        updatedSpecimenDataArray.push({
            ref: specimenDataDoc.ref,
            [fieldMapping.collectionId]: specimenCollectionId,
            [fieldMapping.boxedStatus]: updatedBoxedStatus,
            [fieldMapping.strayTubesList]: updatedStrayTubesList,
        });
    }
    return updatedSpecimenDataArray;
}

const sortBoxOnBagRemoval = (boxData, bagsToRemove, currDate) => {
    const samplesWithinBag = {};
    let hasOrphanFlag = fieldMapping.no;

    for (const conceptID of bagConceptIDs) { 
        const currBag = boxData[conceptID];
        if (!currBag) continue;
        for (const bagID of bagsToRemove) {               
            if (bagTypeConceptIds.some(scan => currBag[scan] === bagID)) {
                const collectionId = bagID.split(' ')[0];
                if (!samplesWithinBag[collectionId]) {
                    samplesWithinBag[collectionId] = boxData[conceptID][fieldMapping.samplesWithinBag] || [];
                } else {
                    samplesWithinBag[collectionId] = [...samplesWithinBag[collectionId], ...boxData[conceptID][fieldMapping.samplesWithinBag]];
                }
                delete boxData[conceptID];
            }
        }
    }

    // Create a new sorted box
    let sortedBox = {};
    for (const conceptID of bagConceptIDs) {
        if (conceptID in boxData) {
            sortedBox[conceptID] = boxData[conceptID];
            delete boxData[conceptID];
        }
    }

    // Merge remaining properties from the original box to the sorted box
    sortedBox = { ...sortedBox, ...boxData }
    delete sortedBox['addedTubes'];

    // iterate over all current bag concept Ids and change the value of hasOrphanFlag
    for(const conceptID of bagConceptIDs) {
        const currBag = sortedBox[conceptID];
        if (!currBag) continue;

        hasOrphanFlag = (currBag[fieldMapping.orphanBagFlag] == fieldMapping.yes) 
            ? fieldMapping.yes 
            : fieldMapping.no;
    }

    const updatedBoxData = {
        ...sortedBox,
        [fieldMapping.boxLastModifiedTimestamp]: currDate,
        [fieldMapping.boxHasOrphanBag]: hasOrphanFlag,
    }

    return {
        updatedBoxData,
        samplesWithinBag,
    };
}

/**
 * Flatten an object into dot notation structure.
 * @param {object} obj - the object to flatten. 
 * @param {string} parentPath - the parent path of the object.
 * @returns {object} - an object with flattened keys.
 */
const flattenObject = (obj, parentPath = '') => {
    const flattened = {};

    const traverse = (currentObj, currentPath) => {
        for (const key in currentObj) {
            const value = currentObj[key];
            const newPath = currentPath ? `${currentPath}.${key}` : key;

            if (value && typeof value === 'object') {
                if (Array.isArray(value)) {
                    // Check if element is an object to decide whether to traverse further. 
                    // This ensures arrays of primitive values are kept intact. 
                    // Seen in keys 173836415.266600170.110349197 & 173836415.266600170.543608829.
                    if (value.length === 0 || typeof value[0] !== 'object') {
                        flattened[newPath] = value;
                    } else {
                        value.forEach((item, index) => {
                            traverse(item, `${newPath}[${index}]`);
                        });
                    }
                } else {
                    traverse(value, newPath);
                }
            } else {
                // Assign scalar values.
                flattened[newPath] = value;
            }
        }
    };

    traverse(obj, parentPath);
    return flattened;
};

/**
 * Validate and update the data object for the cancer occurrences field (637153953) in the participant profile.
 * @param {array<object>} incomingCancerOccurrenceArray - the update object for the participant profile. 
 * @param {array<object>} existingOccurrences - existing cancer occurrences from the participant profile.
 * @param {object} requiredOccurrenceRules - the required fields for each cancer occurrence.
 * @param {string} participantToken - the participant's token.
 * @param {string} participantConnectId - the participant's connect id.
 * @returns {object} - an object with an error flag, message, and data array.
 */
const handleCancerOccurrences = async (incomingCancerOccurrenceArray, requiredOccurrenceRules, participantToken, participantConnectId) => {
    // Validate the new occurrence array data
    for (let i = 0; i < incomingCancerOccurrenceArray.length; i++) {
        const occurrence = incomingCancerOccurrenceArray[i];

        for (const rule of requiredOccurrenceRules) {
            const [, propertyKey] = rule.split('.'); // Example: Splitting '637153953[0]' and '345545422'

            if (!occurrence || !occurrence[propertyKey]) {
                return { error: true, message: `Missing required field: ${propertyKey} in occurrence ${i}`, data: [] };
            }
        }

        const cancerSiteValidationObj = validateCancerOccurrence(occurrence[fieldMapping.primaryCancerSiteObject]);
        if (cancerSiteValidationObj.error === true) {
            return cancerSiteValidationObj;
        }

        const diagnosisAwarenessValidationObj = validateDiagnosisAwareness(occurrence[fieldMapping.vitalStatusCategorical], occurrence[fieldMapping.participantDiagnosisAwareness]);
        if (diagnosisAwarenessValidationObj.error === true) {
            return diagnosisAwarenessValidationObj;
        }
    }

    // Query existing occurrences for the participant
    const { getParticipantCancerOccurrences } = require('./firestore');
    const existingCancerOccurrences = await getParticipantCancerOccurrences(participantToken);

    // Make sure the occurrence is not a duplicate - check timestamp and yes values in primary cancer sites.
    const duplicateCheckResponse = checkForDuplicateCancerOccurrences(incomingCancerOccurrenceArray, existingCancerOccurrences);
    if (duplicateCheckResponse.error) {
        return { error: true, message: duplicateCheckResponse.message, data: [] };
    }

    const finalizedCancerOccurrenceArray = [];
    let mostRecentOccurrenceNum = existingCancerOccurrences.length;
    for (const newOccurrence of incomingCancerOccurrenceArray) {
        // Finalize the occurrence data and add to the finalized array.
        const occurrenceData = finalizeCancerOccurrenceData(newOccurrence, participantToken, participantConnectId, mostRecentOccurrenceNum);
        finalizedCancerOccurrenceArray.push(occurrenceData);
        mostRecentOccurrenceNum++;
    }

    return { error: false, message: 'Success!', data: finalizedCancerOccurrenceArray };
}

/**
 * Validate additional cancer site requirements in the Occurrence object (740819233).
 * Important: fieldMapping.primaryCancerSiteObj value has already been validated in the initial validation step.
 * If the 'fieldMapping.cancerSites.other' cancer site is selected, the 'fieldMapping.anotherTypeOfCancerText' field is required.
 * Else, the 'anotherTypeOfCancerText' field should not be present.
 * @param {object} cancerSitesObject - property (740819233) in the cancer occurrence object (637153953).
 * @returns {object} - Returns an object with error (boolean), message (string), and data (array).
 */
const validateCancerOccurrence = (cancerSitesObject) => {
    if (!cancerSitesObject || Object.keys(cancerSitesObject).length === 0 || !cancerSitesObject[fieldMapping.primaryCancerSiteCategorical]) {
        return { error: true, message: 'Primary cancer site categorical (740819233.149205077) is required.', data: [] };
    }
    
    const isOtherCancerSiteSelected = cancerSitesObject[fieldMapping.primaryCancerSiteCategorical] === fieldMapping.cancerSites.other;
    const isAnotherTypeOfCancerTextValid = 
        cancerSitesObject[fieldMapping.anotherTypeOfCancerText] !== null &&
        typeof cancerSitesObject[fieldMapping.anotherTypeOfCancerText] === 'string' &&
        cancerSitesObject[fieldMapping.anotherTypeOfCancerText].trim().length > 0;

    const otherCancerSiteErrorMessage = "'Another type of cancer description' (868006655) must be included if primary cancer site is 'other' (807835037)." +
    "Otherwise, 'another type of cancer description' (868006655) must be excluded.";

    const hasError = isOtherCancerSiteSelected ? !isAnotherTypeOfCancerTextValid : isAnotherTypeOfCancerTextValid;

    return { error: hasError, message: hasError ? otherCancerSiteErrorMessage : '', data: [] };
}

/**
 * Rules: if vitalStatusCategorical is 'alive' at chart review (114227122: 337516613), participant must be aware of diagnosis (844209241: 353358909). Else, block API request.
 * If vitalStatusCategorical is 'dead' or 'unknown' (114227122: 646675764 or 178420302), participant awareness can be yes, no, or unknown (844209241: 353358909 or 104430631 or 178420302).
 * @param {number} vitalStatusCategorical - the participant's vital status (conceptID).
 * @param {number} participantDiagnosisAwareness - the participant's awareness of diagnosis (conceptID).
 * @returns {object} - Returns an object with error (boolean), message (string), and data (array).
 */
const validateDiagnosisAwareness = (vitalStatusCategorical, participantDiagnosisAwareness) => {
    const isAliveAtChartReview = vitalStatusCategorical === fieldMapping.vitalStatus.alive;
    const isParticipantAwareOfDiagnosis = participantDiagnosisAwareness === fieldMapping.yes;

    const isAwarenessValid = isAliveAtChartReview ? isParticipantAwareOfDiagnosis : true;
    const awarenessErrorMessage = "Participant must be aware of diagnosis if alive at chart review. Otherwise, awareness can be 'yes (353358909)', 'no (104430631)', or 'unknown (178420302)'.";

    return { error: !isAwarenessValid, message: !isAwarenessValid ? awarenessErrorMessage : '', data: [] };
}

/**
 * Check for duplicate cancer occurrences. Occurrences are considered duplicates if the timestamp and primary cancer sites match.
 * @param {array<object>} newOccurrenceArray - the new occurrence array to check.
 * @param {array<object>} existingCancerOccurrences - the existing occurrences to check against.
 * @returns {object} - an object with an error flag, message, and data array.
 * Check for a timestamp match. If a timestamp match is found, compare cancer site data. Short circuit on mismatch (data is unique).
 */
const checkForDuplicateCancerOccurrences = (newOccurrenceArray, existingCancerOccurrences) => {
    const buildTimestampHashMap = (occurrences) => {
        const timestampHashMap = {};
        for (const occurrence of occurrences) {
            const timestamp = occurrence[fieldMapping.cancerOccurrenceTimestamp];
            if (!timestampHashMap[timestamp]) {
                timestampHashMap[timestamp] = [];
            }
            timestampHashMap[timestamp].push(occurrence);
        }
        return timestampHashMap;
    }
    
    const existingOccurrencesHashMap = buildTimestampHashMap(existingCancerOccurrences);

    for (const newOccurrence of newOccurrenceArray) {
        const timestamp = newOccurrence[fieldMapping.cancerOccurrenceTimestamp];
        const potentialDuplicateOccurrences = existingOccurrencesHashMap[timestamp] || [];

        for (const occurrence of potentialDuplicateOccurrences) {
            if (occurrence[fieldMapping.primaryCancerSiteObject][fieldMapping.primaryCancerSiteCategorical] === newOccurrence[fieldMapping.primaryCancerSiteObject][fieldMapping.primaryCancerSiteCategorical]) {
                return {
                    error: true,
                    message: 
                        `Error: Duplicate cancer occurrence. Timestamp and primary site matches existing cancer occurrence for this participant. Timestamp: ${newOccurrence[fieldMapping.cancerOccurrenceTimestamp]}, ` +
                        `Primary Cancer Site: ${newOccurrence[fieldMapping.primaryCancerSiteObject][fieldMapping.primaryCancerSiteCategorical]}`,
                    data: [],
                };    
            }
        }
    }
    return { error: false, message: 'Success!', data: []};
}

const finalizeCancerOccurrenceData = (occurrenceData, participantToken, participantConnectId, mostRecentOccurrenceNum) => {
    const finalizedOccurrenceData = {
        ...occurrenceData,
        [fieldMapping.occurrenceNumber]: mostRecentOccurrenceNum + 1,
        [fieldMapping.isCancerDiagnosis]: fieldMapping.yes,
        [fieldMapping.cancerDataReceivedTimestamp]: new Date().toISOString(),
        'token': participantToken,
        'Connect_ID': participantConnectId,
    };

    if (!finalizedOccurrenceData[fieldMapping.primaryCancerSiteObject][fieldMapping.primaryCancerSiteCategorical]) {
        finalizedOccurrenceData[fieldMapping.primaryCancerSiteObject][fieldMapping.primaryCancerSiteCategorical] = '';
    }

    return finalizedOccurrenceData;
}

const birthdayCardParticipantFields = [
    fieldMapping.preferredName,
    fieldMapping.lastName,
    fieldMapping.address1,
    fieldMapping.address2,
    fieldMapping.city,
    fieldMapping.state,
    fieldMapping.zip,
];

/**
 * Handle the incoming NORC birthday card data:
 *  - Determine the write action needed for the birthday card data.
 *  - Handle (optional) updates to the participant profile.
 * Default to creating a new card document, but other cases need to be handled.
 * If the card exists and is identical, no write action is needed. We still need to update the participant profile if that data is included.
 * If the card exists and has different data, we need to update the existing card. We also need to update the participant profile if that data is included.
 */

const handleNorcBirthdayCard = async (incomingBirthdayCardData, requiredBirthdayCardRules, participantToken, participantConnectId, participantProfileHistory) => {
    // Validate the incoming birthday card data.
    const birthdayCardValidationObj = validateBirthdayCardData(incomingBirthdayCardData, requiredBirthdayCardRules);
    if (birthdayCardValidationObj.error) {
        return birthdayCardValidationObj;
    }

    // Check for an existing card (where token, mailDate, & cardVersion match) in the birthdayCard collection.
    const { getExistingBirthdayCard } = require('./firestore');
    const mailDate = incomingBirthdayCardData[fieldMapping.birthdayCardData.mailDate];
    const cardVersion = incomingBirthdayCardData[fieldMapping.birthdayCardData.cardVersion];
    const { existingCardData, cardDocId } = await getExistingBirthdayCard(participantToken, mailDate, cardVersion);

    // Set up the finalized data objects and determine the write action needed.
    let cardWriteType = '';
    let finalizedBirthdayCardData = {};

    if (existingCardData) {
        // Skip the birthdayCard write operation for identical cards. Otherwise, update the existing card.
        const isBirthdayCardIdenticalResponse = isBirthdayCardIdentical(existingCardData, incomingBirthdayCardData);
        if (!isBirthdayCardIdenticalResponse) {
            cardWriteType = 'update';
        }
    } else {
        // Create a new card document. Add token & Connect_ID.
        cardWriteType = 'create';
        finalizedBirthdayCardData['token'] = participantToken;
        finalizedBirthdayCardData['Connect_ID'] = participantConnectId;
    }

    // Build the finalized birthday card data object.
    if (cardWriteType && ['create', 'update'].includes(cardWriteType)) {
        Object.values(fieldMapping.birthdayCardData).forEach(field => {
            if (incomingBirthdayCardData[field] !== undefined) {
                finalizedBirthdayCardData[field] = incomingBirthdayCardData[field];
            }
        });
    }

    // Build the participantUpdateData Obj depending on the result of the timestamp check. This data won't always be present.
    let norcParticipantUpdateData = {};
    const shouldUpdateProfileHistory = checkProfileHistoryTimestamps(participantProfileHistory, mailDate);
    if (shouldUpdateProfileHistory) {
        for (const field of birthdayCardParticipantFields) {
            if (incomingBirthdayCardData[field] !== undefined) {
                norcParticipantUpdateData[field] = incomingBirthdayCardData[field];
            }
        }
    }

    const birthdayCardWriteDetails = { cardWriteType, cardDocId };

    return { error: false, message: 'Success!', data: [finalizedBirthdayCardData, norcParticipantUpdateData, birthdayCardWriteDetails] };
}

/**
 * Ensure the incoming NORC birthday card data is valid and contains the required fields.
 * @param {object} incomingBirthdayCardData - The incoming birthday card data.
 * @param {object} requiredBirthdayCardRules - The required fields for the birthday card data (from updateParticipantData.json).
 * @returns {object} - Returns an object with an error flag, message, and data array. 
 */
const validateBirthdayCardData = (incomingBirthdayCardData, requiredBirthdayCardRules) => {
    if (!incomingBirthdayCardData || Object.keys(incomingBirthdayCardData).length === 0) {
        const birthdayCardKeys = birthdayCardCollectionFields.join(', ');
        return { error: true, message: `Missing birthday card data. Related keys: ${birthdayCardKeys}`, data: [null, null, null] };
    }

    // Ensure required keys are present.
    for (const rule of requiredBirthdayCardRules) {
        const [, propertyKey] = rule.split('.');

        if (!incomingBirthdayCardData || !incomingBirthdayCardData[propertyKey]) {
            return { error: true, message: `Missing required field: ${propertyKey} in birthday card data`, data: [null, null, null] };
        }
    }

    // If return date is provided, ensure the mail date is not after the return date.
    const mailDate = incomingBirthdayCardData[fieldMapping.birthdayCardData.mailDate];
    const returnDate = incomingBirthdayCardData[fieldMapping.birthdayCardData.returnDate];
    if (returnDate && mailDate > returnDate) {
        return { error: true, message: `Return date cannot be before mail date. Provided data: Mail date: ${mailDate}. Return date ${returnDate}`, data: [null, null, null] };
    }

    return { error: false, message: 'Success!', data: [null, null, null] };
}

/**
 * 
 * @param {object} existingCard - The existing birthday card data.
 * @param {object} incomingBirthdayCardData - The incoming birthday card data.
 * @returns {boolean} - Returns true if the incoming birthday card data is identical to the existing card.
 */
const isBirthdayCardIdentical = (existingCard, incomingBirthdayCardData) => {
    for (let field of Object.values(fieldMapping.birthdayCardData)) {
        if (incomingBirthdayCardData[field] !== existingCard[field]) {
            return false;
        }
    }
    return true;
}

/**
 * Test whether this update is the more recent than the last user profile update. Initial usage: NORC birthday card updates (mailDate).
 * Compare the timestamp of the most recent user profile update to the timestamp of the incoming update. If the incoming update is more recent, return true.
 * @param {array | null} userProfileHistory - The user profile history array, or null if no updates have been made to the user's name or address fields.
 * @param {string} timestampToTest - The timestamp of the incoming update.
 * @returns {boolean} - Returns true if the incoming update is more recent than the last user profile update.
 */
const checkProfileHistoryTimestamps = (userProfileHistory, timestampToTest) => {
    let mostRecentProfileUpdateTimestamp;

    // If no history exists, the change is more recent than the participant's signup date since the participant must be signed up to receive a birthday card.
    if (!userProfileHistory || !Array.isArray(userProfileHistory) || userProfileHistory.length === 0) {
        return true;
    }

    // The most recent profile update timestamp should always be the last element in the array, but this is a safety check.
    for (const profileUpdate of userProfileHistory) {
        const profileUpdateTimestamp = profileUpdate[fieldMapping.userProfileHistoryTimestamp];
        if (profileUpdateTimestamp && (!mostRecentProfileUpdateTimestamp || profileUpdateTimestamp > mostRecentProfileUpdateTimestamp)) {
            mostRecentProfileUpdateTimestamp = profileUpdateTimestamp;
        }
    }

    return !mostRecentProfileUpdateTimestamp || timestampToTest > mostRecentProfileUpdateTimestamp;
}

/**
 * We've had an intermittnt issue with Streck tube data not being added to the specimen data structure (11/2023). 
 * This is a safety net to ensure the data is added. Build the object literal since we only need to do this for streck tubes.
 * @param {string} collectionId - the collection Id of the specimen.
 * @param {object} streckTubeData - the streck tube data object (empty placeholder passed in for assignment by reference).
 */
const buildStreckPlaceholderData = (collectionId, streckTubeData) => {
    Object.assign(streckTubeData, {
        [fieldMapping.tubeIsCollected]: fieldMapping.no,
        [fieldMapping.tubeIsDeviated]: fieldMapping.no,
        [fieldMapping.tubeDiscardFlag]: fieldMapping.no,
        [fieldMapping.tubeDeviationObject]: {
            [fieldMapping.tubeDeviationHemolyzed]: fieldMapping.no,
            [fieldMapping.tubeDeviationMislabeledResolved]: fieldMapping.no,
            [fieldMapping.tubeDeviationOuterTubeContaminated]: fieldMapping.no,
            [fieldMapping.tubeDeviationOther]: fieldMapping.no,
            [fieldMapping.tubeBrokenDeviation]: fieldMapping.no,
            [fieldMapping.tubeDeviationLowTemp]: fieldMapping.no,
            [fieldMapping.tubeMislabelledDeviation]: fieldMapping.no,
            [fieldMapping.tubeDeviationHighTemp]: fieldMapping.no,
            [fieldMapping.tubeDeviationLowVolume]: fieldMapping.no,
            [fieldMapping.tubeDeviationLeakedSpilled]: fieldMapping.no,
            [fieldMapping.tubeDeviationUnexpectedTubeSize]: fieldMapping.no,
            [fieldMapping.tubeDiscardDeviation]: fieldMapping.no,
            [fieldMapping.tubeNotFoundDeviation]: fieldMapping.no,
        },
    });
    console.error(`Issue found in updateSpecimen() (ConnectFaas): Streck Tube not found in biospecimenData for collection Id ${collectionId}. Building placeholder data.`);
}

// Note: prefer validateIso8601Timestamp function for more robust timestamp validation.
const validIso8601Format = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;
const validEmailFormat = /^[a-zA-Z0-9.!#$%&'*+"\/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*\.[a-zA-Z]{2,63}$/;
const validPhoneFormat = /^\d{10}$/;

const queryListFields = {
    firstName: [fieldMapping.firstName, fieldMapping.preferredName],
    lastName: [fieldMapping.lastName],
    allPhoneNo: [fieldMapping.homePhone, fieldMapping.cellPhone, fieldMapping.otherPhone],
    allEmails: [fieldMapping.prefEmail, fieldMapping.additionalEmail1, fieldMapping.additionalEmail2],
}

const userProfileHistoryKeys = [
    fieldMapping.firstName,
    fieldMapping.middleName,
    fieldMapping.lastName,
    fieldMapping.suffix,
    fieldMapping.preferredName,
    fieldMapping.cellPhone,
    fieldMapping.canWeVoicemailMobile,
    fieldMapping.canWeText,
    fieldMapping.homePhone,
    fieldMapping.canWeVoicemailHome,
    fieldMapping.otherPhone,
    fieldMapping.canWeVoicemailOther,
    fieldMapping.address1,
    fieldMapping.address2,
    fieldMapping.city,
    fieldMapping.state,
    fieldMapping.zip,
  ];


/**
 * Check whether dataObj contains any of the fields in participantQueryListFields. If yes, handle the query list fields in the participant profile.
 * @param {object} dataObj - the update object for the participant profile.
 * @returns - true if any of the fields in participantQueryListFields are in dataObj. false otherwise.
 */
const checkForQueryFields = (dataObj) => {
    for (const key in queryListFields) {
        // Check if any field in the array for this key exists in dataObj
        const hasQueryField = queryListFields[key].some(field => dataObj[field] !== undefined);
        if (hasQueryField) {
            return true;
        }
    }
    return false; 
}

const updateQueryListFields = (dataObj, existingDocData) => {
    let updatedQueryObj = existingDocData['query'] ? {...existingDocData['query']} : {};

    for (const queryField in queryListFields) {
        const isFieldPresentInDataObj = queryListFields[queryField].some(field => dataObj[field]);
        
        if (isFieldPresentInDataObj) {
            let currentQueryArray = updatedQueryObj?.[queryField] ? [...updatedQueryObj[queryField]] : [];
            
            queryListFields[queryField].forEach(field => {
                if (dataObj[field]) {
                    currentQueryArray = updateQueryArray(dataObj[field], existingDocData?.[field], currentQueryArray);
                }
            });

            if (currentQueryArray.length > 0) {
                updatedQueryObj[queryField] = currentQueryArray;
            }
        }
    }

    return updatedQueryObj;
}

/**
 * Name, Phone number, and email updates require special handling because the query.firstName, query.lastName, query.allEmails, and query.allPhoneNo arrays are used for participant search.
 * These can not be directly updated in the API call. We need to derive them just like we do in SMDB & PWA.
 * If oldData is present, remove it from the queryArray and replace with newData. If oldData is not present, add newData to the queryArray.
 * @param {string} newData - the updated query data point.
 * @param {string} oldData - the existing query data point.
 * @param {array<string>} queryArray - the array of existing data (query.firstName, query.lastName, query.allPhoneNo, or query.allEmails in the participant record).
 * @returns {array<string>} - the updated queryArray.
 */
const updateQueryArray = (newData, oldData, queryArray) => {
    let updatedQueryArray = [...queryArray];

    newData = typeof newData === 'string' ? newData.toLowerCase() : '';
    oldData = oldData && typeof oldData === 'string' ? oldData.toLowerCase() : '';

    const oldDataIndex = oldData ? updatedQueryArray.indexOf(oldData) : -1;
    if (oldDataIndex !== -1) {
        updatedQueryArray.splice(oldDataIndex, 1);
    }

    if (!updatedQueryArray.includes(newData)) {
        updatedQueryArray.push(newData);
    }

    return updatedQueryArray;
};

/**
 * If any of the fields in userProfileHistoryKeys are in dataObj, update the userProfileHistory object in the participant profile.
 * @param {object} dataObj - the incoming user profile data object. This is a partial object with update data only, not the complete profile.
 * @param {object} existingDocData - the existing user profile data object from firestore.
 * @param {array<string>} siteCodes - the site codes for the user profile update.
 * userProfileHistory is an array of objects. Each object has a timestamp and a set of fields that were updated.
 */
const updateUserProfileHistory = (dataObj, existingDocData, siteCodes) => {
    const userProfileHistory = existingDocData[fieldMapping.userProfileHistory] || [];

    // This data is always added to the update object.
    let updateObject = {
        [fieldMapping.userProfileHistoryTimestamp]: new Date().toISOString(),
        [fieldMapping.profileChangeRequestedBy]: `Site ${siteCodes.toString()}: updateParticipantData API`
    };

    // If any of the fields in userProfileHistoryKeys are in dataObj, add the existing fields from existingDocData to the updateObject.
    for (const key of userProfileHistoryKeys) {
        if (dataObj[key] && existingDocData[key] && dataObj[key] !== existingDocData[key]) {
            updateObject[key] = existingDocData[key];
        }
    }

    // If the updateObject has more than just the timestamp and profileChangeRequestedBy fields, add it to the userProfileHistory array. Else discard.
    if (Object.keys(updateObject).length > 2) {
        userProfileHistory.push(updateObject);
    }

    return userProfileHistory;
}

/**
 * Return selected Concept ID fields for each object in a list of data objects.
 * Fields with nested data include all sub-data.
 * @param {array<object>} dataObjArray - the array of data objects to filter.
 * @param {array<string>} selectedFieldsArray - the array of concept ID fields to return. Top-level (non-nested) fields only.
 * @returns {array<object>} - the filtered array of data objects.
 */

// Note: Prefer using Firestore's new .select() method to select the necessary fields instead of filtering fields in the response.
const filterSelectedFields = (dataObjArray, selectedFieldsArray) => {
    
    const handleNestedData = (obj, path) => {
        return path.split('.').reduce((currentObj, key) => currentObj ? currentObj[key] : undefined, obj);
    }

    return dataObjArray.map(dataObj => {
        const filteredData = { 'Connect_ID': dataObj['Connect_ID'] };        
        for (const field of selectedFieldsArray) {
            const fieldValue = handleNestedData(dataObj, field);
            if (fieldValue !== undefined) {
                filteredData[field] = fieldValue;
            }
        }
        return filteredData;
    });
}

const getTemplateForEmailLink = (
    email,
    continueUrl,
    preferredLanguage = fieldMapping.english
) => {
    return preferredLanguage === fieldMapping.spanish
        ? `
    <html>
    <head></head>
    <body marginheight="0">
      <p>Hola,</p>
      <p>Recibimos una solicitud para iniciar sesión en el Estudio Connect para la Prevención del Cáncer usando esta dirección de correo electrónico. Si desea iniciar sesión con su cuenta ${email}, haga clic en este enlace:</p>
      <p><a href="${continueUrl}" target="_other" rel="nofollow">Iniciar sesión para Estudio Connect para la Prevención del Cáncer:</a></p>
      <p>Si no solicitó este enlace, puede ignorar este correo electrónico de forma segura.</p>
      <p>Gracias,</p>
      <p>Su equipo del Estudio Connect para la Prevención del Cáncer</p>
    </body>
    </html>
  `
        : ` <html>
    <head></head>
    <body marginheight="0">
      <p>Hello,</p>
      <p>We received a request to sign in to Connect for Cancer Prevention Study using this email address. If you want to sign in with your ${email} account, click this link:</p>
      <p><a href="${continueUrl}" target="_other" rel="nofollow">Sign in to Connect for Cancer Prevention Study</a></p>
      <p>If you did not request this link, you can safely ignore this email.</p>
      <p>Thanks,</p>
      <p>Your Connect for Cancer Prevention Study team</p>
    </body>
    </html>`;
};

const nihMailbox = 'NCIConnectStudy@mail.nih.gov'

const getSecret = async (key) => {
    const client = new SecretManagerServiceClient();
    const [version] = await client.accessSecretVersion({
        name: key,
    });
    const payload = version.payload.data.toString();
    return payload;
};

const cidToLangMapper = {
    [fieldMapping.english]: "english",
    [fieldMapping.spanish]: "spanish",
};

/**
 * @param {QuerySnapshot | QuerySnapshot[]} snapshot A query snapshot or an array of snapshots
 * @param {string} infoStr Name of the function and other info to be printed
 * @returns {void}
 */
const printDocsCount = (snapshot, infoStr = "") => {
  let count = 0;
  if (Array.isArray(snapshot)) {
    for (const snap of snapshot) {
      if (snap.constructor.name !== "QuerySnapshot" || snap.empty) continue;
      count += snap.size;
    }
  } else {
    if (snapshot.constructor.name !== "QuerySnapshot" || snapshot.empty) return;
    count = snapshot.size;
  }

  if (count > 0) {
    console.log(`Docs read from Firestore: ${count}; function: ${infoStr}`);
  }
};

const unsubscribeTextObj = {
    english:
        "<p><i>To unsubscribe from emails about Connect from the National Cancer Institute (NCI), <% click here %>.</i></p>",
    spanish:
        "<p><i>Para cancelar la suscripción a los correos electrónicos sobre Connect del Instituto Nacional del Cáncer (NCI), <% haga clic aquí %>.</i></p>",
};

/**
 * Returns a date string five days ago in ISO format
 * @returns {string} - ISO string of the date five days ago
 * @example "2024-08-05T00:00:00.000Z"
*/
const getFiveDaysAgoDateISO = () => { 
    const currentDate = new Date();
    return new Date(currentDate.setDate(currentDate.getDate() - 5)).toISOString();
}

/**
 * Create a new Date object with adjusted time
 * @param {number | string | Date } inputTime - Input time to adjust
 * @param {number} [days = 0] - Number of days to adjust
 * @param {number} [hours = 0] - Number of hours to adjust
 * @param {number} [minutes = 0] - Number of minutes to adjust
 * @returns {Date} Adjusted time
 */
const getAdjustedTime = (inputTime, days = 0, hours = 0, minutes = 0) => {
  let adjustedTime = new Date(inputTime);
  adjustedTime.setDate(adjustedTime.getDate() + days);
  adjustedTime.setHours(adjustedTime.getHours() + hours);
  adjustedTime.setMinutes(adjustedTime.getMinutes() + minutes);

  return adjustedTime;
};

const safeJSONParse = (str) => {
    try {
        return JSON.parse(str);
    } catch (e) {
        console.error(`Error parsing JSON: ${e}`);
        return null;
    }
}

/**
 * Delay for a specified time, to avoid errors (race conditions, rate limiting, etc.) 
 * @param {number} ms Delayed time in milliseconds
 * @returns {Promise<void>}
 */
const delay = (ms) => new Promise((res) => setTimeout(res, ms));

const uspsUrl = {
    auth : 'https://apis.usps.com/oauth2/v3/token',
    addresses: 'https://apis.usps.com/addresses/v3/address'
} 

/**
 * Sanitizes an object by removing potentially dangerous keys to prevent prototype pollution.
 *
 * @param {Object} obj - The object to sanitize.
 * @returns {Object} - A new sanitized object with dangerous keys removed.
 */
const sanitizeObject = (obj) => {
    const dangerousKeys = ['__proto__', 'constructor', 'prototype'];
    return Object.keys(obj).reduce((sanitized, key) => {
        if (!dangerousKeys.includes(key)) {
            sanitized[key] = obj[key];
        }
        return sanitized;
    }, {});
};

const developmentTier = process.env.GCLOUD_PROJECT === 'nih-nci-dceg-connect-prod-6d04'
            ? 'PROD'
            : process.env.GCLOUD_PROJECT === 'nih-nci-dceg-connect-stg-5519'
                ? 'STAGE'
                : 'DEV';

module.exports = {
    getResponseJSON,
    setHeaders,
    generateConnectID,
    generatePIN,
    randomString,
    deleteDocuments,
    setHeadersDomainRestricted,
    incentiveFlags,
    lockedAttributes,
    moduleConceptsToCollections,
    moduleStatusConcepts,
    listOfCollectionsRelatedToDataDestruction,
    incentiveConcepts,
    APIAuthorization,
    isParentEntity,
    defaultFlags,
    defaultStateFlags,
    SSOValidation,
    conceptMappings,
    logIPAddress,
    decodingJWT,
    initializeTimestamps,
    tubeKeytoNum,
    collectionIdConversion,
    sites, 
    bagConceptIDs,
    cleanSurveyData,
    updateBaselineData,
    getAddedStrayTubes,
    getHomeMWKitData,
    replacementKitSort,
    manualRequestSort,
    standardHomeKitSort,
    processParticipantHomeMouthwashKitData,
    refusalWithdrawalConcepts,
    withdrawalConcepts,
    convertSiteLoginToNumber,
    swapObjKeysAndValues,
    batchLimit,
    getUserProfile,
    isEmpty,
    findKeyByValue,
    isDateTimeFormat,
    createChunkArray,
    redactEmailLoginInfo,
    redactPhoneLoginInfo,
    tubeConceptIds,
    bagTypeConceptIds,
    extractCollectionIdsFromBoxes,
    processSpecimenCollections,
    manageSpecimenBoxedStatusAddBag,
    manageSpecimenBoxedStatusRemoveBag,
    sortBoxOnBagRemoval,
    buildStreckPlaceholderData,
    validIso8601Format,
    validEmailFormat,
    validPhoneFormat,
    queryListFields,
    userProfileHistoryKeys,
    checkForQueryFields,
    updateQueryListFields,
    updateUserProfileHistory,
    flattenObject,
    handleCancerOccurrences,
    filterSelectedFields,
    getTemplateForEmailLink,
    nihMailbox,
    twilioErrorMessages,
    getSecret,
    cidToLangMapper,
    printDocsCount,
    unsubscribeTextObj,
    getFiveDaysAgoDateISO,
    delay,
    getAdjustedTime,
    handleNorcBirthdayCard,
    safeJSONParse,
    uspsUrl,
    sanitizeObject,
    developmentTier
};
