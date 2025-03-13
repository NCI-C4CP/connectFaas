const assert = require('chai').assert;
const expect = require('chai').expect; // assert is mostly used, but expect is for testing error handlers
const Supertest = require('supertest');
const supertest = Supertest('http://localhost:5001/nih-nci-dceg-connect-dev/us-central1/app?api=');
const bearerToken = 'Bearer ';
const admin = require('firebase-admin');
const uuid = require('uuid');
const firestore = require('../utils/firestore');
const shared = require('../utils/shared');
const validation = require('../utils/validation');
const functions = require('../index');
const submission = require('../utils/submission');
const serviceAccount = require('../nih-nci-dceg-connect-dev-4a660d0c674e'); 
const sinon = require('sinon');
const httpMocks = require('node-mocks-http');
const conceptIds = require('../utils/fieldToConceptIdMapping.js');
const fieldToConceptIdMapping = require('../utils/fieldToConceptIdMapping.js');
const { profileEnd } = require('console');

// NOTE: Some of these tests will only work when you are running connectFaas locally connected to the dev environment
// Tests may be disabled using test.skip
// You may also pick and choose a few specific tests to run using test.only
// Credentials including localtesting-key.json are not included and must be manually configured if you use the tests which include them

async function getOauthToken() {
    const {google} = require("googleapis");
        const serviceAccount = require('../localtesting-key.json');

        const scopes = ["https://www.googleapis.com/auth/userinfo.email"];

        const jwtClient = new google.auth.JWT(
            serviceAccount.client_email,
            null,
            serviceAccount.private_key,
            scopes
        );

        try {
            const tokens = await jwtClient.authorize();
            const accessToken = tokens.access_token;
            
            return accessToken;
        } 
        catch (error) {
            console.error(error);
            return '';
        };
}



describe('incentiveCompleted', async () => {
    it('Should return 200 for options', async () => {
        const req = httpMocks.createRequest({
            method: 'OPTIONS',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.incentiveCompleted(req, res)
        assert.equal(res.statusCode, 200);
        const data = res._getJSONData();
        assert.equal(data.code, 200);
    });
    it('Should only accept POST', async () => {
        const req = httpMocks.createRequest({
            method: 'GET',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.incentiveCompleted(req, res)
        assert.equal(res.statusCode, 405);
        const data = res._getJSONData();
        assert.equal(data.message, 'Only POST requests are accepted!');
        assert.equal(data.code, 405);
    });
    it('Should authenticate with included OAUTH token', async () => {

        try {
            const accessToken = await getOauthToken();
            
            //set local environment variable to access token
            // process.env.ACCESS_TOKEN = accessToken;

            //set environment variable within launch.json file to access token
            // await fetch(authInfo.auth_uri);
            const req = httpMocks.createRequest({
                method: 'POST',
                headers: {
                    'x-forwarded-for': 'dummy',
                    'authorization': 'Bearer ' + accessToken
                },
                connection: {},
                body: {
                    data:
                        [
                        {
                            "token": "6a2f5550-5cdf-4ff0-a6e8-ca7c51db2d8",
                            "round": "baseline",
                            "incentiveRefused": true,
                            "incentiveRefusedAt": "234",
                            "incentiveChosen": "Amazon Gift Card"
                        }
                    ]
                }
            });
        
            const res = httpMocks.createResponse();
            await functions.incentiveCompleted(req, res)
            // assert.equal(res.statusCode, 405);
            const data = res._getJSONData();
        } 
        catch (error) {
            console.error(error)
        };
        
        
        
    });
});

describe('participantsEligibleForIncentive', async () => {
    it('Should return 200 for options', async () => {
        const req = httpMocks.createRequest({
            method: 'OPTIONS',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.participantsEligibleForIncentive(req, res)
        assert.equal(res.statusCode, 200);
        const data = res._getJSONData();
        assert.equal(data.code, 200);
    });
    it('Should only accept GET', async () => {
        const req = httpMocks.createRequest({
            method: 'POST',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.participantsEligibleForIncentive(req, res);
        assert.equal(res.statusCode, 405);
        const data = res._getJSONData();
        assert.equal(data.message, 'Only GET requests are accepted!');
        assert.equal(data.code, 405);
    });
});

describe('getParticipantToken', async () => {
    it('Should return 200 for options', async () => {
        const req = httpMocks.createRequest({
            method: 'OPTIONS',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.getParticipantToken(req, res)
        assert.equal(res.statusCode, 200);
        const data = res._getJSONData();
        assert.equal(data.code, 200);
    });
    it('Should only accept POST', async () => {
        const req = httpMocks.createRequest({
            method: 'GET',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.getParticipantToken(req, res)
        assert.equal(res.statusCode, 405);
        const data = res._getJSONData();
        assert.equal(data.message, 'Only POST requests are accepted!');
        assert.equal(data.code, 405);
    });
    // This does not currently work because accessToken credentials
    // do not include the required site code
    it.skip('Should generate a user', async () => {
        const uid = uuid.v4();
        const accessToken = await getOauthToken();
        const req = httpMocks.createRequest({
            method: 'POST',
            body: {
                data: [{
                    studyId: uid,

                }]
            },
            headers: {
                'x-forwarded-for': 'dummy',
                'authorization': 'Bearer ' + accessToken
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        functions.getParticipantToken(req, res)
            .then(() => {
                // assert.equal(res.statusCode, 200);
                const data = res._getData();
                // console.log('data', data);
            })
            .catch(console.error);
        
    });
});

describe('validateUsersEmailPhone', () => {
    it('Should only accept GET', async () => {
        const req = httpMocks.createRequest({
            method: 'POST',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.validateUsersEmailPhone(req, res)
        assert.equal(res.statusCode, 405);
        const data = res._getJSONData();
        assert.equal(data.message, 'Only GET requests are accepted!');
        assert.equal(data.code, 405);
    });
    // User no longer found; skipping test until more reliable option available
    it.skip('Should find a user', async () => {
        const req = httpMocks.createRequest({
            method: 'GET',
            query: {
                email: 'test3@team617106.testinator.com'
            },
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
        const res = httpMocks.createResponse();
        await functions.validateUsersEmailPhone(req, res);

        assert.equal(res.statusCode, 200);
        const {data, code} = res._getJSONData();
        assert.equal(code, 200);
        assert.equal(data.accountExists, true); 
    });
    it('Should NOT find a user', async () => {
        const req = httpMocks.createRequest({
            method: 'GET',
            query: {
                email: 'nonexistent@team617106.testinator.com'
            },
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
        const res = httpMocks.createResponse();
        await functions.validateUsersEmailPhone(req, res);

        assert.equal(res.statusCode, 200);
        const {data, code} = res._getJSONData();
        assert.equal(code, 200);
        assert.equal(data.accountExists, false); 
    });
});

describe('getFilteredParticipants', async () => {
    it('Should return 200 for options', async () => {
        const req = httpMocks.createRequest({
            method: 'OPTIONS',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.getFilteredParticipants(req, res)
        assert.equal(res.statusCode, 200);
        const data = res._getJSONData();
        assert.equal(data.code, 200);
    });
    it('Should only accept GET', async () => {
        const req = httpMocks.createRequest({
            method: 'POST',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.getFilteredParticipants(req, res)
        assert.equal(res.statusCode, 405);
        const data = res._getJSONData();
        assert.equal(data.message, 'Only GET requests are accepted!');
        assert.equal(data.code, 405);
    });
});

describe('getParticipants', () => {
    it('Should return 200 for options', async () => {
        const req = httpMocks.createRequest({
            method: 'OPTIONS',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.getParticipants(req, res)
        assert.equal(res.statusCode, 200);
        const data = res._getJSONData();
        assert.equal(data.code, 200);
    });
    it('Should only accept GET', async () => {
        const req = httpMocks.createRequest({
            method: 'POST',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.getParticipants(req, res)
        assert.equal(res.statusCode, 405);
        const data = res._getJSONData();
        assert.equal(data.message, 'Only GET requests are accepted!');
        assert.equal(data.code, 405);
    });
    it('Parent should find participants for all sites', async () => {
        const siteCodes = Object.keys(conceptIds.siteLoginMap).map(key => conceptIds.siteLoginMap[key]);
        const req = httpMocks.createRequest({
            method: 'GET',
            query: {
                type: 'all',
                siteCodes
            },
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        const authObj = {
            isParent: true,
            siteCodes
        };
        await functions.getParticipants(req, res, authObj);
        assert.equal(res.statusCode, 200);
        const {data, code, limit, dataSize} = res._getJSONData();
        assert.equal(code, 200);
        assert.equal(data.length, dataSize);
        assert.equal(limit, 100);
        assert.equal(dataSize > 0, true);
    });
    it('Non-parent should find participants for NIH site', async () => {
        const siteCodes = Object.keys(conceptIds.siteLoginMap).map(key => conceptIds.siteLoginMap[key]);
        const req = httpMocks.createRequest({
            method: 'GET',
            query: {
                type: 'all',
                siteCode: 13
            },
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        const authObj = {
            isParent: false,
            siteCodes: 13
        };
        await functions.getParticipants(req, res, authObj);
        assert.equal(res.statusCode, 200);
        const {data, code, limit, dataSize} = res._getJSONData();
        assert.equal(code, 200);
        assert.equal(data.length, dataSize);
        assert.equal(limit, 100);
        assert.equal(dataSize > 0, true);
    });
});

describe('identifyParticipant', async () => {
    it('Should return 200 for options', async () => {
        const req = httpMocks.createRequest({
            method: 'OPTIONS',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.identifyParticipant(req, res)
        assert.equal(res.statusCode, 200);
        const data = res._getJSONData();
        assert.equal(data.code, 200);
    });
    it('Should only accept GET or POST', async () => {
        const req = httpMocks.createRequest({
            method: 'PUT',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.identifyParticipant(req, res)
        assert.equal(res.statusCode, 405);
        const data = res._getJSONData();
        assert.equal(data.message, 'Only GET or POST requests are accepted!');
        assert.equal(data.code, 405);
    });
});

describe('submitParticipantsData', async () => {
    it('Should return 200 for options', async () => {
        const req = httpMocks.createRequest({
            method: 'OPTIONS',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.submitParticipantsData(req, res)
        assert.equal(res.statusCode, 200);
        const data = res._getJSONData();
        assert.equal(data.code, 200);
    });
    it('Should only accept POST', async () => {
        const req = httpMocks.createRequest({
            method: 'GET',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.submitParticipantsData(req, res)
        assert.equal(res.statusCode, 405);
        const data = res._getJSONData();
        assert.equal(data.message, 'Only POST requests are accepted!');
        assert.equal(data.code, 405);
    });
});

describe('updateParticipantData', async () => {
    it('Should return 200 for options', async () => {
        const req = httpMocks.createRequest({
            method: 'OPTIONS',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.updateParticipantData(req, res)
        assert.equal(res.statusCode, 200);
        const data = res._getJSONData();
        assert.equal(data.code, 200);
    });
    it('Should only accept POST', async () => {
        const req = httpMocks.createRequest({
            method: 'GET',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.updateParticipantData(req, res)
        assert.equal(res.statusCode, 405);
        const data = res._getJSONData();
        assert.equal(data.message, 'Only POST requests are accepted!');
        assert.equal(data.code, 405);
    });
});

describe('getParticipantNotification', async () => {
    it('Should return 200 for options', async () => {
        const req = httpMocks.createRequest({
            method: 'OPTIONS',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.getParticipantNotification(req, res)
        assert.equal(res.statusCode, 200);
        const data = res._getJSONData();
        assert.equal(data.code, 200);
    });
    it('Should only accept GET', async () => {
        const req = httpMocks.createRequest({
            method: 'POST',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.getParticipantNotification(req, res)
        assert.equal(res.statusCode, 405);
        const data = res._getJSONData();
        assert.equal(data.message, 'Only GET requests are accepted!');
        assert.equal(data.code, 405);
    });
});

describe('dashboard', async () => {
    it('Should return 200 for options', async () => {
        const req = httpMocks.createRequest({
            method: 'OPTIONS',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.dashboard(req, res)
        assert.equal(res.statusCode, 200);
        const data = res._getJSONData();
        assert.equal(data.code, 200);
    });
    it('Should reject unauthorized request', async () => {
        const req = httpMocks.createRequest({
            method: 'GET',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.dashboard(req, res)
        assert.equal(res.statusCode, 401);
    });
});

describe('app', async () => {
    it('Should return 200 for options', async () => {
        const req = httpMocks.createRequest({
            method: 'OPTIONS',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.app(req, res)
        assert.equal(res.statusCode, 200);
        const data = res._getJSONData();
        assert.equal(data.code, 200);
    });
    it('Should reject unauthorized request', async () => {
        const req = httpMocks.createRequest({
            method: 'GET',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.app(req, res)
        assert.equal(res.statusCode, 401);
    });
});

describe('biospecimen', async () => {
    it('Should return 200 for options', async () => {
        const req = httpMocks.createRequest({
            method: 'OPTIONS',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.app(req, res)
        assert.equal(res.statusCode, 200);
        const data = res._getJSONData();
        assert.equal(data.code, 200);
    });
    it('Should reject unauthorized request', async () => {
        const req = httpMocks.createRequest({
            method: 'GET',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.app(req, res)
        assert.equal(res.statusCode, 401);
    });

    describe('processParticipantHomeMouthwashKitData', () => {
        const { collectionDetails, baseline, bioKitMouthwash, firstName, lastName, isPOBox, address1, address2, physicalAddress1, physicalAddress2, city, state, zip, physicalCity, physicalState, physicalZip, yes, no } = fieldToConceptIdMapping;
        it('Should return null for PO boxes', () => {
            const result1 = shared.processParticipantHomeMouthwashKitData({
                [address1]: 'PO Box 1033'
            }, false);
            assert.equal(result1, null);
            const result2 = shared.processParticipantHomeMouthwashKitData({
                [address1]: 'P.O. Box 1033'
            }, false);
            assert.equal(result2, null);
            const result3 = shared.processParticipantHomeMouthwashKitData({
                [address1]: 'po box 1033'
            }, false);
            assert.equal(result3, null);
            const result4 = shared.processParticipantHomeMouthwashKitData({
                [address1]: 'p.o. Box 1033'
            }, false);
            assert.equal(result4, null);
            const result5 = shared.processParticipantHomeMouthwashKitData({
                [address1]: 'Post Office Box 1033'
            }, false);
            assert.equal(result5, null);
            const result6 = shared.processParticipantHomeMouthwashKitData({
                [address1]: 'post office box 1033'
            }, false);
            assert.equal(result6, null);
        });

        it('Should return empty array if printLabel is false and record does not have mouthwash', () => {
            const result = shared.processParticipantHomeMouthwashKitData({
                [address1]: '123 Fake Street',
                [collectionDetails]: {
                    [baseline]: {
                        [bioKitMouthwash]: undefined
                    }
                }
            }, false);
            assert.equal(Array.isArray(result), true);
            assert.equal(result.length, 0);
        });

        it('Should return record if record has no mouthwash but printLabel is true', () => {
            const record = {
                [firstName]: 'First',
                [lastName]: 'Last',
                [address1]: '123 Fake Street',
                [city]: 'City',
                [state]: 'PA',
                [zip]: '19104',
                'Connect_ID': 123456789,
                [collectionDetails]: {
                    [baseline]: {
                        [bioKitMouthwash]: undefined
                    }
                }
            };
            const result = shared.processParticipantHomeMouthwashKitData(record, true);
            assert.equal(result.first_name, record[firstName]);
            assert.equal(result.last_name, record[lastName]);
            assert.equal(result.address_1, record[address1]);
            assert.equal(result.address_2, '');
            assert.equal(result.city, record[city]);
            assert.equal(result.state, record[state]);
            assert.equal(result.zip_code, record[zip]);
            assert.equal(result.connect_id, record['Connect_ID']);
        });

        it('Should return record if printLabel is false but record has mouthwash', () => {
            const record = {
                [firstName]: 'First',
                [lastName]: 'Last',
                [address1]: '123 Fake Street',
                [city]: 'City',
                [state]: 'PA',
                [zip]: '19104',
                'Connect_ID': 123456789,
                [collectionDetails]: {
                    [baseline]: {
                        [bioKitMouthwash]: fieldToConceptIdMapping.yes
                    }
                }
            };
            const result = shared.processParticipantHomeMouthwashKitData(record, false);
            assert.equal(result.first_name, record[firstName]);
            assert.equal(result.last_name, record[lastName]);
            assert.equal(result.address_1, record[address1]);
            assert.equal(result.address_2, '');
            assert.equal(result.city, record[city]);
            assert.equal(result.state, record[state]);
            assert.equal(result.zip_code, record[zip]);
            assert.equal(result.connect_id, record['Connect_ID']);
        });

        it('Should return record if printLabel is true and record has mouthwash', () => {
            const record = {
                [firstName]: 'First',
                [lastName]: 'Last',
                [address1]: '123 Fake Street',
                [city]: 'City',
                [state]: 'PA',
                [zip]: '19104',
                'Connect_ID': 123456789,
                [collectionDetails]: {
                    [baseline]: {
                        [bioKitMouthwash]: fieldToConceptIdMapping.yes
                    }
                }
            };
            const result = shared.processParticipantHomeMouthwashKitData(record, true);
            assert.equal(result.first_name, record[firstName]);
            assert.equal(result.last_name, record[lastName]);
            assert.equal(result.address_1, record[address1]);
            assert.equal(result.address_2, '');
            assert.equal(result.city, record[city]);
            assert.equal(result.state, record[state]);
            assert.equal(result.zip_code, record[zip]);
            assert.equal(result.connect_id, record['Connect_ID']);
        });

        it('Should use physical address if physical address is provided even if mailing address is not a PO Box', () => {
            const result1 = shared.processParticipantHomeMouthwashKitData({
                [firstName]: 'First',
                [lastName]: 'Last',
                [isPOBox]: no,
                [address1]: '321 Physical Street',
                [physicalAddress1]: '123 Fake St',
                [physicalCity]: 'City',
                [physicalState]: 'PA',
                [physicalZip]: '19104',
                'Connect_ID': 123456789,
                [collectionDetails]: {
                    [baseline]: {
                        [bioKitMouthwash]: undefined
                    }
                }
            }, true);
            assert.deepEqual(result1, {
                first_name: 'First',
                last_name: 'Last',
                requestDate: undefined,
                visit: 'BL',
                connect_id: 123456789,
                address_1: '123 Fake St',
                address_2: '',
                city: 'City',
                state: 'PA',
                zip_code: '19104'
              });
        });

        it('Should use physical address if primary address is marked as PO box', () => {
            const result1 = shared.processParticipantHomeMouthwashKitData({
                [firstName]: 'First',
                [lastName]: 'Last',
                [isPOBox]: yes,
                [address1]: 'Pno Box 1033',
                [physicalAddress1]: '123 Fake St',
                [physicalCity]: 'City',
                [physicalState]: 'PA',
                [physicalZip]: '19104',
                'Connect_ID': 123456789,
                [collectionDetails]: {
                    [baseline]: {
                        [bioKitMouthwash]: undefined
                    }
                }
            }, true);
            assert.deepEqual(result1, {
                first_name: 'First',
                last_name: 'Last',
                requestDate: undefined,
                visit: 'BL',
                connect_id: 123456789,
                address_1: '123 Fake St',
                address_2: '',
                city: 'City',
                state: 'PA',
                zip_code: '19104'
              });
        });

        it('Should use physical address if primary address is not marked as PO box but matches pattern', () => {
            const result1 = shared.processParticipantHomeMouthwashKitData({
                [firstName]: 'First',
                [lastName]: 'Last',
                requestDate: undefined,
                visit: 'BL',
                [address1]: 'PO Box 1033',
                [physicalAddress1]: '123 Fake St',
                [physicalCity]: 'City',
                [physicalState]: 'PA',
                [physicalZip]: '19104',
                'Connect_ID': 123456789,
                [collectionDetails]: {
                    [baseline]: {
                        [bioKitMouthwash]: undefined
                    }
                }
            }, true);
            assert.deepEqual(result1, {
                first_name: 'First',
                last_name: 'Last',
                requestDate: undefined,
                visit: 'BL',
                connect_id: 123456789,
                address_1: '123 Fake St',
                address_2: '',
                city: 'City',
                state: 'PA',
                zip_code: '19104'
              });
        });

        it('Should use mailing address if physical address is a PO Box and mailing is not', () => {
            const result1 = shared.processParticipantHomeMouthwashKitData({
                [firstName]: 'First',
                [lastName]: 'Last',
                [isPOBox]: no,
                [address1]: '123 Fake St',
                [city]: 'City',
                [state]: 'PA',
                [zip]: '19104',
                [physicalAddress1]: 'PO Box 1033',
                [physicalCity]: 'City',
                [physicalState]: 'PA',
                [physicalZip]: '17102',
                'Connect_ID': 123456789,
                [collectionDetails]: {
                    [baseline]: {
                        [bioKitMouthwash]: undefined
                    }
                }
            }, true);
            assert.deepEqual(result1, {
                first_name: 'First',
                last_name: 'Last',
                requestDate: undefined,
                visit: 'BL',
                connect_id: 123456789,
                address_1: '123 Fake St',
                address_2: '',
                city: 'City',
                state: 'PA',
                zip_code: '19104'
              });
        });

        it('Should return null if physical address and mailing addresses are PO Boxes', () => {
            const result1 = shared.processParticipantHomeMouthwashKitData({
                [firstName]: 'First',
                [lastName]: 'Last',
                [address1]: 'PO Box 1033',
                [physicalAddress1]: 'PO Box 1033',
                [physicalCity]: 'City',
                [physicalState]: 'PA',
                [physicalZip]: '19104',
                'Connect_ID': 123456789,
                [collectionDetails]: {
                    [baseline]: {
                        [bioKitMouthwash]: undefined
                    }
                }
            }, true);
            const result2 = shared.processParticipantHomeMouthwashKitData({
                [firstName]: 'First',
                [lastName]: 'Last',
                [isPOBox]: yes,
                [address1]: 'PznO Box 1033',
                [physicalAddress1]: 'PO Box 1033',
                [physicalCity]: 'City',
                [physicalState]: 'PA',
                [physicalZip]: '19104',
                'Connect_ID': 123456789,
                [collectionDetails]: {
                    [baseline]: {
                        [bioKitMouthwash]: undefined
                    }
                }
            }, true);
            assert.equal(result1, null);
            assert.equal(result2, null);
        });
        
    });

    describe('processMouthwashEligibility', async () => {
        it('Should set kitStatus to initialized with missing bioKitMouthwash object', () => {
            let data = {
                [fieldToConceptIdMapping.withdrawConsent]: fieldToConceptIdMapping.no,
                [fieldToConceptIdMapping.participantDeceasedNORC]: fieldToConceptIdMapping.no,
                [fieldToConceptIdMapping.activityParticipantRefusal]: {
                    [fieldToConceptIdMapping.baselineMouthwashSample]: fieldToConceptIdMapping.no
                },
                [fieldToConceptIdMapping.collectionDetails]: {
                    [fieldToConceptIdMapping.baseline]: {
                        [fieldToConceptIdMapping.bloodOrUrineCollected]: fieldToConceptIdMapping.yes,
                        [fieldToConceptIdMapping.bloodOrUrineCollectedTimestamp]: '2024-09-27T00:00:00.000Z'
                    }
                }
            };
            const updates = validation.processMouthwashEligibility(data);
            assert.equal(updates[`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwash}.${fieldToConceptIdMapping.kitStatus}`], fieldToConceptIdMapping.initialized);
        });

        it('Should set kitStatus to initialized with bioKitMouthwash object present and no kitStatus', () => {
            let data = {
                [fieldToConceptIdMapping.withdrawConsent]: fieldToConceptIdMapping.no,
                [fieldToConceptIdMapping.participantDeceasedNORC]: fieldToConceptIdMapping.no,
                [fieldToConceptIdMapping.activityParticipantRefusal]: {
                    [fieldToConceptIdMapping.baselineMouthwashSample]: fieldToConceptIdMapping.no
                },
                [fieldToConceptIdMapping.collectionDetails]: {
                    [fieldToConceptIdMapping.baseline]: {
                        [fieldToConceptIdMapping.bloodOrUrineCollected]: fieldToConceptIdMapping.yes,
                        [fieldToConceptIdMapping.bloodOrUrineCollectedTimestamp]: '2024-09-27T00:00:00.000Z',
                        [fieldToConceptIdMapping.bioKitMouthwash]: {

                        }
                    }
                },
            };
            const updates = validation.processMouthwashEligibility(data);
            assert.equal(updates[`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwash}.${fieldToConceptIdMapping.kitStatus}`], fieldToConceptIdMapping.initialized);
        });

        it('Should set kitStatus to initialized with bioKitMouthwash object absent', () => {
            let data = {
                [fieldToConceptIdMapping.withdrawConsent]: fieldToConceptIdMapping.no,
                [fieldToConceptIdMapping.participantDeceasedNORC]: fieldToConceptIdMapping.no,
                [fieldToConceptIdMapping.activityParticipantRefusal]: {
                    [fieldToConceptIdMapping.baselineMouthwashSample]: fieldToConceptIdMapping.no
                },
                [fieldToConceptIdMapping.collectionDetails]: {
                    [fieldToConceptIdMapping.baseline]: {
                        [fieldToConceptIdMapping.bloodOrUrineCollected]: fieldToConceptIdMapping.yes,
                        [fieldToConceptIdMapping.bloodOrUrineCollectedTimestamp]: '2024-09-27T00:00:00.000Z'
                    }
                },
            };
            const updates = validation.processMouthwashEligibility(data);
            assert.equal(updates[`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwash}.${fieldToConceptIdMapping.kitStatus}`], fieldToConceptIdMapping.initialized);
        });

        it('Should not set kitStatus because participant withdrew consent', () => {
            let data = {
                [fieldToConceptIdMapping.withdrawConsent]: fieldToConceptIdMapping.yes,
                [fieldToConceptIdMapping.participantDeceasedNORC]: fieldToConceptIdMapping.no,
                [fieldToConceptIdMapping.activityParticipantRefusal]: {
                    [fieldToConceptIdMapping.baselineMouthwashSample]: fieldToConceptIdMapping.no
                },
                [fieldToConceptIdMapping.collectionDetails]: {
                    [fieldToConceptIdMapping.baseline]: {
                        [fieldToConceptIdMapping.bloodOrUrineCollected]: fieldToConceptIdMapping.yes,
                        [fieldToConceptIdMapping.bloodOrUrineCollectedTimestamp]: '2024-09-27T00:00:00.000Z',
                        [fieldToConceptIdMapping.bioKitMouthwash]: {

                        }
                    }
                },
            };
            const updates = validation.processMouthwashEligibility(data);
            assert.equal(Object.keys(updates).length, 0);
        });

        it('Should not set kitStatus because participant is deceased', () => {
            let data = {
                [fieldToConceptIdMapping.withdrawConsent]: fieldToConceptIdMapping.no,
                [fieldToConceptIdMapping.participantDeceasedNORC]: fieldToConceptIdMapping.yes,
                [fieldToConceptIdMapping.activityParticipantRefusal]: {
                    [fieldToConceptIdMapping.baselineMouthwashSample]: fieldToConceptIdMapping.no
                },
                [fieldToConceptIdMapping.collectionDetails]: {
                    [fieldToConceptIdMapping.baseline]: {
                        [fieldToConceptIdMapping.bloodOrUrineCollected]: fieldToConceptIdMapping.yes,
                        [fieldToConceptIdMapping.bloodOrUrineCollectedTimestamp]: '2024-09-27T00:00:00.000Z',
                        [fieldToConceptIdMapping.bioKitMouthwash]: {

                        }
                    }
                },
            };
            const updates = validation.processMouthwashEligibility(data);
            assert.equal(Object.keys(updates).length, 0);
        });

        it('Should not set kitStatus because participant refused baseline mouthwash', () => {
            let data = {
                [fieldToConceptIdMapping.withdrawConsent]: fieldToConceptIdMapping.no,
                [fieldToConceptIdMapping.participantDeceasedNORC]: fieldToConceptIdMapping.no,
                [fieldToConceptIdMapping.activityParticipantRefusal]: {
                    [fieldToConceptIdMapping.baselineMouthwashSample]: fieldToConceptIdMapping.yes
                },
                [fieldToConceptIdMapping.collectionDetails]: {
                    [fieldToConceptIdMapping.baseline]: {
                        [fieldToConceptIdMapping.bloodOrUrineCollected]: fieldToConceptIdMapping.yes,
                        [fieldToConceptIdMapping.bloodOrUrineCollectedTimestamp]: '2024-09-27T00:00:00.000Z',
                        [fieldToConceptIdMapping.bioKitMouthwash]: {

                        }
                    }
                },
            };
            const updates = validation.processMouthwashEligibility(data);
            assert.equal(Object.keys(updates).length, 0);
        });

        it('Should not set kitStatus because participant blood or urine not collected', () => {
            let data = {
                [fieldToConceptIdMapping.withdrawConsent]: fieldToConceptIdMapping.no,
                [fieldToConceptIdMapping.participantDeceasedNORC]: fieldToConceptIdMapping.no,
                [fieldToConceptIdMapping.activityParticipantRefusal]: {
                    [fieldToConceptIdMapping.baselineMouthwashSample]: fieldToConceptIdMapping.no
                },
                [fieldToConceptIdMapping.collectionDetails]: {
                    [fieldToConceptIdMapping.baseline]: {
                        [fieldToConceptIdMapping.bloodOrUrineCollected]: fieldToConceptIdMapping.no,
                        [fieldToConceptIdMapping.bioKitMouthwash]: {

                        }
                    }
                },
            };
            const updates = validation.processMouthwashEligibility(data);
            assert.equal(Object.keys(updates).length, 0);
        });

        it('Should not set kitStatus because participant blood or urine collected  before April 1 2024', () => {
            let data = {
                [fieldToConceptIdMapping.withdrawConsent]: fieldToConceptIdMapping.no,
                [fieldToConceptIdMapping.participantDeceasedNORC]: fieldToConceptIdMapping.no,
                [fieldToConceptIdMapping.activityParticipantRefusal]: {
                    [fieldToConceptIdMapping.baselineMouthwashSample]: fieldToConceptIdMapping.yes
                },
                [fieldToConceptIdMapping.collectionDetails]: {
                    [fieldToConceptIdMapping.baseline]: {
                        [fieldToConceptIdMapping.bloodOrUrineCollected]: fieldToConceptIdMapping.yes,
                        [fieldToConceptIdMapping.bloodOrUrineCollectedTimestamp]: '2023-09-27T00:00:00.000Z',
                        [fieldToConceptIdMapping.bioKitMouthwash]: {

                        }
                    }
                },
            };
            const updates = validation.processMouthwashEligibility(data);
            assert.equal(Object.keys(updates).length, 0);
        });

        it('Should clear kitStatus because participant has P.O. box', () => {
            let data = {
                [fieldToConceptIdMapping.withdrawConsent]: fieldToConceptIdMapping.no,
                [fieldToConceptIdMapping.participantDeceasedNORC]: fieldToConceptIdMapping.no,
                [fieldToConceptIdMapping.activityParticipantRefusal]: {
                    [fieldToConceptIdMapping.baselineMouthwashSample]: fieldToConceptIdMapping.no
                },
                [fieldToConceptIdMapping.collectionDetails]: {
                    [fieldToConceptIdMapping.baseline]: {
                        [fieldToConceptIdMapping.bloodOrUrineCollected]: fieldToConceptIdMapping.yes,
                        [fieldToConceptIdMapping.bloodOrUrineCollectedTimestamp]: '2024-09-27T00:00:00.000Z',
                        [fieldToConceptIdMapping.bioKitMouthwash]: {
                            [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.initialized
                        }
                    }
                },
                [fieldToConceptIdMapping.address1]: 'PO Box 1033'

            };
            const updates = validation.processMouthwashEligibility(data);
            assert.equal(updates[`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwash}.${fieldToConceptIdMapping.kitStatus}`], undefined);
        });

        it('Should not set kitStatus because participant kit has already shipped', () => {
            let data = {
                [fieldToConceptIdMapping.withdrawConsent]: fieldToConceptIdMapping.no,
                [fieldToConceptIdMapping.participantDeceasedNORC]: fieldToConceptIdMapping.no,
                [fieldToConceptIdMapping.activityParticipantRefusal]: {
                    [fieldToConceptIdMapping.baselineMouthwashSample]: fieldToConceptIdMapping.no
                },
                [fieldToConceptIdMapping.collectionDetails]: {
                    [fieldToConceptIdMapping.baseline]: {
                        [fieldToConceptIdMapping.bloodOrUrineCollected]: fieldToConceptIdMapping.yes,
                        [fieldToConceptIdMapping.bloodOrUrineCollectedTimestamp]: '2024-09-27T00:00:00.000Z',
                        [fieldToConceptIdMapping.bioKitMouthwash]: {
                            [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.shipped
                        }
                    }
                },
            };
            const updates = validation.processMouthwashEligibility(data);
            assert.deepEqual(updates, {});
        });

        const testCasesFromDev = [
            // These are the participants as logged out after the relevant logic has run, not before
            // However large parts of their data are likely still useful
        ];
    });

    describe('getHomeMWReplacementKitData', () => {
        describe('First replacement kit', () => {
            it('Should set a first replacement kit', () => {
                const statuses = [
                    fieldToConceptIdMapping.shipped,
                    fieldToConceptIdMapping.received,
                ];
                const baseObj = {
                    [fieldToConceptIdMapping.firstName]: 'First',
                    [fieldToConceptIdMapping.lastName]: 'Last',
                    [fieldToConceptIdMapping.address1]: '123 Fake Street',
                    [fieldToConceptIdMapping.city]: 'City',
                    [fieldToConceptIdMapping.state]: 'PA',
                    [fieldToConceptIdMapping.zip]: '19104',
                    'Connect_ID': 123456789
                }
                statuses.forEach(status => {
                    const data = Object.assign({}, baseObj, {
                        [fieldToConceptIdMapping.collectionDetails]: {
                            [fieldToConceptIdMapping.baseline]: {
                                [fieldToConceptIdMapping.bioKitMouthwash]: {
                                    [fieldToConceptIdMapping.kitStatus]: status
                                }
                            }
                        }
                    });
                    const updates = shared.getHomeMWReplacementKitData(data);
                    const clonedUpdates = Object.assign({}, updates);
                    delete clonedUpdates[`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwashBL1}.${fieldToConceptIdMapping.dateKitRequested}`];
                    assert.closeTo
                        (+new Date(updates[`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwashBL1}.${fieldToConceptIdMapping.dateKitRequested}`]),
                        +new Date(), 
                        60000, 
                        'Date kit requested is within a minute of test completion'
                    );
                    // console.log('updates', updates);
                    assert.deepEqual(clonedUpdates, {
                        [`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwashBL1}.${fieldToConceptIdMapping.kitType}`]: 976461859,
                        [`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwashBL1}.${fieldToConceptIdMapping.kitStatus}`]: 728267588
                      },
                       `Kit status ${status} eligible for replacement kit`);
                });
            });
            it('Should prevent participant with a pending home MW kit from obtaining a replacement', () => {
                const data = {
                    [fieldToConceptIdMapping.firstName]: 'First',
                    [fieldToConceptIdMapping.lastName]: 'Last',
                    [fieldToConceptIdMapping.address1]: '123 Fake Street',
                    [fieldToConceptIdMapping.city]: 'City',
                    [fieldToConceptIdMapping.state]: 'PA',
                    [fieldToConceptIdMapping.zip]: '19104',
                    'Connect_ID': 123456789,
                    [fieldToConceptIdMapping.collectionDetails]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.bioKitMouthwash]: {
                                [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.pending
                            }
                        }
                    }
                };
    
                expect(shared.getHomeMWReplacementKitData.bind(null, data)).to.throw(/This participant is not yet eligible for a home mouthwash kit/gi);
            });
            it('Should prevent a participant whose initial home MW kit has not been sent from obtaining a replacement', () => {
                const statuses = [
                    fieldToConceptIdMapping.initialized,
                    fieldToConceptIdMapping.addressPrinted,
                    fieldToConceptIdMapping.assigned,
                ];
                const baseObj = {
                    [fieldToConceptIdMapping.firstName]: 'First',
                    [fieldToConceptIdMapping.lastName]: 'Last',
                    [fieldToConceptIdMapping.address1]: '123 Fake Street',
                    [fieldToConceptIdMapping.city]: 'City',
                    [fieldToConceptIdMapping.state]: 'PA',
                    [fieldToConceptIdMapping.zip]: '19104',
                    'Connect_ID': 123456789,
                };
                statuses.forEach(status => {
                    const data = Object.assign({}, baseObj, {
                        [fieldToConceptIdMapping.collectionDetails]: {
                            [fieldToConceptIdMapping.baseline]: {
                                [fieldToConceptIdMapping.bioKitMouthwash]: {
                                    [fieldToConceptIdMapping.kitStatus]: status
                                }
                            }
                        }
                    });
        
                    expect(shared.getHomeMWReplacementKitData.bind(null, data)).to.throw(/This participant\'s initial home mouthwash kit has not been sent/);
                });
                
            });
            it('Should throw error on unrecognized kitStatus for initial home MW kit', () => {
                const data = {
                    [fieldToConceptIdMapping.firstName]: 'First',
                    [fieldToConceptIdMapping.lastName]: 'Last',
                    [fieldToConceptIdMapping.address1]: '123 Fake Street',
                    [fieldToConceptIdMapping.city]: 'City',
                    [fieldToConceptIdMapping.state]: 'PA',
                    [fieldToConceptIdMapping.zip]: '19104',
                    'Connect_ID': 123456789,
                    [fieldToConceptIdMapping.collectionDetails]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.bioKitMouthwash]: {
                                [fieldToConceptIdMapping.kitStatus]: 'fake'
                            }
                        }
                    }
                };
    
                expect(shared.getHomeMWReplacementKitData.bind(null, data)).to.throw(/Unrecognized kit status fake/);
            });
        });

        describe('Second replacement kit', () => {
            it('Should set a second replacement kit', () => {
                const statuses = [
                    fieldToConceptIdMapping.shipped,
                    fieldToConceptIdMapping.received,
                ];
                const baseObj = {
                    [fieldToConceptIdMapping.firstName]: 'First',
                    [fieldToConceptIdMapping.lastName]: 'Last',
                    [fieldToConceptIdMapping.address1]: '123 Fake Street',
                    [fieldToConceptIdMapping.city]: 'City',
                    [fieldToConceptIdMapping.state]: 'PA',
                    [fieldToConceptIdMapping.zip]: '19104',
                    'Connect_ID': 123456789
                }
                statuses.forEach(status => {
                    const data = Object.assign({}, baseObj, {
                        [fieldToConceptIdMapping.collectionDetails]: {
                            [fieldToConceptIdMapping.baseline]: {
                                [fieldToConceptIdMapping.bioKitMouthwash]: {
                                    [fieldToConceptIdMapping.kitStatus]: status
                                },
                                [fieldToConceptIdMapping.bioKitMouthwashBL1]: {
                                    [fieldToConceptIdMapping.kitStatus]: status
                                }
                            }
                        }
                    });
                    const updates = shared.getHomeMWReplacementKitData(data);
                    const clonedUpdates = Object.assign({}, updates);
                    delete clonedUpdates[`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwashBL2}.${fieldToConceptIdMapping.dateKitRequested}`];
                    assert.closeTo
                        (+new Date(updates[`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwashBL2}.${fieldToConceptIdMapping.dateKitRequested}`]),
                        +new Date(), 
                        60000, 
                        'Date kit requested is within a minute of test completion'
                    );
        
                    // console.log('updates', updates);
                    assert.deepEqual(clonedUpdates, {
                        [`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwashBL2}.${fieldToConceptIdMapping.kitType}`]: 976461859,
                        [`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwashBL2}.${fieldToConceptIdMapping.kitStatus}`]: 728267588
                      },
                       `Kit status ${status} eligible for replacement kit`);
                });
            });
            it('Should prevent participant with a pending replacement home MW kit from obtaining a second replacement', () => {
                const data = {
                    [fieldToConceptIdMapping.firstName]: 'First',
                    [fieldToConceptIdMapping.lastName]: 'Last',
                    [fieldToConceptIdMapping.address1]: '123 Fake Street',
                    [fieldToConceptIdMapping.city]: 'City',
                    [fieldToConceptIdMapping.state]: 'PA',
                    [fieldToConceptIdMapping.zip]: '19104',
                    'Connect_ID': 123456789,
                    [fieldToConceptIdMapping.collectionDetails]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.bioKitMouthwash]: {
                                [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.addressPrinted
                            },
                            [fieldToConceptIdMapping.bioKitMouthwashBL1]: {
                                [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.pending
                            }
                        }
                    }
                };
    
                expect(shared.getHomeMWReplacementKitData.bind(null, data)).to.throw(/This participant is not eligible for a second replacement home mouthwash kit/gi);
            });
            it('Should prevent a participant whose first replacement home MW kit has not been sent from obtaining a second replacement', () => {
                const statuses = [
                    fieldToConceptIdMapping.initialized,
                    fieldToConceptIdMapping.addressPrinted,
                    fieldToConceptIdMapping.assigned,
                ];
                const baseObj = {
                    [fieldToConceptIdMapping.firstName]: 'First',
                    [fieldToConceptIdMapping.lastName]: 'Last',
                    [fieldToConceptIdMapping.address1]: '123 Fake Street',
                    [fieldToConceptIdMapping.city]: 'City',
                    [fieldToConceptIdMapping.state]: 'PA',
                    [fieldToConceptIdMapping.zip]: '19104',
                    'Connect_ID': 123456789,
                };
                statuses.forEach(status => {
                    const data = Object.assign({}, baseObj, {
                        [fieldToConceptIdMapping.collectionDetails]: {
                            [fieldToConceptIdMapping.baseline]: {
                                [fieldToConceptIdMapping.bioKitMouthwash]: {
                                    [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.addressPrinted
                                },
                                [fieldToConceptIdMapping.bioKitMouthwashBL1]: {
                                    [fieldToConceptIdMapping.kitStatus]: status
                                }
                            }
                        }
                    });
        
                    expect(shared.getHomeMWReplacementKitData.bind(null, data)).to.throw(/This participant\'s first replacement home mouthwash kit has not been sent/);
                });
    
            });
            it('Should throw error on unrecognized kitStatus for first replacement home MW kit', () => {
                const data = {
                    [fieldToConceptIdMapping.firstName]: 'First',
                    [fieldToConceptIdMapping.lastName]: 'Last',
                    [fieldToConceptIdMapping.address1]: '123 Fake Street',
                    [fieldToConceptIdMapping.city]: 'City',
                    [fieldToConceptIdMapping.state]: 'PA',
                    [fieldToConceptIdMapping.zip]: '19104',
                    'Connect_ID': 123456789,
                    [fieldToConceptIdMapping.collectionDetails]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.bioKitMouthwash]: {
                                [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.addressPrinted
                            },
                            [fieldToConceptIdMapping.bioKitMouthwashBL1]: {
                                [fieldToConceptIdMapping.kitStatus]: 'fake'
                            }
                        }
                    }
                };
    
                expect(shared.getHomeMWReplacementKitData.bind(null, data)).to.throw(/Unrecognized kit status fake/);
            });

        });
        it('Should prevent a participant who already has a second replacement kit from obtaining another', () => {
            const data = {
                [fieldToConceptIdMapping.firstName]: 'First',
                [fieldToConceptIdMapping.lastName]: 'Last',
                [fieldToConceptIdMapping.address1]: '123 Fake Street',
                [fieldToConceptIdMapping.city]: 'City',
                [fieldToConceptIdMapping.state]: 'PA',
                [fieldToConceptIdMapping.zip]: '19104',
                'Connect_ID': 123456789,
                [fieldToConceptIdMapping.collectionDetails]: {
                    [fieldToConceptIdMapping.baseline]: {
                        [fieldToConceptIdMapping.bioKitMouthwash]: {
                            [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.addressPrinted
                        },
                        [fieldToConceptIdMapping.bioKitMouthwashBL1]: {
                            [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.addressPrinted
                        },[fieldToConceptIdMapping.bioKitMouthwashBL2]: {
                            [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.initialized
                        }
                    }
                }
            };

            expect(shared.getHomeMWReplacementKitData.bind(null, data)).to.throw(/Participant has exceeded supported number of replacement kits./);
        });
        it('Should prevent a participant with an invalid address from obtaining a replacement kit', () => {
            const data = {
                [fieldToConceptIdMapping.firstName]: 'First',
                [fieldToConceptIdMapping.lastName]: 'Last',
                [fieldToConceptIdMapping.address1]: 'P.O. Box 1234',
                [fieldToConceptIdMapping.city]: 'City',
                [fieldToConceptIdMapping.state]: 'PA',
                [fieldToConceptIdMapping.zip]: '19104',
                'Connect_ID': 123456789,
                [fieldToConceptIdMapping.collectionDetails]: {
                    [fieldToConceptIdMapping.baseline]: {
                        [fieldToConceptIdMapping.bioKitMouthwash]: {
                            [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.addressPrinted
                        }
                    }
                }
            };
            expect(shared.getHomeMWReplacementKitData.bind(null, data)).to.throw(/Participant address information is invalid./);

        });
        
    });

    describe('updateBaselineData', async () => {
        const {updateBaselineData} = require('../utils/shared.js');
        it('Should not update if visit is neither baseline nor clinical', () => {
            const biospecimenData = {};
            const participantData = {};
            const siteTubesList = [];
            const participantUpdates = updateBaselineData(biospecimenData, participantData, siteTubesList)
            assert.deepEqual(participantUpdates, {});
        });
        it('Should not update if visit is baseline but not clinical', () => {
            const biospecimenData = {
                [fieldToConceptIdMapping.collectionSelectedVisit]: fieldToConceptIdMapping.baseline
            };
            const participantData = {};
            const siteTubesList = [];
            const participantUpdates = updateBaselineData(biospecimenData, participantData, siteTubesList)
            assert.deepEqual(participantUpdates, {});
        });
        it('Should not update if visit is clinical but not baseline', () => {
            const biospecimenData = {
                [fieldToConceptIdMapping.collectionSetting]: fieldToConceptIdMapping.clinical
            };
            const participantData = {};
            const siteTubesList = [];
            const participantUpdates = updateBaselineData(biospecimenData, participantData, siteTubesList)
            assert.deepEqual(participantUpdates, {});
        });
        it.skip('Should update if visit is clinical baseline visit', () => {
            const biospecimenData = {
                [fieldToConceptIdMapping.collectionSelectedVisit]: fieldToConceptIdMapping.baseline,
                [fieldToConceptIdMapping.collectionSetting]: fieldToConceptIdMapping.clinical
            };

            const siteTubesList = [
                {}, // blood tube
                {}, // urine tube
                {}, // mouthwash tube
            ];

        });
    })

    describe('checkDerivedVariables with beforeEach', async () => {
        let i = 0;
        const testInfo = [
            {
                label: 'incentiveEligible only, blood and urine refusal',
                participantInfo: {
                    [fieldToConceptIdMapping.dataDestruction.incentive]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.dataDestruction.incentiveEligible]: fieldToConceptIdMapping.no // incentiveEligible
                        }
                    },
                    // bloodUrine refusal updates
                    [fieldToConceptIdMapping.activityParticipantRefusal]: {
                        [fieldToConceptIdMapping.baselineBloodSampleRefused]: fieldToConceptIdMapping.yes,
                        [fieldToConceptIdMapping.baselineUrineSampleRefused]: fieldToConceptIdMapping.yes
                    },
                    [fieldToConceptIdMapping.dataDestruction.baselineSurveyStatusModuleBackgroundAndOverallHealthFlag]: fieldToConceptIdMapping.submitted, // module1
                    [fieldToConceptIdMapping.dataDestruction.baselineSurveyStatusModuleMedications]: fieldToConceptIdMapping.submitted, //module2
                    [fieldToConceptIdMapping.dataDestruction.baselineSurveyStatusModuleSmoking]: fieldToConceptIdMapping.submitted, //module3
                    [fieldToConceptIdMapping.dataDestruction.baselineSurveyStatusModuleWhereYouLiveAndWorkFlag]: fieldToConceptIdMapping.submitted, //module4
                    [fieldToConceptIdMapping.dataDestruction.baselineBloodSampleCollected]: fieldToConceptIdMapping.yes, // Baseline blood sample collected
                    state: {
                        uid: uuid.v4()
                    }
                },
                specimens: [],
                surveys: {},
                updatesHolder: undefined,
                expected: {
                    [fieldToConceptIdMapping.dataDestruction.anyRefusalOrWithdrawal]: fieldToConceptIdMapping.yes,
                    [fieldToConceptIdMapping.baselineBloodAndUrineIsRefused]: fieldToConceptIdMapping.yes,
                    [`${fieldToConceptIdMapping.dataDestruction.incentive}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.dataDestruction.incentiveEligible}`]: fieldToConceptIdMapping.yes,
                    [`${fieldToConceptIdMapping.dataDestruction.incentive}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.dataDestruction.norcIncentiveEligible}`]: fieldToConceptIdMapping.yes
                }
            }, 
            {
                label: 'incentiveEligible only, no blood and urine refusal',
                participantInfo: {
                    [fieldToConceptIdMapping.dataDestruction.incentive]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.dataDestruction.incentiveEligible]: fieldToConceptIdMapping.no // incentiveEligible
                        }
                    },
                    // no bloodUrine refusal updates
                    [fieldToConceptIdMapping.activityParticipantRefusal]: {
                        [fieldToConceptIdMapping.baselineBloodSampleRefused]: fieldToConceptIdMapping.no,
                        [fieldToConceptIdMapping.baselineUrineSampleRefused]: fieldToConceptIdMapping.no
                    },
                    // Interestingly, this only works if this is explicitly set to no
                    // If it is undefined it is treated as yes
                    // and if it is yes it is never changed
                    [fieldToConceptIdMapping.dataDestruction.anyRefusalOrWithdrawal]: fieldToConceptIdMapping.no,
                    [fieldToConceptIdMapping.dataDestruction.baselineSurveyStatusModuleBackgroundAndOverallHealthFlag]: fieldToConceptIdMapping.submitted, // module1
                    [fieldToConceptIdMapping.dataDestruction.baselineSurveyStatusModuleMedications]: fieldToConceptIdMapping.submitted, //module2
                    [fieldToConceptIdMapping.dataDestruction.baselineSurveyStatusModuleSmoking]: fieldToConceptIdMapping.submitted, //module3
                    [fieldToConceptIdMapping.dataDestruction.baselineSurveyStatusModuleWhereYouLiveAndWorkFlag]: fieldToConceptIdMapping.submitted, //module4
                    [fieldToConceptIdMapping.dataDestruction.baselineBloodSampleCollected]: fieldToConceptIdMapping.yes, // Baseline blood sample collected
                    state: {
                        uid: uuid.v4()
                    }
                },
                specimens: [],
                surveys: {},
                updatesHolder: undefined,
                expected: {
                    [fieldToConceptIdMapping.baselineBloodAndUrineIsRefused]: fieldToConceptIdMapping.no,
                    [`${fieldToConceptIdMapping.dataDestruction.incentive}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.dataDestruction.incentiveEligible}`]: fieldToConceptIdMapping.yes,
                    [`${fieldToConceptIdMapping.dataDestruction.incentive}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.dataDestruction.norcIncentiveEligible}`]: fieldToConceptIdMapping.yes
                }
            },
            {
                label: 'incentiveEligible only, clinical blood collection case, blood and urine refusal',
                participantInfo: {
                    [fieldToConceptIdMapping.dataDestruction.incentive]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.dataDestruction.incentiveEligible]: fieldToConceptIdMapping.no // incentiveEligible
                        }
                    },
                    // bloodUrine refusal updates
                    [fieldToConceptIdMapping.activityParticipantRefusal]: {
                        [fieldToConceptIdMapping.baselineBloodSampleRefused]: fieldToConceptIdMapping.yes,
                        [fieldToConceptIdMapping.baselineUrineSampleRefused]: fieldToConceptIdMapping.yes
                    },
                    [fieldToConceptIdMapping.dataDestruction.baselineSurveyStatusModuleBackgroundAndOverallHealthFlag]: fieldToConceptIdMapping.submitted, // module1
                    [fieldToConceptIdMapping.dataDestruction.baselineSurveyStatusModuleMedications]: fieldToConceptIdMapping.submitted, //module2
                    [fieldToConceptIdMapping.dataDestruction.baselineSurveyStatusModuleSmoking]: fieldToConceptIdMapping.submitted, //module3
                    [fieldToConceptIdMapping.dataDestruction.baselineSurveyStatusModuleWhereYouLiveAndWorkFlag]: fieldToConceptIdMapping.submitted, //module4
                    // Second bloodCollected case
                    // This also triggers the calculateBaselineOrderPlaced case, resulting in additional update keys
                    // This combination will result in calculateBaselineOrderPlaced of true
                    [fieldToConceptIdMapping.collectionDetails]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.clinicalSiteBloodCollected]: fieldToConceptIdMapping.yes
                        }
                    },
                    state: {
                        uid: uuid.v4()
                    }
                },
                specimens: [],
                surveys: {},
                updatesHolder: undefined,
                expected: {
                    [fieldToConceptIdMapping.dataDestruction.anyRefusalOrWithdrawal]: fieldToConceptIdMapping.yes,
                    [fieldToConceptIdMapping.baselineBloodAndUrineIsRefused]: fieldToConceptIdMapping.yes,
                    [`${fieldToConceptIdMapping.dataDestruction.incentive}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.dataDestruction.incentiveEligible}`]: fieldToConceptIdMapping.yes,
                    [`${fieldToConceptIdMapping.dataDestruction.incentive}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.dataDestruction.norcIncentiveEligible}`]: fieldToConceptIdMapping.yes,
                    '173836415.266600170.880794013': 104430631,
                    '173836415.266600170.156605577': fieldToConceptIdMapping.yes
                }
            },
            {
                label: 'bloodCollected values not set, but research blood specimen for participant submitted',
                participantInfo: {
                    [fieldToConceptIdMapping.dataDestruction.incentive]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.dataDestruction.incentiveEligible]: fieldToConceptIdMapping.no // incentiveEligible
                        }
                    },
                    // bloodUrine refusal updates
                    [fieldToConceptIdMapping.activityParticipantRefusal]: {
                        [fieldToConceptIdMapping.baselineBloodSampleRefused]: fieldToConceptIdMapping.yes,
                        [fieldToConceptIdMapping.baselineUrineSampleRefused]: fieldToConceptIdMapping.yes
                    },
                    [fieldToConceptIdMapping.dataDestruction.baselineSurveyStatusModuleBackgroundAndOverallHealthFlag]: fieldToConceptIdMapping.submitted, // module1
                    [fieldToConceptIdMapping.dataDestruction.baselineSurveyStatusModuleMedications]: fieldToConceptIdMapping.submitted, //module2
                    [fieldToConceptIdMapping.dataDestruction.baselineSurveyStatusModuleSmoking]: fieldToConceptIdMapping.submitted, //module3
                    [fieldToConceptIdMapping.dataDestruction.baselineSurveyStatusModuleWhereYouLiveAndWorkFlag]: fieldToConceptIdMapping.submitted, //module4
                    state: {
                        uid: uuid.v4()
                    }
                },
                specimens: [{
                    ['331584571']:  266600170,
                    ['650516960']: 534621077,
                    ['299553921']: {
                        [883732523]: 'not 681745422'
                    }
                }],
                surveys: {},
                updatesHolder: undefined,
                expected: {
                    [fieldToConceptIdMapping.dataDestruction.anyRefusalOrWithdrawal]: fieldToConceptIdMapping.yes,
                    [fieldToConceptIdMapping.baselineBloodAndUrineIsRefused]: fieldToConceptIdMapping.yes,
                    [`${fieldToConceptIdMapping.dataDestruction.incentive}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.dataDestruction.incentiveEligible}`]: fieldToConceptIdMapping.yes,
                    [`${fieldToConceptIdMapping.dataDestruction.incentive}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.dataDestruction.norcIncentiveEligible}`]: fieldToConceptIdMapping.yes
                }
            },
            {
                label: 'menstrualCycleSurveyEligible only, first if case',
                participantInfo: {
                    // This path must be set or else it will cause an error trying to read the property
                    [fieldToConceptIdMapping.dataDestruction.incentive]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.dataDestruction.incentiveEligible]: fieldToConceptIdMapping.yes // incentiveEligible
                        }
                    },
                    [fieldToConceptIdMapping.dataDestruction.menstrualSurveyEligible]: fieldToConceptIdMapping.no,
                    [fieldToConceptIdMapping.dataDestruction.bloodUrineMouthwashCombinedResearchSurveyFlag]: fieldToConceptIdMapping.submitted,
                    [fieldToConceptIdMapping.activityParticipantRefusal]: {},
                    state: {
                        uid: uuid.v4()
                    }
                },
                specimens: [],
                surveys: {
                    ['D_299215535']: {
                        ['D_112151599']: fieldToConceptIdMapping.yes
                    }
                },
                updatesHolder: undefined,
                skipDateComparison: true,
                expected: {
                    [fieldToConceptIdMapping.dataDestruction.menstrualSurveyEligible]: fieldToConceptIdMapping.yes,
                    [fieldToConceptIdMapping.dataDestruction.anyRefusalOrWithdrawal]: fieldToConceptIdMapping.no,
                    [fieldToConceptIdMapping.baselineBloodAndUrineIsRefused]: fieldToConceptIdMapping.no
                  }
            },
            {
                label: 'menstrualCycleSurveyEligible only, second if case',
                participantInfo: {
                    // This path must be set or else it will cause an error trying to read the property
                    [fieldToConceptIdMapping.dataDestruction.incentive]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.dataDestruction.incentiveEligible]: fieldToConceptIdMapping.yes // incentiveEligible
                        }
                    },
                    [fieldToConceptIdMapping.dataDestruction.menstrualSurveyEligible]: fieldToConceptIdMapping.no,
                    [fieldToConceptIdMapping.dataDestruction.bloodUrineMouthwashCombinedResearchSurveyFlag]: fieldToConceptIdMapping.submitted,
                    [fieldToConceptIdMapping.activityParticipantRefusal]: {},
                    state: {
                        uid: uuid.v4()
                    }
                },
                specimens: [],
                surveys: {
                    ['D_299215535']: {
                        ['D_112151599']: fieldToConceptIdMapping.yes
                    }
                },
                updatesHolder: undefined,
                skipDateComparison: true,
                expected: {
                    [fieldToConceptIdMapping.dataDestruction.menstrualSurveyEligible]: fieldToConceptIdMapping.yes,
                    [fieldToConceptIdMapping.dataDestruction.anyRefusalOrWithdrawal]: fieldToConceptIdMapping.no,
                    [fieldToConceptIdMapping.baselineBloodAndUrineIsRefused]: fieldToConceptIdMapping.no
                  }
            },
            {
                label: 'allBaselineComplete only',
                participantInfo: {
                    // This path must be set or else it will cause an error trying to read the property
                    [fieldToConceptIdMapping.dataDestruction.incentive]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.dataDestruction.incentiveEligible]: fieldToConceptIdMapping.yes // incentiveEligible
                        }
                    },
                    [fieldToConceptIdMapping.dataDestruction.allBaselineSurveysCompleted]: fieldToConceptIdMapping.no,
                    [fieldToConceptIdMapping.dataDestruction.baselineSurveyStatusModuleBackgroundAndOverallHealthFlag]: fieldToConceptIdMapping.submitted,
                    [fieldToConceptIdMapping.dataDestruction.baselineSurveyStatusModuleMedications]: fieldToConceptIdMapping.submitted,
                    [fieldToConceptIdMapping.dataDestruction.baselineSurveyStatusModuleSmoking]: fieldToConceptIdMapping.submitted,
                    [fieldToConceptIdMapping.dataDestruction.baselineSurveyStatusModuleWhereYouLiveAndWorkFlag]: fieldToConceptIdMapping.submitted,
                    [fieldToConceptIdMapping.dataDestruction.bloodUrineMouthwashCombinedResearchSurveyFlag]: fieldToConceptIdMapping.submitted,
                    [fieldToConceptIdMapping.activityParticipantRefusal]: {},
                    state: {
                        uid: uuid.v4()
                    }
                },
                specimens: [],
                surveys: {},
                updatesHolder: undefined,
                skipDateComparison: true,
                expected: {
                    [fieldToConceptIdMapping.dataDestruction.allBaselineSurveysCompleted]: fieldToConceptIdMapping.yes,
                    [fieldToConceptIdMapping.dataDestruction.anyRefusalOrWithdrawal]: fieldToConceptIdMapping.no,
                    [fieldToConceptIdMapping.baselineBloodAndUrineIsRefused]: fieldToConceptIdMapping.no
                  }
            },
            {
                label: 'only some baseline complete',
                participantInfo: {
                    // This path must be set or else it will cause an error trying to read the property
                    [fieldToConceptIdMapping.dataDestruction.incentive]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.dataDestruction.incentiveEligible]: fieldToConceptIdMapping.yes // incentiveEligible
                        }
                    },
                    [fieldToConceptIdMapping.dataDestruction.allBaselineSurveysCompleted]: fieldToConceptIdMapping.no,
                    [fieldToConceptIdMapping.dataDestruction.baselineSurveyStatusModuleBackgroundAndOverallHealthFlag]: fieldToConceptIdMapping.submitted,
                    [fieldToConceptIdMapping.dataDestruction.baselineSurveyStatusModuleMedications]: fieldToConceptIdMapping.submitted,
                    [fieldToConceptIdMapping.dataDestruction.baselineSurveyStatusModuleSmoking]: fieldToConceptIdMapping.submitted,
                    [fieldToConceptIdMapping.dataDestruction.baselineSurveyStatusModuleWhereYouLiveAndWorkFlag]: fieldToConceptIdMapping.notStarted,
                    [fieldToConceptIdMapping.dataDestruction.bloodUrineMouthwashCombinedResearchSurveyFlag]: fieldToConceptIdMapping.notStarted,
                    [fieldToConceptIdMapping.activityParticipantRefusal]: {},
                    state: {
                        uid: uuid.v4()
                    }
                },
                specimens: [],
                surveys: {},
                updatesHolder: undefined,
                skipDateComparison: true,
                expected: {
                    [fieldToConceptIdMapping.dataDestruction.anyRefusalOrWithdrawal]: fieldToConceptIdMapping.no,
                    [fieldToConceptIdMapping.baselineBloodAndUrineIsRefused]: fieldToConceptIdMapping.no
                  }
            },
            {
                label: 'bloodUrineNotRefused - baseline blood and urine refused',
                participantInfo: {
                    // This path must be set or else it will cause an error trying to read the property
                    [fieldToConceptIdMapping.dataDestruction.incentive]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.dataDestruction.incentiveEligible]: fieldToConceptIdMapping.yes // incentiveEligible
                        }
                    },
                    [fieldToConceptIdMapping.baselineBloodAndUrineIsRefused]: fieldToConceptIdMapping.no,
                    [fieldToConceptIdMapping.activityParticipantRefusal]: {
                        [fieldToConceptIdMapping.baselineBloodSampleRefused]: fieldToConceptIdMapping.yes,
                        [fieldToConceptIdMapping.baselineUrineSampleRefused]: fieldToConceptIdMapping.yes
                    },
                    [fieldToConceptIdMapping.activityParticipantRefusal]: {},
                    state: {
                        uid: uuid.v4()
                    }
                },
                specimens: [],
                surveys: {},
                updatesHolder: undefined,
                skipDateComparison: true,
                expected: {
                    [fieldToConceptIdMapping.dataDestruction.anyRefusalOrWithdrawal]: fieldToConceptIdMapping.no
                }
            },
            {
                label: 'bloodUrineNotRefused - neither baseline blood nor urine refused',
                participantInfo: {
                    // This path must be set or else it will cause an error trying to read the property
                    [fieldToConceptIdMapping.dataDestruction.incentive]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.dataDestruction.incentiveEligible]: fieldToConceptIdMapping.yes // incentiveEligible
                        }
                    },
                    [fieldToConceptIdMapping.baselineBloodAndUrineIsRefused]: fieldToConceptIdMapping.no,
                    [fieldToConceptIdMapping.activityParticipantRefusal]: {
                        [fieldToConceptIdMapping.baselineBloodSampleRefused]: fieldToConceptIdMapping.no,
                        [fieldToConceptIdMapping.baselineUrineSampleRefused]: fieldToConceptIdMapping.no
                    },
                    [fieldToConceptIdMapping.activityParticipantRefusal]: {},
                    state: {
                        uid: uuid.v4()
                    }
                },
                specimens: [],
                surveys: {},
                updatesHolder: undefined,
                skipDateComparison: true,
                expected: {
                    [fieldToConceptIdMapping.dataDestruction.anyRefusalOrWithdrawal]: fieldToConceptIdMapping.no
                }
            },
            {
                label: 'bloodUrineNotRefused - baseline blood refused but not urine',
                participantInfo: {
                    // This path must be set or else it will cause an error trying to read the property
                    [fieldToConceptIdMapping.dataDestruction.incentive]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.dataDestruction.incentiveEligible]: fieldToConceptIdMapping.yes // incentiveEligible
                        }
                    },
                    [fieldToConceptIdMapping.baselineBloodAndUrineIsRefused]: fieldToConceptIdMapping.no,
                    [fieldToConceptIdMapping.activityParticipantRefusal]: {
                        [fieldToConceptIdMapping.baselineBloodSampleRefused]: fieldToConceptIdMapping.yes,
                        [fieldToConceptIdMapping.baselineUrineSampleRefused]: fieldToConceptIdMapping.no
                    },
                    [fieldToConceptIdMapping.activityParticipantRefusal]: {},
                    state: {
                        uid: uuid.v4()
                    }
                },
                specimens: [],
                surveys: {},
                updatesHolder: undefined,
                skipDateComparison: true,
                expected: {
                    [fieldToConceptIdMapping.dataDestruction.anyRefusalOrWithdrawal]: fieldToConceptIdMapping.no
                }
            },
            {
                label: 'bloodUrineNotRefused - baseline urine refused but not blood',
                participantInfo: {
                    // This path must be set or else it will cause an error trying to read the property
                    [fieldToConceptIdMapping.dataDestruction.incentive]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.dataDestruction.incentiveEligible]: fieldToConceptIdMapping.yes // incentiveEligible
                        }
                    },
                    [fieldToConceptIdMapping.baselineBloodAndUrineIsRefused]: fieldToConceptIdMapping.no,
                    [fieldToConceptIdMapping.activityParticipantRefusal]: {
                        [fieldToConceptIdMapping.baselineBloodSampleRefused]: fieldToConceptIdMapping.no,
                        [fieldToConceptIdMapping.baselineUrineSampleRefused]: fieldToConceptIdMapping.yes
                    },
                    [fieldToConceptIdMapping.activityParticipantRefusal]: {},
                    state: {
                        uid: uuid.v4()
                    }
                },
                specimens: [],
                surveys: {},
                updatesHolder: undefined,
                skipDateComparison: true,
                expected: {
                    [fieldToConceptIdMapping.dataDestruction.anyRefusalOrWithdrawal]: fieldToConceptIdMapping.no
                }
            },
            {
                label: 'bloodUrineNotRefused - baselineBloodAndUrineIsRefused already marked as yes, both baseline blood and urine refused',
                participantInfo: {
                    // This path must be set or else it will cause an error trying to read the property
                    [fieldToConceptIdMapping.dataDestruction.incentive]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.dataDestruction.incentiveEligible]: fieldToConceptIdMapping.yes // incentiveEligible
                        }
                    },
                    [fieldToConceptIdMapping.baselineBloodAndUrineIsRefused]: fieldToConceptIdMapping.yes,
                    [fieldToConceptIdMapping.activityParticipantRefusal]: {
                        [fieldToConceptIdMapping.baselineBloodSampleRefused]: fieldToConceptIdMapping.yes,
                        [fieldToConceptIdMapping.baselineUrineSampleRefused]: fieldToConceptIdMapping.yes
                    },
                    [fieldToConceptIdMapping.activityParticipantRefusal]: {},
                    state: {
                        uid: uuid.v4()
                    }
                },
                specimens: [],
                surveys: {},
                updatesHolder: undefined,
                skipDateComparison: true,
                expected: {
                    [fieldToConceptIdMapping.dataDestruction.anyRefusalOrWithdrawal]: fieldToConceptIdMapping.no
                }
            },
            {
                label: 'bloodUrineNotRefused - baselineBloodAndUrineIsRefused already marked as yes, neither baseline blood nor urine refused',
                participantInfo: {
                    // This path must be set or else it will cause an error trying to read the property
                    [fieldToConceptIdMapping.dataDestruction.incentive]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.dataDestruction.incentiveEligible]: fieldToConceptIdMapping.yes // incentiveEligible
                        }
                    },
                    [fieldToConceptIdMapping.baselineBloodAndUrineIsRefused]: fieldToConceptIdMapping.yes,
                    [fieldToConceptIdMapping.activityParticipantRefusal]: {
                        [fieldToConceptIdMapping.baselineBloodSampleRefused]: fieldToConceptIdMapping.no,
                        [fieldToConceptIdMapping.baselineUrineSampleRefused]: fieldToConceptIdMapping.no
                    },
                    [fieldToConceptIdMapping.activityParticipantRefusal]: {},
                    state: {
                        uid: uuid.v4()
                    }
                },
                specimens: [],
                surveys: {},
                updatesHolder: undefined,
                skipDateComparison: true,
                expected: {
                    [fieldToConceptIdMapping.dataDestruction.anyRefusalOrWithdrawal]: fieldToConceptIdMapping.no
                }
            },
            {
                label: 'calculateBaselineOrderPlaced, blood order placed',
                participantInfo: {
                    // This path must be set or else it will cause an error trying to read the property
                    [fieldToConceptIdMapping.dataDestruction.incentive]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.dataDestruction.incentiveEligible]: fieldToConceptIdMapping.yes // incentiveEligible
                        }
                    },
                    [fieldToConceptIdMapping.collectionDetails]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.baselineBloodOrUrineOrderPlaced]: fieldToConceptIdMapping.no,
                            [fieldToConceptIdMapping.bloodOrderPlaced]: fieldToConceptIdMapping.yes
                        }
                    },
                    [fieldToConceptIdMapping.activityParticipantRefusal]: {},
                    state: {
                        uid: uuid.v4()
                    }
                },
                specimens: [],
                surveys: {},
                updatesHolder: undefined,
                skipDateComparison: true,
                expected: {
                    [fieldToConceptIdMapping.dataDestruction.anyRefusalOrWithdrawal]: fieldToConceptIdMapping.no,
                    [fieldToConceptIdMapping.baselineBloodAndUrineIsRefused]: fieldToConceptIdMapping.no,
                    [`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.baselineBloodOrUrineOrderPlaced}`]: fieldToConceptIdMapping.yes,
                    [`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bloodOrUrineCollected}`]: fieldToConceptIdMapping.no
                  }
            },
            {
                label: 'calculateBaselineOrderPlaced, scenario 1',
                participantInfo: {
                    // This path must be set or else it will cause an error trying to read the property
                    [fieldToConceptIdMapping.dataDestruction.incentive]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.dataDestruction.incentiveEligible]: fieldToConceptIdMapping.yes // incentiveEligible
                        }
                    },
                    [fieldToConceptIdMapping.collectionDetails]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.bloodOrderPlaced]: fieldToConceptIdMapping.no,
                            [fieldToConceptIdMapping.urineOrderPlaced]: fieldToConceptIdMapping.no
                        }
                    },
                    [fieldToConceptIdMapping.activityParticipantRefusal]: {},
                    state: {
                        uid: uuid.v4()
                    }
                },
                specimens: [],
                surveys: {},
                updatesHolder: undefined,
                skipDateComparison: true,
                expected: {
                    [fieldToConceptIdMapping.dataDestruction.anyRefusalOrWithdrawal]: fieldToConceptIdMapping.no,
                    [fieldToConceptIdMapping.baselineBloodAndUrineIsRefused]: fieldToConceptIdMapping.no,
                    [`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.baselineBloodOrUrineOrderPlaced}`]: fieldToConceptIdMapping.no,
                    [`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bloodOrUrineCollected}`]: fieldToConceptIdMapping.no
                  }
            },
            {
                label: 'calculateBaselineOrderPlaced, scenario 2',
                participantInfo: {
                    // This path must be set or else it will cause an error trying to read the property
                    [fieldToConceptIdMapping.dataDestruction.incentive]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.dataDestruction.incentiveEligible]: fieldToConceptIdMapping.yes // incentiveEligible
                        }
                    },
                    [fieldToConceptIdMapping.collectionDetails]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.bloodOrderPlaced]: fieldToConceptIdMapping.no
                        }
                    },
                    [fieldToConceptIdMapping.activityParticipantRefusal]: {},
                    state: {
                        uid: uuid.v4()
                    }
                },
                specimens: [],
                surveys: {},
                updatesHolder: undefined,
                skipDateComparison: true,
                expected: {
                    [fieldToConceptIdMapping.dataDestruction.anyRefusalOrWithdrawal]: fieldToConceptIdMapping.no,
                    [fieldToConceptIdMapping.baselineBloodAndUrineIsRefused]: fieldToConceptIdMapping.no,
                    [`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.baselineBloodOrUrineOrderPlaced}`]: fieldToConceptIdMapping.no,
                    [`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bloodOrUrineCollected}`]: fieldToConceptIdMapping.no
                  }
            },
            {
                label: 'calculateBaselineOrderPlaced, scenario 3',
                participantInfo: {
                    // This path must be set or else it will cause an error trying to read the property
                    [fieldToConceptIdMapping.dataDestruction.incentive]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.dataDestruction.incentiveEligible]: fieldToConceptIdMapping.yes // incentiveEligible
                        }
                    },
                    [fieldToConceptIdMapping.collectionDetails]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.urineOrderPlaced]: fieldToConceptIdMapping.no
                        }
                    },
                    [fieldToConceptIdMapping.activityParticipantRefusal]: {},
                    state: {
                        uid: uuid.v4()
                    }
                },
                specimens: [],
                surveys: {},
                updatesHolder: undefined,
                skipDateComparison: true,
                expected: {
                    [fieldToConceptIdMapping.dataDestruction.anyRefusalOrWithdrawal]: fieldToConceptIdMapping.no,
                    [fieldToConceptIdMapping.baselineBloodAndUrineIsRefused]: fieldToConceptIdMapping.no,
                    [`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.baselineBloodOrUrineOrderPlaced}`]: fieldToConceptIdMapping.no,
                    [`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bloodOrUrineCollected}`]: fieldToConceptIdMapping.no
                  }
            },
            /*
            {
                label: '',
                participantInfo: {},
                specimens: [],
                surveys: {},
                updatesHolder: undefined,
                skipDateComparison: false,
                expected: {}
            },
            */
        ];
        beforeEach(async () => {
            sinon.replace(firestore, 'getParticipantData', () => {
                return {data: testInfo[i].participantInfo, id: testInfo[i].participantInfo.state.uid};
            });
            sinon.replace(firestore, 'getSpecimenCollections', () => {
                return testInfo[i].specimens;
            });
            sinon.replace(firestore, 'retrieveUserSurveys', () => {
                return testInfo[i].surveys;
            })
            sinon.replace(firestore, 'updateParticipantData', (doc, updates) => testInfo[i].updatesHolder = updates);
        });

        for(let j = 0; j < testInfo.length; j++) {
            let thisTest = testInfo[j];
            it(j + ': ' + thisTest.label, async () => {
                try {
                    await validation.checkDerivedVariables('fake', 'fake');
                } catch(err) {
                    console.error('Error', err);
                }

                assert.isDefined(thisTest.updatesHolder);
                
                const clonedUpdatesHolder = Object.assign({}, thisTest.updatesHolder);
                // Comparing without the timestamp, which will never match exactly and is checked for closeness elsewhere.
                delete clonedUpdatesHolder[`${fieldToConceptIdMapping.dataDestruction.incentive}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.dataDestruction.dateIncentiveEligible}`];
                // console.log('updatesHolder for %s', j, thisTest.updatesHolder);
                assert.deepEqual(thisTest.expected, clonedUpdatesHolder);
                if(!thisTest.skipDateComparison) {
                    assert.closeTo(+new Date(thisTest.updatesHolder[`${fieldToConceptIdMapping.dataDestruction.incentive}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.dataDestruction.dateIncentiveEligible}`]), +new Date(), 60000, 'Date incentive eligible is within a minute of test completion');
                }
            });
        }

        afterEach(async() => {
            sinon.restore();
            i++;
        });
    });

    describe('resetParticipantHelper', async () => {
        it('resetParticipantHelper test for live user', async () => {
            
            // Not saving to DB

            // Currently using an existing participant ID but not saving changes to DB
        

            const uid = 'Iw37pmEJUWWjRhTaH7A32DH384n1';
            const db = admin.firestore();
            const snapshot = await db.collection('participants').where('state.uid', '==', uid).get();
            assert.isAbove(snapshot.size, 0, 'Participant found');
            const prevUserData = snapshot.docs[0].data();

            let keysToPreserve = [
                fieldToConceptIdMapping.iDoNotHaveAPIN.toString(),
                fieldToConceptIdMapping.healthCareProvider.toString(),
                fieldToConceptIdMapping.heardAboutStudyFrom.toString(),
                fieldToConceptIdMapping.dataDestruction.consentFirstName.toString(),
                fieldToConceptIdMapping.dataDestruction.consentMiddleName.toString(),
                fieldToConceptIdMapping.dataDestruction.consentLastName.toString(),
                fieldToConceptIdMapping.dataDestruction.consentSuffixName.toString(),
                fieldToConceptIdMapping.dataDestruction.userProfileNameFirstName.toString(),
                fieldToConceptIdMapping.dataDestruction.userProfileNameMiddleName.toString(),
                fieldToConceptIdMapping.dataDestruction.userProfileNameLastName.toString(),
                fieldToConceptIdMapping.dataDestruction.userProfileNameSuffixName.toString(),
                'query',
                fieldToConceptIdMapping.autogeneratedConsentDate.toString(),
                fieldToConceptIdMapping.participantMap.consentFormSubmitted.toString(),
                fieldToConceptIdMapping.dataDestruction.informedConsentDateSigned.toString(),
                fieldToConceptIdMapping.dataDestruction.informedConsentVersion.toString(),
                fieldToConceptIdMapping.dataDestruction.hipaaAuthorizationDateSigned.toString(),
                fieldToConceptIdMapping.dataDestruction.hipaaAuthorizationFlag.toString(),
                fieldToConceptIdMapping.dataDestruction.hipaaAuthorizationVersion.toString(),
                fieldToConceptIdMapping.dataDestruction.firebaseAuthenticationEmail.toString(),
                fieldToConceptIdMapping.firebaseAuthenticationFirstAndLastName.toString(),
                fieldToConceptIdMapping.authenticationPhone.toString(),
                fieldToConceptIdMapping.signInMechanism.toString(),
                fieldToConceptIdMapping.preferredLanguage.toString(),
                fieldToConceptIdMapping.preferredName.toString(),
                fieldToConceptIdMapping.dataDestruction.birthMonth.toString(),
                fieldToConceptIdMapping.dataDestruction.birthDay.toString(),
                fieldToConceptIdMapping.dataDestruction.birthYear.toString(),
                fieldToConceptIdMapping.dataDestruction.dateOfBirth.toString(),
                fieldToConceptIdMapping.cellPhone.toString(),
                fieldToConceptIdMapping.homePhone.toString(),
                fieldToConceptIdMapping.otherPhone.toString(),
                fieldToConceptIdMapping.prefEmail.toString(),
                fieldToConceptIdMapping.additionalEmail1.toString(),
                fieldToConceptIdMapping.additionalEmail2.toString(),
                fieldToConceptIdMapping.additionalEmail3.toString(),
                fieldToConceptIdMapping.address1.toString(),
                fieldToConceptIdMapping.address2.toString(),
                fieldToConceptIdMapping.city.toString(),
                fieldToConceptIdMapping.state.toString(),
                fieldToConceptIdMapping.zip.toString(),
                fieldToConceptIdMapping.isPOBox.toString(),
                fieldToConceptIdMapping.physicalAddress1.toString(),
                fieldToConceptIdMapping.physicalAddress2.toString(),
                fieldToConceptIdMapping.physicalCity.toString(),
                fieldToConceptIdMapping.physicalState.toString(),
                fieldToConceptIdMapping.physicalZip.toString(),
                fieldToConceptIdMapping.canWeVoicemailMobile.toString(),
                fieldToConceptIdMapping.canWeVoicemailHome.toString(),
                fieldToConceptIdMapping.canWeVoicemailOther.toString(),
                fieldToConceptIdMapping.canWeText.toString(),
                fieldToConceptIdMapping.prefContactMethod.toString(),
                fieldToConceptIdMapping.haveYouEverBeenDiagnosedWithCancer.toString(),
                fieldToConceptIdMapping.whatYearWereYouDiagnosed.toString(),
                fieldToConceptIdMapping.whatTypeOfCancer.toString(),
                fieldToConceptIdMapping.anyCommentsAboutYourCancerDiagnosis.toString(),
                fieldToConceptIdMapping.derivedAge.toString(),
                fieldToConceptIdMapping.dataDestruction.userProfileSubmittedFlag.toString(),
                fieldToConceptIdMapping.autogeneratedProfileSubmittedTime.toString(),
                fieldToConceptIdMapping.participantMap.consentFormSubmitted.toString(),
                fieldToConceptIdMapping.verificationStatus.toString(),
                fieldToConceptIdMapping.autogeneratedSignedInTime.toString(),
                fieldToConceptIdMapping.autogeneratedVerificationStatusUpdatedTime.toString(),
                // These are deprecated but left in to ensure data consistency
                '983784715', 
                '700668490',
                '430184574',
                '507120821',
                '383945929'
            ];

            let results;

            try {
                results = await firestore.resetParticipantHelper(uid, false);
            } catch(err) {
                console.error('Error', err);
            }

            const {data, deleted} = results;

            assert.equal(
                data[fieldToConceptIdMapping.verificationStatus], fieldToConceptIdMapping.verified,
                'Status is verified'
            );
            assert.equal(
                prevUserData[fieldToConceptIdMapping.autogeneratedVerificationStatusUpdatedTime],
                data[fieldToConceptIdMapping.autogeneratedVerificationStatusUpdatedTime],
                'Original verification date is retained if present'
            );

            // All surveys deleted, and survey flags and dates reset not started and null respectively
            // All notifications deleted
            // All refusal and withdrawal and data destruction reversed and reset to null/default status
            // All biospecimens deleted from the Biospecimens table
            // All Kit assembly data deleted from the Kit Assembly table
            // All biospecimen data deleted from the Participants table and default variables related to biospecimens reset to default/null settings

            // All incentive data reset to default/null settings
            const {incentiveFlags, withdrawalConcepts, defaultFlags, moduleConceptsToCollections} = require('../utils/shared.js');
            Object.keys(incentiveFlags).forEach(flag => {
                if(keysToPreserve.indexOf(flag) > -1) {
                    return;
                }
                Object.keys(incentiveFlags[flag]).forEach(subflag => {
                    assert.equal(
                        data[flag][subflag],
                        incentiveFlags[flag][subflag],
                        `${flag}.${subflag} matches`
                    );
                })
            });


            // They should still be consented and user profile completed, consent form and HIPAA form signed
            Object.keys(withdrawalConcepts).forEach(key => {
                if(keysToPreserve.indexOf(key.toString()) > -1) {
                    return;
                }
                if(typeof withdrawalConcepts[key] === 'object') {
                    Object.keys(withdrawalConcepts[key]).forEach(subkey => {
                        assert.equal(
                            data[key][subkey],
                            withdrawalConcepts[key][subkey],
                            `${key}.${subkey} matches`
                        );
                    });
                } else {
                    assert.equal(
                        data[key],
                        withdrawalConcepts[key],
                        `${key} matches`
                    );
                }
            });

            Object.keys(defaultFlags).forEach(key => {
                
                // Ignore any keys already checked
                if(incentiveFlags[key] || withdrawalConcepts[key] || keysToPreserve.indexOf(key.toString()) > -1) {
                    return;
                }

                assert.equal(
                    data[key],
                    defaultFlags[key],
                    `${key} matches`
                );
            });

            // Assert that keys marked as keysToPreserve match the live object
            // and are not overwritten
            keysToPreserve.forEach(key => {
                if(typeof data[key] === 'object') {
                    assert.deepEqual(prevUserData[key], data[key], `${key} matches`);
                } else {
                    assert.equal(prevUserData[key], data[key], `${key} matches`);
                }
            });

            // Because this test does not have control over this data and it could change,
            //  right now we are just ensuring that values are found for each of these categories
            Object.keys(moduleConceptsToCollections).forEach(concept => {
                assert.exists(deleted[moduleConceptsToCollections[concept]], `${concept} exists in deleted`);
            });

            assert.exists(deleted.notifications);
            assert.exists(deleted.biospecimen);
            assert.exists(deleted.cancerOccurrence);
            assert.exists(deleted.kitAssembly);

        });
        
    });
});

describe('sendScheduledNotificationsGen2', async () => {

});

describe('importToBigQuery', async () => {

});

describe('scheduleFirestoreDataExport', async () => {

});

describe('exportNotificationsToBucket', async () => {

});

describe('importNotificationsToBigquery', async () => {

});

describe('participantDataCleanup', async () => {

});

describe('webhook', async () => {
    it('Should only accept POST', async () => {
        const req = httpMocks.createRequest({
            method: 'GET',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.updateParticipantData(req, res)
        assert.equal(res.statusCode, 405);
        const data = res._getJSONData();
        assert.equal(data.message, 'Only POST requests are accepted!');
        assert.equal(data.code, 405);
    });
});

describe('heartbeat', async () => {
    it('Should return 200 for options', async () => {
        const req = httpMocks.createRequest({
            method: 'OPTIONS',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.getParticipantNotification(req, res)
        assert.equal(res.statusCode, 200);
        const data = res._getJSONData();
        assert.equal(data.code, 200);
    });
    it('Should only accept GET', async () => {
        const req = httpMocks.createRequest({
            method: 'POST',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.getParticipantNotification(req, res)
        assert.equal(res.statusCode, 405);
        const data = res._getJSONData();
        assert.equal(data.message, 'Only GET requests are accepted!');
        assert.equal(data.code, 405);
    });

    // Currently not set up for BigQuery access needed for this
    it.skip('Should allow get', async() => {
        const req = httpMocks.createRequest({
            method: 'GET',
            headers: {
                'x-forwarded-for': 'dummy'
            },
            connection: {}
        });
    
        const res = httpMocks.createResponse();
        await functions.heartbeat(req, res);
    });
});