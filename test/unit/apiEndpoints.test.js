const httpMocks = require('node-mocks-http');
const { setupTestSuite } = require('../shared/testHelpers');

let firestore;
let api;

beforeAll(() => {
    setupTestSuite({
        setupConsole: false,
        setupModuleMocks: true,
    });

    const { incentiveCompleted, eligibleForIncentive } = require('../../utils/incentive');
    const { getToken, validateUsersEmailPhone } = require('../../utils/validation');
    const { getFilteredParticipants, getParticipants, identifyParticipant } = require('../../utils/submission');
    const { submitParticipantsData, updateParticipantData, getBigQueryData } = require('../../utils/sites');
    const { getParticipantNotification } = require('../../utils/notifications');
    const { dashboard } = require('../../utils/dashboard');
    const { connectApp } = require('../../utils/connectApp');
    const { biospecimenAPIs } = require('../../utils/biospecimen');
    const { webhook } = require('../../utils/webhook');
    const { heartbeat } = require('../../utils/heartbeat');
    const { physicalActivity } = require('../../utils/reports');

    api = {
        incentiveCompleted,
        participantsEligibleForIncentive: eligibleForIncentive,
        getParticipantToken: getToken,
        validateUsersEmailPhone,
        getFilteredParticipants,
        getParticipants,
        identifyParticipant,
        submitParticipantsData,
        updateParticipantData,
        getBigQueryData,
        getParticipantNotification,
        dashboard,
        app: connectApp,
        biospecimen: biospecimenAPIs,
        webhook,
        heartbeat,
        physicalActivity,
    };

    firestore = require('../../utils/firestore');
});

const createRequest = (method, overrides = {}) => {
    const { headers = {}, ...rest } = overrides;
    return httpMocks.createRequest({
        method,
        headers: {
            'x-forwarded-for': 'dummy',
            ...headers,
        },
        connection: {},
        ...rest,
    });
};

const invoke = async (handler, method, overrides = {}) => {
    const req = createRequest(method, overrides);
    const res = httpMocks.createResponse();
    await handler(req, res);
    return res;
};

describe('API Endpoint Method Guards', () => {
    describe('OPTIONS handling', () => {
        const optionsCases = [
            ['incentiveCompleted', () => api.incentiveCompleted],
            ['participantsEligibleForIncentive', () => api.participantsEligibleForIncentive],
            ['getParticipantToken', () => api.getParticipantToken],
            ['getFilteredParticipants', () => api.getFilteredParticipants],
            ['getParticipants', () => api.getParticipants],
            ['identifyParticipant', () => api.identifyParticipant],
            ['submitParticipantsData', () => api.submitParticipantsData],
            ['updateParticipantData', () => api.updateParticipantData],
            ['getBigQueryData', () => api.getBigQueryData],
            ['getParticipantNotification', () => api.getParticipantNotification],
            ['dashboard', () => api.dashboard],
            ['app', () => api.app],
            ['biospecimen', () => api.biospecimen],
            ['heartbeat', () => api.heartbeat],
            ['physicalActivity', () => api.physicalActivity],
        ];

        for (const [name, getHandler] of optionsCases) {
            it(`should return 200 for ${name} OPTIONS requests`, async () => {
                const res = await invoke(getHandler(), 'OPTIONS');
                expect(res.statusCode).toBe(200);
                expect(res._getJSONData().code).toBe(200);
            });
        }
    });

    describe('HTTP method restrictions', () => {
        const methodCases = [
            ['incentiveCompleted', () => api.incentiveCompleted, 'GET', 'Only POST requests are accepted!'],
            ['participantsEligibleForIncentive', () => api.participantsEligibleForIncentive, 'POST', 'Only GET requests are accepted!'],
            ['getParticipantToken', () => api.getParticipantToken, 'GET', 'Only POST requests are accepted!'],
            ['validateUsersEmailPhone', () => api.validateUsersEmailPhone, 'POST', 'Only GET requests are accepted!'],
            ['getFilteredParticipants', () => api.getFilteredParticipants, 'POST', 'Only GET requests are accepted!'],
            ['getParticipants', () => api.getParticipants, 'POST', 'Only GET requests are accepted!'],
            ['identifyParticipant', () => api.identifyParticipant, 'PUT', 'Only GET or POST requests are accepted!'],
            ['submitParticipantsData', () => api.submitParticipantsData, 'GET', 'Only POST requests are accepted!'],
            ['updateParticipantData', () => api.updateParticipantData, 'GET', 'Only POST requests are accepted!'],
            ['getBigQueryData', () => api.getBigQueryData, 'POST', 'Only GET requests are accepted!'],
            ['getParticipantNotification', () => api.getParticipantNotification, 'POST', 'Only GET requests are accepted!'],
            ['webhook', () => api.webhook, 'GET', 'Only POST requests are accepted!'],
        ];

        for (const [name, getHandler, method, expectedMessage] of methodCases) {
            it(`should enforce allowed methods for ${name}`, async () => {
                const res = await invoke(getHandler(), method);
                expect(res.statusCode).toBe(405);
                const data = res._getJSONData();
                expect(data.code).toBe(405);
                expect(data.message).toBe(expectedMessage);
            });
        }

        it('should enforce GET-only for heartbeat with expected payload shape', async () => {
            const res = await invoke(api.heartbeat, 'POST');
            expect(res.statusCode).toBe(405);
            const data = res._getJSONData();
            expect(data.code).toBe(405);
            expect(data.data).toBe('Only GET requests are accepted!');
        });

        it('should enforce GET-only for physicalActivity with expected payload shape', async () => {
            const res = await invoke(api.physicalActivity, 'POST');
            expect(res.statusCode).toBe(405);
            const data = res._getJSONData();
            expect(data.code).toBe(405);
            expect(data.data).toBe('Only GET requests are accepted!');
        });
    });

    describe('Authorization guards', () => {
        const unauthorizedCases = [
            ['dashboard', () => api.dashboard],
            ['app', () => api.app],
            ['biospecimen', () => api.biospecimen],
            ['getBigQueryData', () => api.getBigQueryData],
        ];

        for (const [name, getHandler] of unauthorizedCases) {
            it(`should reject unauthorized ${name} requests`, async () => {
                const res = await invoke(getHandler(), 'GET');
                expect(res.statusCode).toBe(401);
            });
        }
    });

    describe('validateUsersEmailPhone account lookup', () => {
        it('should return accountExists true/false based on lookup result', async () => {
            const verifyStub = vi.spyOn(firestore, 'verifyUsersEmailOrPhone');
            verifyStub.mockResolvedValueOnce(false);
            verifyStub.mockResolvedValueOnce(true);

            const missingUserRes = await invoke(api.validateUsersEmailPhone, 'GET', {
                query: {
                    email: 'nonexistent@example.com',
                },
            });
            expect(missingUserRes.statusCode).toBe(200);
            expect(missingUserRes._getJSONData().code).toBe(200);
            expect(missingUserRes._getJSONData().data.accountExists).toBe(false);

            const existingUserRes = await invoke(api.validateUsersEmailPhone, 'GET', {
                query: {
                    email: 'existing@example.com',
                },
            });
            expect(existingUserRes.statusCode).toBe(200);
            expect(existingUserRes._getJSONData().code).toBe(200);
            expect(existingUserRes._getJSONData().data.accountExists).toBe(true);
        });
    });

    describe('heartbeat success path', () => {
        it('should return 200 payload for heartbeat GET requests', async () => {
            const { BigQuery } = require('@google-cloud/bigquery');
            const querySpy = vi.spyOn(BigQuery.prototype, 'query').mockResolvedValue([
                [
                    {
                        num_active_participants: 21,
                        num_male_participants: 9,
                        num_female_participants: 12,
                    },
                ],
            ]);

            const res = await invoke(api.heartbeat, 'GET');
            const data = res._getJSONData();

            expect(querySpy).toHaveBeenCalledTimes(1);
            expect(res.statusCode).toBe(200);
            expect(data.code).toBe(200);
            expect(data.data.activeParticipants).toBe(21);
            expect(data.data.maleParticipants).toBe(9);
            expect(data.data.femaleParticipants).toBe(12);
            expect(data.data.utc).toMatch(/^\d{2}:\d{2}:\d{2}$/);
        });
    });

    describe('getBigQueryData success path', () => {
        it('should return 200 and query results for authorized requests', async () => {
            const shared = require('../../utils/shared');
            const bigqueryUtils = require('../../utils/bigquery');

            vi.spyOn(shared, 'APIAuthorization').mockResolvedValue({
                siteCode: 12345,
                user: 'tester',
            });
            vi.spyOn(bigqueryUtils, 'validateTableAccess').mockResolvedValue(true);
            vi.spyOn(bigqueryUtils, 'getBigQueryData').mockResolvedValue([
                { Connect_ID: 1001, status: 'ok' },
                { Connect_ID: 1002, status: 'ok' },
            ]);

            const res = await invoke(api.getBigQueryData, 'GET', {
                query: {
                    dataset: 'ROI',
                    table: 'physical_activity',
                },
            });

            expect(res.statusCode).toBe(200);
            const data = res._getJSONData();
            expect(data.code).toBe(200);
            expect(data.results).toHaveLength(2);
            expect(data.results[0].Connect_ID).toBe(1001);
        });
    });

    describe('physicalActivity success path', () => {
        it('should return 200 and call processPhysicalActivity with default date expression', async () => {
            const processPhysicalActivitySpy = vi.spyOn(firestore, 'processPhysicalActivity').mockResolvedValue(true);

            const res = await invoke(api.physicalActivity, 'GET');
            const data = res._getJSONData();

            expect(processPhysicalActivitySpy).toHaveBeenCalledWith('CURRENT_DATE()');
            expect(res.statusCode).toBe(200);
            expect(data.code).toBe(200);
            expect(data.message).toBe('Physical Activity data processed successfully!');
        });

        it('should pass explicit date expression when year/month/day query is provided', async () => {
            const processPhysicalActivitySpy = vi.spyOn(firestore, 'processPhysicalActivity').mockResolvedValue(true);

            const res = await invoke(api.physicalActivity, 'GET', {
                query: {
                    year: '2025',
                    month: '12',
                    day: '31',
                },
            });

            expect(processPhysicalActivitySpy).toHaveBeenCalledWith("'2025-12-31'");
            expect(res.statusCode).toBe(200);
        });
    });

    describe('webhook sendgrid signature verification', () => {
        it('should return 403 for invalid sendgrid webhook signatures', async () => {
            const { SecretManagerServiceClient } = require('@google-cloud/secret-manager');
            const { EventWebhook, EventWebhookHeader } = require('@sendgrid/eventwebhook');

            vi.spyOn(SecretManagerServiceClient.prototype, 'accessSecretVersion').mockResolvedValue([
                {
                    payload: {
                        data: Buffer.from('public-key'),
                    },
                },
            ]);
            vi.spyOn(EventWebhook.prototype, 'convertPublicKeyToECDSA').mockReturnValue('ecdsa-key');
            vi.spyOn(EventWebhook.prototype, 'verifySignature').mockReturnValue(false);

            const events = [];
            const res = await invoke(api.webhook, 'POST', {
                query: {
                    api: 'sendgrid-email-status',
                },
                body: events,
                rawBody: Buffer.from(JSON.stringify(events)),
                headers: {
                    [EventWebhookHeader.SIGNATURE()]: 'bad-signature',
                    [EventWebhookHeader.TIMESTAMP()]: '1700000000',
                },
            });

            expect(res.statusCode).toBe(403);
            expect(res._getData()).toBe('Forbidden');
        });

        it('should return 200 for valid sendgrid webhook signatures', async () => {
            const { SecretManagerServiceClient } = require('@google-cloud/secret-manager');
            const { EventWebhook, EventWebhookHeader } = require('@sendgrid/eventwebhook');

            vi.spyOn(SecretManagerServiceClient.prototype, 'accessSecretVersion').mockResolvedValue([
                {
                    payload: {
                        data: Buffer.from('public-key'),
                    },
                },
            ]);
            vi.spyOn(EventWebhook.prototype, 'convertPublicKeyToECDSA').mockReturnValue('ecdsa-key');
            vi.spyOn(EventWebhook.prototype, 'verifySignature').mockReturnValue(true);

            const events = [];
            const res = await invoke(api.webhook, 'POST', {
                query: {
                    api: 'sendgrid-email-status',
                },
                body: events,
                rawBody: Buffer.from(JSON.stringify(events)),
                headers: {
                    [EventWebhookHeader.SIGNATURE()]: 'valid-signature',
                    [EventWebhookHeader.TIMESTAMP()]: '1700000001',
                },
            });

            expect(res.statusCode).toBe(200);
            expect(res._getJSONData().code).toBe(200);
        });
    });
});

describe('Index onRequest Wrapper Handlers', () => {
    const indexPath = require.resolve('../../index.js');
    const httpsPath = require.resolve('firebase-functions/v2/https');
    const notificationsPath = require.resolve('../../utils/notifications.js');
    const dhqPath = require.resolve('../../utils/dhq.js');

    const loadIndexWithOnRequestMocks = () => {
        const originalIndex = require.cache[indexPath];
        const originalHttps = require.cache[httpsPath];
        const originalNotifications = require.cache[notificationsPath];
        const originalDhq = require.cache[dhqPath];

        const onRequestSpy = vi.fn((handler) => handler);

        const notificationStubs = {
            getParticipantNotification: vi.fn(),
            sendScheduledNotifications: vi.fn(async (req, res) => {
                return res.status(210).json({
                    code: 210,
                    handler: 'sendScheduledNotifications',
                    method: req.method,
                });
            }),
        };

        const dhqStubs = {
            generateDHQReports: vi.fn(async (req, res) => {
                return res.status(211).json({
                    code: 211,
                    handler: 'generateDHQReports',
                    method: req.method,
                });
            }),
            processDHQReports: vi.fn(async (req, res) => {
                return res.status(212).json({
                    code: 212,
                    handler: 'processDHQReports',
                    method: req.method,
                });
            }),
            scheduledSyncDHQ3Status: vi.fn(async (req, res) => {
                return res.status(213).json({
                    code: 213,
                    handler: 'scheduledSyncDHQ3Status',
                    method: req.method,
                });
            }),
            scheduledCountDHQ3Credentials: vi.fn(async (req, res) => {
                return res.status(214).json({
                    code: 214,
                    handler: 'scheduledCountDHQ3Credentials',
                    method: req.method,
                });
            }),
        };

        require.cache[httpsPath] = {
            id: httpsPath,
            filename: httpsPath,
            loaded: true,
            exports: {
                onRequest: onRequestSpy,
            },
        };

        require.cache[notificationsPath] = {
            id: notificationsPath,
            filename: notificationsPath,
            loaded: true,
            exports: notificationStubs,
        };

        require.cache[dhqPath] = {
            id: dhqPath,
            filename: dhqPath,
            loaded: true,
            exports: dhqStubs,
        };

        delete require.cache[indexPath];
        const indexExports = require('../../index.js');

        const restore = () => {
            if (originalIndex) {
                require.cache[indexPath] = originalIndex;
            } else {
                delete require.cache[indexPath];
            }

            if (originalHttps) {
                require.cache[httpsPath] = originalHttps;
            } else {
                delete require.cache[httpsPath];
            }

            if (originalNotifications) {
                require.cache[notificationsPath] = originalNotifications;
            } else {
                delete require.cache[notificationsPath];
            }

            if (originalDhq) {
                require.cache[dhqPath] = originalDhq;
            } else {
                delete require.cache[dhqPath];
            }
        };

        return {
            indexExports,
            onRequestSpy,
            notificationStubs,
            dhqStubs,
            restore,
        };
    };

    it('should register scheduled handlers via onRequest in index.js', () => {
        const { onRequestSpy, notificationStubs, dhqStubs, restore } = loadIndexWithOnRequestMocks();

        try {
            expect(onRequestSpy).toHaveBeenCalledTimes(5);
            expect(onRequestSpy).toHaveBeenCalledWith(notificationStubs.sendScheduledNotifications);
            expect(onRequestSpy).toHaveBeenCalledWith(dhqStubs.generateDHQReports);
            expect(onRequestSpy).toHaveBeenCalledWith(dhqStubs.processDHQReports);
            expect(onRequestSpy).toHaveBeenCalledWith(dhqStubs.scheduledSyncDHQ3Status);
            expect(onRequestSpy).toHaveBeenCalledWith(dhqStubs.scheduledCountDHQ3Credentials);
        } finally {
            restore();
        }
    });

    it('should invoke wrapped scheduled handlers exported from index.js', async () => {
        const { indexExports, notificationStubs, dhqStubs, restore } = loadIndexWithOnRequestMocks();

        try {
            const scheduledNotificationsRes = await invoke(indexExports.sendScheduledNotificationsGen2, 'POST', {
                body: {
                    scheduleAt: '2026-01-01T00:00:00.000Z',
                },
            });
            expect(notificationStubs.sendScheduledNotifications).toHaveBeenCalledTimes(1);
            expect(scheduledNotificationsRes.statusCode).toBe(210);
            expect(scheduledNotificationsRes._getJSONData().handler).toBe('sendScheduledNotifications');

            const generateReportsRes = await invoke(indexExports.generateDHQReports, 'POST');
            expect(dhqStubs.generateDHQReports).toHaveBeenCalledTimes(1);
            expect(generateReportsRes.statusCode).toBe(211);
            expect(generateReportsRes._getJSONData().handler).toBe('generateDHQReports');

            const processReportsRes = await invoke(indexExports.processDHQReports, 'POST');
            expect(dhqStubs.processDHQReports).toHaveBeenCalledTimes(1);
            expect(processReportsRes.statusCode).toBe(212);
            expect(processReportsRes._getJSONData().handler).toBe('processDHQReports');

            const syncStatusRes = await invoke(indexExports.scheduledSyncDHQ3Status, 'POST');
            expect(dhqStubs.scheduledSyncDHQ3Status).toHaveBeenCalledTimes(1);
            expect(syncStatusRes.statusCode).toBe(213);
            expect(syncStatusRes._getJSONData().handler).toBe('scheduledSyncDHQ3Status');

            const countCredentialsRes = await invoke(indexExports.scheduledCountDHQ3Credentials, 'POST');
            expect(dhqStubs.scheduledCountDHQ3Credentials).toHaveBeenCalledTimes(1);
            expect(countCredentialsRes.statusCode).toBe(214);
            expect(countCredentialsRes._getJSONData().handler).toBe('scheduledCountDHQ3Credentials');
        } finally {
            restore();
        }
    });
});
