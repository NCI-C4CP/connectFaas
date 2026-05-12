const httpMocks = require('node-mocks-http');
const { setupTestSuite } = require('../shared/testHelpers');

let firestore;
let api;
let notifications;
let generateUnsubscribeSignature;
const TEST_UNSUB_SECRET = 'test-unsubscribe-secret-for-hmac';

beforeAll(() => {
    setupTestSuite({
        setupConsole: false,
        setupModuleMocks: true,
    });

    const { incentiveCompleted, eligibleForIncentive } = require('../../utils/incentive');
    const { getToken } = require('../../utils/validation');
    const { getFilteredParticipants, getParticipants, identifyParticipant } = require('../../utils/submission');
    const { submitParticipantsData, updateParticipantData, getBigQueryData } = require('../../utils/sites');
    const { dashboard } = require('../../utils/dashboard');
    const { connectApp } = require('../../utils/connectApp');
    const { biospecimenAPIs } = require('../../utils/biospecimen');
    const { webhook } = require('../../utils/webhook');
    const { heartbeat } = require('../../utils/heartbeat');
    const { physicalActivity } = require('../../utils/reports');
    notifications = require('../../utils/notifications');
    generateUnsubscribeSignature = notifications.generateUnsubscribeSignature;

    api = {
        incentiveCompleted,
        participantsEligibleForIncentive: eligibleForIncentive,
        getParticipantToken: getToken,
        getFilteredParticipants,
        getParticipants,
        identifyParticipant,
        submitParticipantsData,
        updateParticipantData,
        getBigQueryData,
        getParticipantNotification: notifications.getParticipantNotification,
        dashboard,
        app: connectApp,
        biospecimen: biospecimenAPIs,
        webhook,
        heartbeat,
        physicalActivity,
    };

    firestore = require('../../utils/firestore');

});

beforeEach(() => {
    // Re-establish spy before each test (restoreMocks: true clears it after each)
    vi.spyOn(notifications, 'resolveUnsubscribeSecret').mockResolvedValue(TEST_UNSUB_SECRET);
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
            ['getFilteredParticipants', () => api.getFilteredParticipants, 'POST', 'Only GET requests are accepted!'],
            ['getParticipants', () => api.getParticipants, 'POST', 'Only GET requests are accepted!'],
            ['identifyParticipant', () => api.identifyParticipant, 'PUT', 'Only GET or POST requests are accepted!'],
            ['submitParticipantsData', () => api.submitParticipantsData, 'GET', 'Only POST requests are accepted!'],
            ['updateParticipantData', () => api.updateParticipantData, 'GET', 'Only POST requests are accepted!'],
            ['getBigQueryData', () => api.getBigQueryData, 'POST', 'Only GET requests are accepted!'],
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

        it('should return 500 when sendgrid webhook verification setup fails', async () => {
            const { SecretManagerServiceClient } = require('@google-cloud/secret-manager');

            vi.spyOn(SecretManagerServiceClient.prototype, 'accessSecretVersion')
                .mockRejectedValueOnce(new Error('secret lookup failed'));

            const events = [];
            const res = await invoke(api.webhook, 'POST', {
                query: {
                    api: 'sendgrid-email-status',
                },
                body: events,
                rawBody: Buffer.from(JSON.stringify(events)),
            });

            expect(res.statusCode).toBe(500);
            expect(res._getJSONData().code).toBe(500);
        });
    });

    describe('webhook route dispatch', () => {
        it('should return 400 for unknown webhook api values', async () => {
            const res = await invoke(api.webhook, 'POST', {
                query: { api: 'not-a-real-route' },
                body: {},
            });

            expect(res.statusCode).toBe(400);
            expect(res._getJSONData().message).toBe('Bad request!');
        });

        it('should dispatch twilio-message-status through the Twilio validation and processing helpers', async () => {
            const validateSpy = vi.spyOn(notifications, 'validateTwilioRequest').mockResolvedValue(true);
            const processSpy = vi.spyOn(firestore, 'processTwilioEvent').mockResolvedValue(undefined);

            const res = await invoke(api.webhook, 'POST', {
                query: { api: 'twilio-message-status' },
                body: { MessageSid: 'SM123' },
            });

            expect(validateSpy).toHaveBeenCalledTimes(1);
            expect(processSpy).toHaveBeenCalledWith({ MessageSid: 'SM123' });
            expect(res.statusCode).toBe(200);
            expect(res._getJSONData().code).toBe(200);
        });

        it('should dispatch twilio-incoming-sms through the notifications route helper', async () => {
            const incomingSpy = vi.spyOn(notifications, 'handleIncomingSms').mockImplementation(async (_req, res) => {
                return res.status(204).json({ code: 204 });
            });

            const res = await invoke(api.webhook, 'POST', {
                query: { api: 'twilio-incoming-sms' },
                body: { Body: 'hello' },
            });

            expect(incomingSpy).toHaveBeenCalledTimes(1);
            expect(res.statusCode).toBe(204);
        });
    });

    // Baseline specs for webhook processing

    describe('webhook email-unsubscribe route', () => {
        // One-click unsubscribe endpoint
        it('should return 200 and call addEmailSuppression on valid POST', async () => {
            const addEmailSuppressionSpy = vi.spyOn(firestore, 'addEmailSuppression').mockResolvedValue(undefined);
            const sig = generateUnsubscribeSignature('user@example.com', 'tok-abc', TEST_UNSUB_SECRET);

            const res = await invoke(api.webhook, 'POST', {
                query: { api: 'email-unsubscribe', email: 'user@example.com', token: 'tok-abc', sig },
                body: {},
            });

            expect(res.statusCode).toBe(200);
            expect(res._getJSONData().code).toBe(200);
            expect(addEmailSuppressionSpy).toHaveBeenCalledTimes(1);
        });

        it('should return 405 for non-POST requests', async () => {
            const res = await invoke(api.webhook, 'GET', {
                query: { api: 'email-unsubscribe' },
            });

            expect(res.statusCode).toBe(405);
        });

        it('should return 400 when email is missing', async () => {
            const res = await invoke(api.webhook, 'POST', {
                query: { api: 'email-unsubscribe', token: 'tok-abc', sig: 'bad' },
                body: {},
            });

            expect(res.statusCode).toBe(400);
        });

        it('should return 403 when token or sig is missing', async () => {
            const res = await invoke(api.webhook, 'POST', {
                query: { api: 'email-unsubscribe', email: 'user@example.com' },
                body: {},
            });

            expect(res.statusCode).toBe(403);
        });

        it('should return 403 when signature is invalid', async () => {
            const res = await invoke(api.webhook, 'POST', {
                query: { api: 'email-unsubscribe', email: 'user@example.com', token: 'tok-abc', sig: 'badsig' },
                body: {},
            });

            expect(res.statusCode).toBe(403);
        });

        it('should return 500 when unsubscribe secret resolution fails', async () => {
            vi.spyOn(notifications, 'resolveUnsubscribeSecret')
                .mockRejectedValueOnce(new Error('secret unavailable'));

            const res = await invoke(api.webhook, 'POST', {
                query: { api: 'email-unsubscribe', email: 'user@example.com', token: 'tok-abc', sig: 'ignored' },
                body: {},
            });

            expect(res.statusCode).toBe(500);
            expect(res._getJSONData().message).toBe('Internal configuration error.');
        });

        it('should normalize email to lowercase before suppression', async () => {
            const addEmailSuppressionSpy = vi.spyOn(firestore, 'addEmailSuppression').mockResolvedValue(undefined);
            const sig = generateUnsubscribeSignature('user@example.com', 'tok-abc', TEST_UNSUB_SECRET);

            await invoke(api.webhook, 'POST', {
                query: { api: 'email-unsubscribe', email: '  User@Example.COM  ', token: 'tok-abc', sig },
                body: {},
            });

            expect(addEmailSuppressionSpy).toHaveBeenCalledTimes(1);
            const calledEmail = addEmailSuppressionSpy.mock.calls[0][0];
            expect(calledEmail).toBe('user@example.com');
        });

        it('should suppress bulk only (suppressBulk:true, suppressTransactional:false)', async () => {
            const addEmailSuppressionSpy = vi.spyOn(firestore, 'addEmailSuppression').mockResolvedValue(undefined);
            const sig = generateUnsubscribeSignature('user@example.com', 'tok-abc', TEST_UNSUB_SECRET);

            await invoke(api.webhook, 'POST', {
                query: { api: 'email-unsubscribe', email: 'user@example.com', token: 'tok-abc', sig },
                body: {},
            });

            expect(addEmailSuppressionSpy).toHaveBeenCalledWith(
                'user@example.com',
                'unsubscribed',
                null,
                true,
                false,
                { token: 'tok-abc' }
            );
        });

        it('should pass the unsubscribe token through for data-destruction linkage', async () => {
            const addEmailSuppressionSpy = vi.spyOn(firestore, 'addEmailSuppression').mockResolvedValue(undefined);
            const sig = generateUnsubscribeSignature('user@example.com', 'tok-abc', TEST_UNSUB_SECRET);

            await invoke(api.webhook, 'POST', {
                query: { api: 'email-unsubscribe', email: 'user@example.com', token: 'tok-abc', sig },
                body: {},
            });

            expect(addEmailSuppressionSpy).toHaveBeenCalledWith(
                'user@example.com',
                'unsubscribed',
                null,
                true,
                false,
                { token: 'tok-abc' }
            );
        });

        it('should accept email from query params for one-click List-Unsubscribe', async () => {
            const addEmailSuppressionSpy = vi.spyOn(firestore, 'addEmailSuppression').mockResolvedValue(undefined);
            const sig = generateUnsubscribeSignature('oneclick@example.com', 'tok-123', TEST_UNSUB_SECRET);

            const res = await invoke(api.webhook, 'POST', {
                query: { api: 'email-unsubscribe', email: 'oneclick@example.com', token: 'tok-123', sig },
                body: {},
            });

            expect(res.statusCode).toBe(200);
            expect(addEmailSuppressionSpy).toHaveBeenCalledTimes(1);
            expect(addEmailSuppressionSpy.mock.calls[0][0]).toBe('oneclick@example.com');
        });
    });

    describe('webhook concurrent event processing', () => {
        const setupSendGridMocks = () => {
            const { SecretManagerServiceClient } = require('@google-cloud/secret-manager');
            const { EventWebhook, EventWebhookHeader } = require('@sendgrid/eventwebhook');

            vi.spyOn(SecretManagerServiceClient.prototype, 'accessSecretVersion').mockResolvedValue([
                { payload: { data: Buffer.from('public-key') } },
            ]);
            vi.spyOn(EventWebhook.prototype, 'convertPublicKeyToECDSA').mockReturnValue('ecdsa-key');
            vi.spyOn(EventWebhook.prototype, 'verifySignature').mockReturnValue(true);

            return EventWebhookHeader;
        };

        const makeCorrelatedSendGridEvent = (overrides = {}) => ({
            event: 'delivered',
            email: 'user@test.com',
            notification_id: 'notif-1',
            gcloud_project: process.env.GCLOUD_PROJECT,
            ...overrides,
        });

        it('should process events concurrently in chunks of 10', async () => {
            const EventWebhookHeader = setupSendGridMocks();
            const processSpy = vi.spyOn(firestore, 'processSendGridEvent').mockResolvedValue(undefined);

            const events = Array.from({ length: 25 }, (_, i) => makeCorrelatedSendGridEvent({
                email: `user${i}@test.com`,
                notification_id: `notif-${i}`,
            }));
            const res = await invoke(api.webhook, 'POST', {
                query: { api: 'sendgrid-email-status' },
                body: events,
                rawBody: Buffer.from(JSON.stringify(events)),
                headers: {
                    [EventWebhookHeader.SIGNATURE()]: 'valid-signature',
                    [EventWebhookHeader.TIMESTAMP()]: '1700000002',
                },
            });

            expect(res.statusCode).toBe(200);
            expect(processSpy).toHaveBeenCalledTimes(25);
        });

        it('should return 200 on partial failure to avoid SendGrid redelivering already-processed events', async () => {
            const EventWebhookHeader = setupSendGridMocks();
            const processSpy = vi.spyOn(firestore, 'processSendGridEvent')
                .mockRejectedValueOnce(new Error('Event 0 failed'))
                .mockResolvedValueOnce(undefined)
                .mockResolvedValueOnce(undefined);

            const events = [
                makeCorrelatedSendGridEvent({ event: 'bounce', email: 'fail@test.com', notification_id: 'notif-fail' }),
                makeCorrelatedSendGridEvent({ email: 'ok1@test.com', notification_id: 'notif-ok1' }),
                makeCorrelatedSendGridEvent({ email: 'ok2@test.com', notification_id: 'notif-ok2' }),
            ];
            const res = await invoke(api.webhook, 'POST', {
                query: { api: 'sendgrid-email-status' },
                body: events,
                rawBody: Buffer.from(JSON.stringify(events)),
                headers: {
                    [EventWebhookHeader.SIGNATURE()]: 'valid-signature',
                    [EventWebhookHeader.TIMESTAMP()]: '1700000003',
                },
            });

            expect(res.statusCode).toBe(200);
            expect(processSpy).toHaveBeenCalledTimes(3);
            const data = res._getJSONData();
            expect(data.processed).toBe(2);
            expect(data.failed).toBe(1);
        });

        it('should log errors for failed events without crashing', async () => {
            const EventWebhookHeader = setupSendGridMocks();
            const consoleSpy = vi.spyOn(console, 'error');
            vi.spyOn(firestore, 'processSendGridEvent')
                .mockRejectedValueOnce(new Error('processing error'));

            const events = [makeCorrelatedSendGridEvent({
                event: 'bounce',
                email: 'fail@test.com',
                notification_id: 'notif-fail',
            })];
            const res = await invoke(api.webhook, 'POST', {
                query: { api: 'sendgrid-email-status' },
                body: events,
                rawBody: Buffer.from(JSON.stringify(events)),
                headers: {
                    [EventWebhookHeader.SIGNATURE()]: 'valid-signature',
                    [EventWebhookHeader.TIMESTAMP()]: '1700000004',
                },
            });

            expect(res.statusCode).toBe(500);
            const errorCalls = consoleSpy.mock.calls.map(c => c[0]);
            expect(errorCalls.some(msg => typeof msg === 'string' && msg.includes('SendGrid event processing error'))).toBe(true);
        });

        it('should process all events even when some fail and return 200 for mixed batches', async () => {
            const EventWebhookHeader = setupSendGridMocks();
            const processSpy = vi.spyOn(firestore, 'processSendGridEvent');
            processSpy.mockRejectedValueOnce(new Error('fail 1'));
            processSpy.mockResolvedValueOnce(undefined);
            processSpy.mockRejectedValueOnce(new Error('fail 3'));
            processSpy.mockResolvedValueOnce(undefined);

            const events = Array.from({ length: 4 }, (_, i) => makeCorrelatedSendGridEvent({
                email: `u${i}@test.com`,
                notification_id: `notif-${i}`,
            }));
            const res = await invoke(api.webhook, 'POST', {
                query: { api: 'sendgrid-email-status' },
                body: events,
                rawBody: Buffer.from(JSON.stringify(events)),
                headers: {
                    [EventWebhookHeader.SIGNATURE()]: 'valid-signature',
                    [EventWebhookHeader.TIMESTAMP()]: '1700000005',
                },
            });

            expect(res.statusCode).toBe(200);
            expect(processSpy).toHaveBeenCalledTimes(4);
            const data = res._getJSONData();
            expect(data.failed).toBe(2);
            expect(data.processed).toBe(2);
        });

        it('should return 500 when every event in the batch fails so SendGrid retries', async () => {
            const EventWebhookHeader = setupSendGridMocks();
            const processSpy = vi.spyOn(firestore, 'processSendGridEvent');
            processSpy.mockRejectedValueOnce(new Error('fail 1'));
            processSpy.mockRejectedValueOnce(new Error('fail 2'));

            const events = Array.from({ length: 2 }, (_, i) => makeCorrelatedSendGridEvent({
                email: `u${i}@test.com`,
                notification_id: `notif-${i}`,
            }));
            const res = await invoke(api.webhook, 'POST', {
                query: { api: 'sendgrid-email-status' },
                body: events,
                rawBody: Buffer.from(JSON.stringify(events)),
                headers: {
                    [EventWebhookHeader.SIGNATURE()]: 'valid-signature',
                    [EventWebhookHeader.TIMESTAMP()]: '1700000099',
                },
            });

            expect(res.statusCode).toBe(500);
            expect(processSpy).toHaveBeenCalledTimes(2);
        });

        it('should ignore uncorrelated events on the normal SendGrid webhook route', async () => {
            const EventWebhookHeader = setupSendGridMocks();
            const processSpy = vi.spyOn(firestore, 'processSendGridEvent').mockResolvedValue(undefined);

            const events = [
                { event: 'processed', email: 'example1@test.com' },
                { event: 'delivered', email: 'example2@test.com', gcloud_project: 'other-project' },
            ];
            const res = await invoke(api.webhook, 'POST', {
                query: { api: 'sendgrid-email-status' },
                body: events,
                rawBody: Buffer.from(JSON.stringify(events)),
                headers: {
                    [EventWebhookHeader.SIGNATURE()]: 'valid-signature',
                    [EventWebhookHeader.TIMESTAMP()]: '1700000006',
                },
            });

            expect(res.statusCode).toBe(200);
            expect(processSpy).not.toHaveBeenCalled();
            const data = res._getJSONData();
            expect(data.exampleEventsAccepted).toBe(0);
            expect(data.processed).toBe(0);
            expect(data.ignored).toBe(2);
        });

        it('should accept uncorrelated SendGrid example events on the test route', async () => {
            const EventWebhookHeader = setupSendGridMocks();
            const processSpy = vi.spyOn(firestore, 'processSendGridEvent').mockResolvedValue(undefined);

            const events = [
                { event: 'processed', email: 'example1@test.com' },
                { event: 'delivered', email: 'example2@test.com', gcloud_project: 'other-project' },
            ];
            const res = await invoke(api.webhook, 'POST', {
                query: { api: 'sendgrid-email-status-test' },
                body: events,
                rawBody: Buffer.from(JSON.stringify(events)),
                headers: {
                    [EventWebhookHeader.SIGNATURE()]: 'valid-signature',
                    [EventWebhookHeader.TIMESTAMP()]: '1700000006',
                },
            });

            expect(res.statusCode).toBe(200);
            expect(processSpy).not.toHaveBeenCalled();
            const data = res._getJSONData();
            expect(data.exampleEventsAccepted).toBe(2);
            expect(data.processed).toBe(0);
            expect(data.ignored).toBe(0);
        });
    });
});

describe('Index onRequest Wrapper Handlers', () => {
    const indexPath = require.resolve('../../index.js');
    const httpsPath = require.resolve('firebase-functions/v2/https');
    const tasksPath = require.resolve('firebase-functions/v2/tasks');
    const notificationsPath = require.resolve('../../utils/notifications.js');
    const eventsPath = require.resolve('../../utils/events.js');
    const participantDataCleanupPath = require.resolve('../../utils/participantDataCleanup.js');
    const dhqPath = require.resolve('../../utils/dhq.js');

    const loadIndexWithOnRequestMocks = () => {
        const originalIndex = require.cache[indexPath];
        const originalHttps = require.cache[httpsPath];
        const originalTasks = require.cache[tasksPath];
        const originalNotifications = require.cache[notificationsPath];
        const originalEvents = require.cache[eventsPath];
        const originalParticipantDataCleanup = require.cache[participantDataCleanupPath];
        const originalDhq = require.cache[dhqPath];

        const onRequestSpy = vi.fn((handler) => handler);
        const onTaskDispatchedSpy = vi.fn((optsOrHandler, maybeHandler) =>
            typeof maybeHandler === 'function' ? maybeHandler : optsOrHandler
        );

        const notificationStubs = {
            getParticipantNotification: vi.fn(),
            sendScheduledNotifications: vi.fn((req, res) => {
                return Promise.resolve(res.status(200).json({
                    code: 200,
                    handler: 'sendScheduledNotifications',
                    method: req.method,
                }));
            }),
            processNotificationBatchBulkDefault: vi.fn(async (req) => ({
                code: 216,
                handler: 'processNotificationBatchBulkDefault',
                method: req?.method || 'TASK',
            })),
            processNotificationBatchBulkMicrosoft: vi.fn(async (req) => ({
                code: 217,
                handler: 'processNotificationBatchBulkMicrosoft',
                method: req?.method || 'TASK',
            })),
        };

        const eventStubs = {
            importToBigQuery: vi.fn((req, res) => {
                return Promise.resolve(res.status(200).json({
                    code: 200,
                    handler: 'importToBigQuery',
                    method: req.method,
                }));
            }),
            firestoreExport: vi.fn((req, res) => {
                return Promise.resolve(res.status(200).json({
                    code: 200,
                    handler: 'firestoreExport',
                    method: req.method,
                }));
            }),
            exportNotificationsToBucket: vi.fn((req, res) => {
                return Promise.resolve(res.status(200).json({
                    code: 200,
                    handler: 'exportNotificationsToBucket',
                    method: req.method,
                }));
            }),
        };

        const participantDataCleanupStubs = {
            participantDataCleanup: vi.fn((req, res) => {
                return Promise.resolve(res.status(200).json({
                    code: 200,
                    handler: 'participantDataCleanup',
                    method: req.method,
                }));
            }),
        };

        const dhqStubs = {
            generateDHQReports: vi.fn((req, res) => {
                return Promise.resolve(res.status(200).json({
                    code: 200,
                    handler: 'generateDHQReports',
                    method: req.method,
                }));
            }),
            processDHQReports: vi.fn((req, res) => {
                return Promise.resolve(res.status(200).json({
                    code: 200,
                    handler: 'processDHQReports',
                    method: req.method,
                }));
            }),
            scheduledSyncDHQ3Status: vi.fn((req, res) => {
                return Promise.resolve(res.status(200).json({
                    code: 200,
                    handler: 'scheduledSyncDHQ3Status',
                    method: req.method,
                }));
            }),
            scheduledCountDHQ3Credentials: vi.fn((req, res) => {
                return Promise.resolve(res.status(200).json({
                    code: 200,
                    handler: 'scheduledCountDHQ3Credentials',
                    method: req.method,
                }));
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

        require.cache[tasksPath] = {
            id: tasksPath,
            filename: tasksPath,
            loaded: true,
            exports: {
                onTaskDispatched: onTaskDispatchedSpy,
            },
        };

        require.cache[notificationsPath] = {
            id: notificationsPath,
            filename: notificationsPath,
            loaded: true,
            exports: notificationStubs,
        };

        require.cache[eventsPath] = {
            id: eventsPath,
            filename: eventsPath,
            loaded: true,
            exports: eventStubs,
        };

        require.cache[participantDataCleanupPath] = {
            id: participantDataCleanupPath,
            filename: participantDataCleanupPath,
            loaded: true,
            exports: participantDataCleanupStubs,
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

            if (originalTasks) {
                require.cache[tasksPath] = originalTasks;
            } else {
                delete require.cache[tasksPath];
            }

            if (originalNotifications) {
                require.cache[notificationsPath] = originalNotifications;
            } else {
                delete require.cache[notificationsPath];
            }

            if (originalEvents) {
                require.cache[eventsPath] = originalEvents;
            } else {
                delete require.cache[eventsPath];
            }

            if (originalParticipantDataCleanup) {
                require.cache[participantDataCleanupPath] = originalParticipantDataCleanup;
            } else {
                delete require.cache[participantDataCleanupPath];
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
            onTaskDispatchedSpy,
            notificationStubs,
            eventStubs,
            participantDataCleanupStubs,
            dhqStubs,
            restore,
        };
    };

    it('should register wrapped handlers via onRequest and task handlers via onTaskDispatched in index.js', () => {
        const {
            onRequestSpy,
            onTaskDispatchedSpy,
            notificationStubs,
            eventStubs,
            participantDataCleanupStubs,
            dhqStubs,
            restore,
        } = loadIndexWithOnRequestMocks();

        try {
            const expectedOnRequestHandlers = [
                notificationStubs.sendScheduledNotifications,
                eventStubs.importToBigQuery,
                eventStubs.firestoreExport,
                eventStubs.exportNotificationsToBucket,
                participantDataCleanupStubs.participantDataCleanup,
                dhqStubs.generateDHQReports,
                dhqStubs.processDHQReports,
                dhqStubs.scheduledSyncDHQ3Status,
                dhqStubs.scheduledCountDHQ3Credentials,
            ];

            expect(onRequestSpy).toHaveBeenCalledTimes(expectedOnRequestHandlers.length);
            for (const handler of expectedOnRequestHandlers) {
                expect(onRequestSpy).toHaveBeenCalledWith(handler);
            }

            expect(onTaskDispatchedSpy).toHaveBeenCalledTimes(2);
            expect(onTaskDispatchedSpy).toHaveBeenCalledWith(notificationStubs.processNotificationBatchBulkDefault);
            expect(onTaskDispatchedSpy).toHaveBeenCalledWith(notificationStubs.processNotificationBatchBulkMicrosoft);
        } finally {
            restore();
        }
    });

    it('should invoke wrapped handlers exported from index.js', async () => {
        const {
            indexExports,
            notificationStubs,
            eventStubs,
            participantDataCleanupStubs,
            dhqStubs,
            restore,
        } = loadIndexWithOnRequestMocks();

        try {
            const scheduledNotificationsRes = await invoke(indexExports.sendScheduledNotifications, 'POST', {
                body: {
                    scheduleAt: '2026-01-01T00:00:00.000Z',
                },
            });
            expect(notificationStubs.sendScheduledNotifications).toHaveBeenCalledTimes(1);
            expect(scheduledNotificationsRes.statusCode).toBe(200);
            expect(scheduledNotificationsRes._getJSONData().handler).toBe('sendScheduledNotifications');

            const importToBigQueryRes = await invoke(indexExports.importToBigQuery, 'POST');
            expect(eventStubs.importToBigQuery).toHaveBeenCalledTimes(1);
            expect(importToBigQueryRes.statusCode).toBe(200);
            expect(importToBigQueryRes._getJSONData().handler).toBe('importToBigQuery');

            const firestoreExportRes = await invoke(indexExports.scheduleFirestoreDataExport, 'POST');
            expect(eventStubs.firestoreExport).toHaveBeenCalledTimes(1);
            expect(firestoreExportRes.statusCode).toBe(200);
            expect(firestoreExportRes._getJSONData().handler).toBe('firestoreExport');

            const exportNotificationsRes = await invoke(indexExports.exportNotificationsToBucket, 'POST');
            expect(eventStubs.exportNotificationsToBucket).toHaveBeenCalledTimes(1);
            expect(exportNotificationsRes.statusCode).toBe(200);
            expect(exportNotificationsRes._getJSONData().handler).toBe('exportNotificationsToBucket');

            const participantDataCleanupRes = await invoke(indexExports.participantDataCleanup, 'POST');
            expect(participantDataCleanupStubs.participantDataCleanup).toHaveBeenCalledTimes(1);
            expect(participantDataCleanupRes.statusCode).toBe(200);
            expect(participantDataCleanupRes._getJSONData().handler).toBe('participantDataCleanup');

            const generateReportsRes = await invoke(indexExports.generateDHQReports, 'POST');
            expect(dhqStubs.generateDHQReports).toHaveBeenCalledTimes(1);
            expect(generateReportsRes.statusCode).toBe(200);
            expect(generateReportsRes._getJSONData().handler).toBe('generateDHQReports');

            const processReportsRes = await invoke(indexExports.processDHQReports, 'POST');
            expect(dhqStubs.processDHQReports).toHaveBeenCalledTimes(1);
            expect(processReportsRes.statusCode).toBe(200);
            expect(processReportsRes._getJSONData().handler).toBe('processDHQReports');

            const syncStatusRes = await invoke(indexExports.scheduledSyncDHQ3Status, 'POST');
            expect(dhqStubs.scheduledSyncDHQ3Status).toHaveBeenCalledTimes(1);
            expect(syncStatusRes.statusCode).toBe(200);
            expect(syncStatusRes._getJSONData().handler).toBe('scheduledSyncDHQ3Status');

            const countCredentialsRes = await invoke(indexExports.scheduledCountDHQ3Credentials, 'POST');
            expect(dhqStubs.scheduledCountDHQ3Credentials).toHaveBeenCalledTimes(1);
            expect(countCredentialsRes.statusCode).toBe(200);
            expect(countCredentialsRes._getJSONData().handler).toBe('scheduledCountDHQ3Credentials');

            const defaultTaskResult = await indexExports.processNotificationBatchBulkDefault({ data: { runId: 'run-1', batchId: 'default-batch-1' } });
            expect(notificationStubs.processNotificationBatchBulkDefault).toHaveBeenCalledTimes(1);
            expect(defaultTaskResult.handler).toBe('processNotificationBatchBulkDefault');

            const microsoftTaskResult = await indexExports.processNotificationBatchBulkMicrosoft({ data: { runId: 'run-1', batchId: 'microsoft-batch-1' } });
            expect(notificationStubs.processNotificationBatchBulkMicrosoft).toHaveBeenCalledTimes(1);
            expect(microsoftTaskResult.handler).toBe('processNotificationBatchBulkMicrosoft');
        } finally {
            restore();
        }
    });
});
