const { createFirebaseMocks } = require("../mocks/mockFactory");

let factory;
let mocks;
let audit;
let fieldMapping;
let policy;
let sharedModule;

const firestoreCacheKey = require.resolve("../../utils/firestore");
const auditCacheKey = require.resolve("../../utils/dataDestructionAudit");

const destroyedParticipantDoc = (overrides = {}) => {
    const data = {
        Connect_ID: 1001,
        token: "token-1001",
        pin: "123456",
        query: { firstName: "Jane", lastName: "Doe", studyId: "S1" },
        state: { uid: "uid-1001" },
        [fieldMapping.participantMap.dataHasBeenDestroyed.toString()]: fieldMapping.yes,
        [fieldMapping.participantMap.dateTimeDataDestroyed.toString()]: "2026-05-14T05:00:00.000Z",
        [fieldMapping.participationStatus]: fieldMapping.participantMap.dataDestroyedStatus,
        ...overrides,
    };

    return {
        id: "participant-doc-1001",
        data: () => data,
        exists: true,
        ref: { id: "participant-doc-1001" },
    };
};

const emptySnapshot = { empty: true, size: 0, docs: [] };

const queryReturning = (snapshot) => ({
    where: vi.fn().mockReturnThis(),
    select: vi.fn().mockReturnThis(),
    limit: vi.fn().mockReturnThis(),
    get: vi.fn().mockResolvedValue(snapshot),
});

const okJson = (body) => ({
    ok: true,
    status: 200,
    json: vi.fn().mockResolvedValue(body),
});

describe("dataDestructionAudit", () => {
    let savedFirestoreCache;
    let savedAuditCache;
    let restoreRequire;

    beforeAll(() => {
        savedFirestoreCache = require.cache[firestoreCacheKey];
        savedAuditCache = require.cache[auditCacheKey];
        delete require.cache[firestoreCacheKey];
        delete require.cache[auditCacheKey];

        const mockResult = createFirebaseMocks({
            setupConsole: false,
            setupModuleMocks: false,
        });
        factory = mockResult.factory;
        mocks = mockResult.mocks;
        restoreRequire = factory.helpers.setupModuleMocks(mocks.admin);

        process.env.GCLOUD_PROJECT = "nih-nci-dceg-connect-prod-6d04";

        if (!vi.isMockFunction(console.log)) vi.spyOn(console, "log").mockImplementation(() => {});
        if (!vi.isMockFunction(console.error)) vi.spyOn(console, "error").mockImplementation(() => {});

        fieldMapping = require("../../utils/fieldToConceptIdMapping");
        policy = require("../../utils/dataDestructionPolicy");
        sharedModule = require("../../utils/shared");
        audit = require("../../utils/dataDestructionAudit");
    });

    afterAll(() => {
        if (restoreRequire) restoreRequire();

        if (savedFirestoreCache) {
            require.cache[firestoreCacheKey] = savedFirestoreCache;
        } else {
            delete require.cache[firestoreCacheKey];
        }
        if (savedAuditCache) {
            require.cache[auditCacheKey] = savedAuditCache;
        } else {
            delete require.cache[auditCacheKey];
        }

        vi.restoreAllMocks();
        factory.reset();
    });

    beforeEach(() => {
        mocks.firestore.collection.mockReset();
        mocks.firestore.batch.mockReset();
        mocks.storage.bucket.mockReset();
    });

    afterEach(() => {
        delete process.env.BOX_CLIENT_ID_SECRET;
        delete process.env.BOX_CLIENT_SECRET;
        delete process.env.BOX_ENTERPRISE_ID;
        delete process.env.GCLOUD_SENDGRID_SECRET;
    });

    const stubAppSettings = (dataDestructionAudit) =>
        vi.fn().mockResolvedValue(
            dataDestructionAudit === undefined ? {} : { dataDestructionAudit }
        );

    it("uses token, DHQ username, and Connect_ID lookup keys for orphan detection", () => {
        const participant = {
            Connect_ID: 1001,
            token: "token-1001",
            [fieldMapping.dhq3Username]: "dhq-user-1",
        };

        expect(audit.getDataDestructionCollectionQuerySpec("emailAddressStatus", participant)).toMatchObject({
            field: "token",
            value: "token-1001",
        });
        expect(audit.getDataDestructionCollectionQuerySpec("notifications", participant)).toMatchObject({
            field: "token",
            value: "token-1001",
        });
        expect(audit.getDataDestructionCollectionQuerySpec("ssn", participant)).toMatchObject({
            field: "token",
            value: "token-1001",
        });
        expect(audit.getDataDestructionCollectionQuerySpec("dhqRawAnswers", participant)).toMatchObject({
            field: fieldMapping.dhq3Username.toString(),
            value: "dhq-user-1",
        });
        expect(audit.getDataDestructionCollectionQuerySpec("bioSurvey_v1", participant)).toMatchObject({
            field: "Connect_ID",
            value: 1001,
        });
    });

    describe("normalizeAuditOptions", () => {
        it("requires exact confirmation for cleanup mode", () => {
            expect(() => audit.normalizeAuditOptions({ mode: "cleanup" })).toThrow(/confirmCleanup/);
            expect(audit.normalizeAuditOptions({
                mode: "cleanup",
                confirmCleanup: "DELETE_ORPHANED_DATA",
                connectIds: [1001],
            })).toEqual({
                mode: "cleanup",
                dryRun: false,
                connectIds: [1001],
            });
        });

        it("accepts dryRun without a confirmation token", () => {
            expect(audit.normalizeAuditOptions({
                mode: "cleanup",
                dryRun: true,
                connectIds: [1001, 2002],
            })).toEqual({
                mode: "cleanup",
                dryRun: true,
                connectIds: [1001, 2002],
            });
        });

        it("rejects dryRun without cleanup mode", () => {
            expect(() => audit.normalizeAuditOptions({ dryRun: true })).toThrow(/dryRun is only valid with cleanup mode/);
        });

        it("rejects invalid modes", () => {
            expect(() => audit.normalizeAuditOptions({ mode: "wipe" })).toThrow(/Invalid mode/);
        });

        it("rejects connectIds outside cleanup mode", () => {
            expect(() => audit.normalizeAuditOptions({ mode: "audit", connectIds: [1] })).toThrow(/connectIds can only be used with cleanup/);
        });

        it("rejects non-array connectIds", () => {
            expect(() => audit.normalizeAuditOptions({
                mode: "cleanup",
                dryRun: true,
                connectIds: "1001",
            })).toThrow(/connectIds must be an array/);
        });

        it("rejects non-numeric connectIds", () => {
            expect(() => audit.normalizeAuditOptions({
                mode: "cleanup",
                dryRun: true,
                connectIds: ["abc"],
            })).toThrow(/connectIds must be an array of numbers/);
        });

        it("parses a JSON-string body", () => {
            const result = audit.normalizeAuditOptions(JSON.stringify({ mode: "audit" }));
            expect(result).toEqual({ mode: "audit", dryRun: false, connectIds: [] });
        });
    });

    describe("getProjectContext", () => {
        it("returns the developmentTier from shared.js", () => {
            const context = audit.getProjectContext();
            expect(context.tier).toBe(sharedModule.developmentTier);
            expect(["DEV", "STAGE", "PROD"]).toContain(context.tier);
            expect(context.projectId).toBe(process.env.GCLOUD_PROJECT || "");
        });
    });

    describe("policy resolution per participant", () => {
        it("records policyResolution with the destruction timestamp and resolves to V0 while V1 is not yet effective", async () => {
            const participantDoc = destroyedParticipantDoc();
            mocks.firestore.collection.mockImplementation(() => ({
                where: vi.fn().mockReturnValue(queryReturning(emptySnapshot)),
            }));

            const result = await audit.auditParticipantDataDestruction({
                doc: participantDoc,
                mode: "audit",
                dryRun: false,
                runId: "run-1",
                projectId: "p",
                tier: "PROD",
                checkedAt: "2026-05-14T05:00:00.000Z",
            });

            expect(result.policyVersion).toBe("v0");
            expect(result.policyResolution).toEqual({
                destructionAt: "2026-05-14T05:00:00.000Z",
                effectiveFrom: null,
                appliedDeltas: [],
            });
        });

        it("falls back to V0 when the destruction timestamp is missing", async () => {
            const dateTimeCid = fieldMapping.participantMap.dateTimeDataDestroyed.toString();
            const participantDoc = destroyedParticipantDoc({ [dateTimeCid]: undefined });
            mocks.firestore.collection.mockImplementation(() => ({
                where: vi.fn().mockReturnValue(queryReturning(emptySnapshot)),
            }));

            const result = await audit.auditParticipantDataDestruction({
                doc: participantDoc,
                mode: "audit",
                dryRun: false,
                runId: "run-1",
                projectId: "p",
                tier: "PROD",
                checkedAt: "2026-05-14T05:00:00.000Z",
            });

            expect(result.policyVersion).toBe("v0");
            expect(result.policyResolution.destructionAt).toBeNull();
        });
    });

    it("does not mutate data in read-only audit mode", async () => {
        const participantDoc = destroyedParticipantDoc();
        const relatedRef = { id: "email-status-1" };
        const relatedSnapshot = {
            empty: false,
            size: 1,
            docs: [{ id: "email-status-1", ref: relatedRef, data: () => ({}) }],
        };
        const participantUpdate = vi.fn().mockResolvedValue();
        const batchDelete = vi.fn();

        mocks.firestore.batch.mockReturnValue({
            delete: batchDelete,
            commit: vi.fn().mockResolvedValue(),
        });

        mocks.firestore.collection.mockImplementation((collectionPath) => {
            if (collectionPath === "participants") {
                return {
                    doc: vi.fn().mockReturnValue({ update: participantUpdate }),
                };
            }

            if (collectionPath === "emailAddressStatus") {
                return { where: vi.fn().mockReturnValue(queryReturning(relatedSnapshot)) };
            }

            if (collectionPath === "pathologyReports") {
                return { where: vi.fn().mockReturnValue(queryReturning(emptySnapshot)) };
            }

            return { where: vi.fn().mockReturnValue(queryReturning(emptySnapshot)) };
        });

        const result = await audit.auditParticipantDataDestruction({
            doc: participantDoc,
            mode: "audit",
            dryRun: false,
            runId: "run-1",
            projectId: "project",
            tier: "PROD",
            checkedAt: "2026-05-14T05:00:00.000Z",
        });

        expect(result.orphanedCollections).toEqual([
            {
                collection: "emailAddressStatus",
                queryField: "token",
                count: 1,
            },
        ]);
        expect(result.status).toBe("fail");
        expect(batchDelete).not.toHaveBeenCalled();
        expect(participantUpdate).not.toHaveBeenCalled();
    });

    it("cleanup mode removes orphan docs, pathology metadata, and unexpected stub extras", async () => {
        const participantDoc = destroyedParticipantDoc({
            unexpectedField: "remove",
            query: {
                firstName: "Jane",
                lastName: "Doe",
                studyId: "S1",
                extraQuery: "remove",
            },
        });

        const relatedRef = { id: "email-status-1" };
        const relatedSnapshot = {
            empty: false,
            size: 1,
            docs: [{ id: "email-status-1", ref: relatedRef, data: () => ({}) }],
        };
        const fileNameCidStr = fieldMapping.pathologyReportFilename.toString();
        const pathologyRef = { id: "pathology-1" };
        const pathologySnapshot = {
            empty: false,
            size: 1,
            docs: [{
                id: "pathology-1",
                ref: pathologyRef,
                data: () => ({
                    bucketName: "pathology-reports_kpco_connect-prod",
                    [fileNameCidStr]: "not-output.pdf",
                }),
            }],
        };

        const participantUpdate = vi.fn().mockResolvedValue();
        const batchDelete = vi.fn();
        const batchCommit = vi.fn().mockResolvedValue();
        const batch = {
            delete: batchDelete,
            commit: batchCommit,
        };
        batchDelete.mockReturnValue(batch);
        mocks.firestore.batch.mockReturnValue(batch);

        mocks.storage.bucket.mockReturnValue({
            exists: vi.fn().mockResolvedValue([true]),
            getFiles: vi.fn().mockResolvedValue([[]]),
        });

        mocks.firestore.collection.mockImplementation((collectionPath) => {
            if (collectionPath === "participants") {
                return {
                    doc: vi.fn().mockReturnValue({ update: participantUpdate }),
                };
            }

            if (collectionPath === "emailAddressStatus") {
                return { where: vi.fn().mockReturnValue(queryReturning(relatedSnapshot)) };
            }

            if (collectionPath === "pathologyReports") {
                return {
                    doc: vi.fn().mockReturnValue(pathologyRef),
                    where: vi.fn().mockReturnValue(queryReturning(pathologySnapshot)),
                };
            }

            return { where: vi.fn().mockReturnValue(queryReturning(emptySnapshot)) };
        });

        const result = await audit.auditParticipantDataDestruction({
            doc: participantDoc,
            mode: "cleanup",
            dryRun: false,
            runId: "run-1",
            projectId: "project",
            tier: "PROD",
            checkedAt: "2026-05-14T05:00:00.000Z",
        });

        expect(batchDelete).toHaveBeenCalledWith(relatedRef);
        expect(batchDelete).toHaveBeenCalledWith(pathologyRef);
        expect(participantUpdate).toHaveBeenCalledOnce();
        const updateArg = participantUpdate.mock.calls[0][0];
        expect(updateArg).toHaveProperty("unexpectedField");
        expect(updateArg).toHaveProperty("query.extraQuery");
        expect(updateArg).not.toHaveProperty("Connect_ID");
        expect(result.cleanupActions.map((action) => action.type)).toEqual(expect.arrayContaining([
            "deleteRelatedDocs",
            "deletePathologyMetadata",
            "deleteUnexpectedStubFields",
        ]));
        expect(result.cleanupActions.every((action) => !action.dryRun)).toBe(true);
        expect(JSON.stringify(result)).not.toContain("not-output.pdf");
    });

    it("dry-run cleanup records planned actions without any deletes or updates", async () => {
        const participantDoc = destroyedParticipantDoc({
            unexpectedField: "remove",
        });

        const relatedRef = { id: "email-status-1" };
        const relatedSnapshot = {
            empty: false,
            size: 1,
            docs: [{ id: "email-status-1", ref: relatedRef, data: () => ({}) }],
        };
        const fileNameCidStr = fieldMapping.pathologyReportFilename.toString();
        const pathologyRef = { id: "pathology-1" };
        const pathologySnapshot = {
            empty: false,
            size: 1,
            docs: [{
                id: "pathology-1",
                ref: pathologyRef,
                data: () => ({
                    bucketName: "pathology-reports_kpco_connect-prod",
                    [fileNameCidStr]: "not-output.pdf",
                }),
            }],
        };

        const participantUpdate = vi.fn().mockResolvedValue();
        const batchDelete = vi.fn();
        const batchCommit = vi.fn().mockResolvedValue();
        const fileDelete = vi.fn().mockResolvedValue();
        const batch = { delete: batchDelete, commit: batchCommit };
        batchDelete.mockReturnValue(batch);
        mocks.firestore.batch.mockReturnValue(batch);

        mocks.storage.bucket.mockReturnValue({
            exists: vi.fn().mockResolvedValue([true]),
            getFiles: vi.fn().mockResolvedValue([[{ delete: fileDelete }]]),
        });

        mocks.firestore.collection.mockImplementation((collectionPath) => {
            if (collectionPath === "participants") {
                return { doc: vi.fn().mockReturnValue({ update: participantUpdate }) };
            }
            if (collectionPath === "emailAddressStatus") {
                return { where: vi.fn().mockReturnValue(queryReturning(relatedSnapshot)) };
            }
            if (collectionPath === "pathologyReports") {
                return {
                    doc: vi.fn().mockReturnValue(pathologyRef),
                    where: vi.fn().mockReturnValue(queryReturning(pathologySnapshot)),
                };
            }
            return { where: vi.fn().mockReturnValue(queryReturning(emptySnapshot)) };
        });

        const result = await audit.auditParticipantDataDestruction({
            doc: participantDoc,
            mode: "cleanup",
            dryRun: true,
            runId: "run-1",
            projectId: "project",
            tier: "PROD",
            checkedAt: "2026-05-14T05:00:00.000Z",
        });

        expect(batchDelete).not.toHaveBeenCalled();
        expect(batchCommit).not.toHaveBeenCalled();
        expect(participantUpdate).not.toHaveBeenCalled();
        expect(fileDelete).not.toHaveBeenCalled();
        expect(result.dryRun).toBe(true);
        const actionTypes = result.cleanupActions.map((action) => action.type);
        expect(actionTypes).toEqual(expect.arrayContaining([
            "deleteRelatedDocs",
            "deletePathologyStorageFiles",
            "deletePathologyMetadata",
            "deleteUnexpectedStubFields",
        ]));
        expect(result.cleanupActions.every((action) => action.dryRun === true)).toBe(true);
    });

    it("skips unexpected-stub cleanup when collection or storage errors are present", async () => {
        const participantDoc = destroyedParticipantDoc({ unexpectedField: "remove" });

        const participantUpdate = vi.fn().mockResolvedValue();
        mocks.firestore.batch.mockReturnValue({ delete: vi.fn(), commit: vi.fn().mockResolvedValue() });

        mocks.firestore.collection.mockImplementation((collectionPath) => {
            if (collectionPath === "participants") {
                return { doc: vi.fn().mockReturnValue({ update: participantUpdate }) };
            }
            if (collectionPath === "emailAddressStatus") {
                return {
                    where: vi.fn().mockReturnValue({
                        select: vi.fn().mockReturnThis(),
                        get: vi.fn().mockRejectedValue(new Error("boom")),
                    }),
                };
            }
            return { where: vi.fn().mockReturnValue(queryReturning(emptySnapshot)) };
        });

        const result = await audit.auditParticipantDataDestruction({
            doc: participantDoc,
            mode: "cleanup",
            dryRun: false,
            runId: "run-1",
            projectId: "project",
            tier: "PROD",
            checkedAt: "2026-05-14T05:00:00.000Z",
        });

        expect(participantUpdate).not.toHaveBeenCalled();
        expect(result.status).toBe("error");
        expect(result.warnings).toEqual(expect.arrayContaining([
            expect.stringMatching(/Skipped unexpected stub cleanup/),
        ]));
    });

    it("generates exact Box artifact filenames", () => {
        expect(audit.buildAuditFileNames("20260514")).toEqual({
            summaryFileName: "20260514_data_destruction_summary.json",
            participantsFileName: "20260514_data_destruction_participants.ndjson",
        });
    });

    describe("Box upload", () => {
        beforeEach(() => {
            process.env.BOX_CLIENT_ID_SECRET = "projects/test/secrets/box-client-id/versions/latest";
            process.env.BOX_CLIENT_SECRET = "projects/test/secrets/box-client-secret/versions/latest";
            process.env.BOX_ENTERPRISE_ID = "enterprise-1";
        });

        it("gets a Box access token with client credentials", async () => {
            const fetchFn = vi.fn().mockResolvedValue(okJson({ access_token: "box-token" }));
            const getSecretFn = vi.fn()
                .mockResolvedValueOnce("client-id")
                .mockResolvedValueOnce("client-secret");

            await expect(audit.getBoxAccessToken({ getSecretFn, fetchFn })).resolves.toBe("box-token");
            expect(fetchFn).toHaveBeenCalledWith("https://api.box.com/oauth2/token", expect.objectContaining({
                method: "POST",
            }));
        });

        it("throws when Box token acquisition fails", async () => {
            const fetchFn = vi.fn().mockResolvedValue({
                ok: false,
                status: 401,
                json: vi.fn().mockResolvedValue({ error: "invalid_client" }),
            });
            const getSecretFn = vi.fn().mockResolvedValue("secret");

            await expect(audit.getBoxAccessToken({ getSecretFn, fetchFn })).rejects.toThrow(/Box token request failed/);
        });

        it("uploads Box audit artifacts with exact filenames", async () => {
            const fetchFn = vi.fn()
                .mockResolvedValueOnce(okJson({ access_token: "box-token" }))
                .mockResolvedValueOnce({
                    ok: true,
                    status: 201,
                    json: vi.fn().mockResolvedValue({ entries: [{ id: "participants-file-id", name: "20260514_data_destruction_participants.ndjson" }] }),
                })
                .mockResolvedValueOnce({
                    ok: true,
                    status: 201,
                    json: vi.fn().mockResolvedValue({ entries: [{ id: "summary-file-id", name: "20260514_data_destruction_summary.json" }] }),
                })
                .mockResolvedValueOnce({
                    ok: true,
                    status: 201,
                    json: vi.fn().mockResolvedValue({ entries: [{ id: "summary-file-id", name: "20260514_data_destruction_summary.json" }] }),
                });

            const summary = { runId: "run-1", boxFiles: null };
            const fileNames = audit.buildAuditFileNames("20260514");
            const uploadResult = await audit.uploadAuditArtifacts({
                summary,
                participantsNdjson: "",
                fileNames,
                settings: { boxFolderID: "folder-1" },
                getSecretFn: vi.fn().mockResolvedValue("secret"),
                fetchFn,
            });

            expect(uploadResult).toEqual({
                summaryFileName: fileNames.summaryFileName,
                participantsFileName: fileNames.participantsFileName,
                summaryFileId: "summary-file-id",
                participantsFileId: "participants-file-id",
            });

            // Last call is the summary version upload that self-references the Box file ID.
            const lastCall = fetchFn.mock.calls[fetchFn.mock.calls.length - 1];
            expect(lastCall[0]).toBe(`https://upload.box.com/api/2.0/files/summary-file-id/content`);
        });

        it("refuses upload when the folder ID is still a TODO placeholder", async () => {
            await expect(audit.uploadAuditArtifacts({
                summary: {},
                participantsNdjson: "",
                fileNames: audit.buildAuditFileNames("20260514"),
                settings: { boxFolderID: "TODO_DEV_BOX_FOLDER_ID" },
                getSecretFn: vi.fn(),
                fetchFn: vi.fn(),
            })).rejects.toThrow(/Box upload is not configured/);
        });

        it("refuses upload when the folder ID is an empty string", async () => {
            await expect(audit.uploadAuditArtifacts({
                summary: {},
                participantsNdjson: "",
                fileNames: audit.buildAuditFileNames("20260514"),
                settings: { boxFolderID: "" },
                getSecretFn: vi.fn(),
                fetchFn: vi.fn(),
            })).rejects.toThrow(/Box upload is not configured/);
        });

        it("refuses upload when the folder ID is missing from settings entirely", async () => {
            await expect(audit.uploadAuditArtifacts({
                summary: {},
                participantsNdjson: "",
                fileNames: audit.buildAuditFileNames("20260514"),
                settings: {},
                getSecretFn: vi.fn(),
                fetchFn: vi.fn(),
            })).rejects.toThrow(/Box upload is not configured/);
        });

        it("falls back to version-upload on a Box 409 conflict", async () => {
            const fetchFn = vi.fn()
                .mockResolvedValueOnce({
                    ok: false,
                    status: 409,
                    json: vi.fn().mockResolvedValue({
                        context_info: { conflicts: { id: "existing-file-id" } },
                    }),
                })
                .mockResolvedValueOnce({
                    ok: true,
                    status: 201,
                    json: vi.fn().mockResolvedValue({ entries: [{ id: "existing-file-id", name: "20260514_data_destruction_summary.json" }] }),
                });

            const result = await audit.uploadBoxFile({
                accessToken: "tok",
                folderId: "folder-1",
                fileName: "20260514_data_destruction_summary.json",
                content: "{}",
                contentType: "application/json",
                fetchFn,
            });

            expect(result).toEqual({
                fileId: "existing-file-id",
                fileName: "20260514_data_destruction_summary.json",
            });
            expect(fetchFn).toHaveBeenCalledTimes(2);
            expect(fetchFn.mock.calls[1][0]).toBe(`https://upload.box.com/api/2.0/files/existing-file-id/content`);
        });

        it("throws when Box upload fails", async () => {
            const fetchFn = vi.fn().mockResolvedValue({
                ok: false,
                status: 500,
                json: vi.fn().mockResolvedValue({ message: "server error" }),
            });

            await expect(audit.uploadBoxFile({
                accessToken: "token",
                folderId: "folder-1",
                fileName: "20260514_data_destruction_summary.json",
                content: "{}",
                contentType: "application/json",
                fetchFn,
            })).rejects.toThrow(/Box upload failed/);
        });
    });

    describe("email delivery", () => {
        it("parses comma-, semicolon-, and whitespace-separated recipients and drops TODO placeholders", () => {
            expect(audit.parseEmailRecipients("a@x.com, b@y.com;c@z.com\nd@w.com")).toEqual([
                "a@x.com", "b@y.com", "c@z.com", "d@w.com",
            ]);
            expect(audit.parseEmailRecipients("TODO_PROD_EMAIL,real@example.com")).toEqual(["real@example.com"]);
            expect(audit.parseEmailRecipients("")).toEqual([]);
            expect(audit.parseEmailRecipients(null)).toEqual([]);
        });

        it("sends summary + NDJSON attachments through the injected SendGrid client", async () => {
            process.env.GCLOUD_SENDGRID_SECRET = "projects/p/secrets/sendgrid/versions/1";

            const sgClient = { setApiKey: vi.fn(), send: vi.fn().mockResolvedValue() };
            const getSecretFn = vi.fn().mockResolvedValue("sg-key");

            const summary = {
                runId: "r-1",
                tier: "PROD",
                mode: "audit",
                dryRun: false,
                startedAt: "2026-05-14T05:00:00.000Z",
                participantCounts: { total: 2, status: { pass: 1, warn: 0, fail: 1, error: 0 } },
            };
            const fileNames = audit.buildAuditFileNames("20260514");

            const result = await audit.emailAuditArtifacts({
                summary,
                participantsNdjson: '{"x":1}\n',
                fileNames,
                settings: { emailRecipients: ["team@example.com", "lead@example.com"] },
                getSecretFn,
                sgClient,
            });

            expect(sgClient.setApiKey).toHaveBeenCalledWith("sg-key");
            expect(sgClient.send).toHaveBeenCalledOnce();
            const msg = sgClient.send.mock.calls[0][0];
            expect(msg.to).toEqual(["team@example.com", "lead@example.com"]);
            expect(msg.subject).toContain("PROD");
            expect(msg.attachments).toHaveLength(2);
            expect(msg.attachments.map((a) => a.filename)).toEqual([
                fileNames.summaryFileName,
                fileNames.participantsFileName,
            ]);
            const ndjsonAttachment = msg.attachments.find((a) => a.filename === fileNames.participantsFileName);
            expect(Buffer.from(ndjsonAttachment.content, "base64").toString()).toBe('{"x":1}\n');
            expect(result.recipients).toEqual(["team@example.com", "lead@example.com"]);
        });

        it("throws when settings has no recipients configured", async () => {
            await expect(audit.emailAuditArtifacts({
                summary: {},
                participantsNdjson: "",
                fileNames: audit.buildAuditFileNames("20260514"),
                settings: {},
                getSecretFn: vi.fn(),
                sgClient: { setApiKey: vi.fn(), send: vi.fn() },
            })).rejects.toThrow(/Email delivery is not configured/);
        });

        it("throws when GCLOUD_SENDGRID_SECRET is unset", async () => {
            await expect(audit.emailAuditArtifacts({
                summary: {},
                participantsNdjson: "",
                fileNames: audit.buildAuditFileNames("20260514"),
                settings: { emailRecipients: ["a@b.com"] },
                getSecretFn: vi.fn(),
                sgClient: { setApiKey: vi.fn(), send: vi.fn() },
            })).rejects.toThrow(/GCLOUD_SENDGRID_SECRET/);
        });

        it("prefixes subject with DRY RUN when dryRun is set", async () => {
            process.env.GCLOUD_SENDGRID_SECRET = "x";
            const sgClient = { setApiKey: vi.fn(), send: vi.fn().mockResolvedValue() };
            await audit.emailAuditArtifacts({
                summary: {
                    runId: "r", tier: "DEV", mode: "cleanup", dryRun: true,
                    startedAt: "t", participantCounts: { total: 0, status: { pass: 0, warn: 0, fail: 0, error: 0 } },
                },
                participantsNdjson: "",
                fileNames: audit.buildAuditFileNames("20260514"),
                settings: { emailRecipients: ["a@b.com"] },
                getSecretFn: vi.fn().mockResolvedValue("k"),
                sgClient,
            });
            expect(sgClient.send.mock.calls[0][0].subject.startsWith("DRY RUN ")).toBe(true);
        });

        describe("settings helpers", () => {
            it("getDataDestructionAuditSettings reads the dataDestructionAudit sub-object once", async () => {
                const getAppSettingsFn = stubAppSettings({
                    emailRecipients: ["a@b.com"],
                    boxFolderID: "folder-1",
                });
                const result = await audit.getDataDestructionAuditSettings(getAppSettingsFn);
                expect(result).toEqual({ emailRecipients: ["a@b.com"], boxFolderID: "folder-1" });
                expect(getAppSettingsFn).toHaveBeenCalledOnce();
                expect(getAppSettingsFn).toHaveBeenCalledWith("connectFaas", ["dataDestructionAudit"]);
            });

            it("getDataDestructionAuditSettings returns {} when the sub-object is missing", async () => {
                expect(await audit.getDataDestructionAuditSettings(stubAppSettings(undefined))).toEqual({});
            });

            it("extractEmailRecipientsFromSettings handles array form", () => {
                expect(audit.extractEmailRecipientsFromSettings({
                    emailRecipients: ["a@b.com", "c@d.com"],
                })).toEqual(["a@b.com", "c@d.com"]);
            });

            it("extractEmailRecipientsFromSettings falls back to parsing a string", () => {
                expect(audit.extractEmailRecipientsFromSettings({
                    emailRecipients: "a@b.com, c@d.com; e@f.com",
                })).toEqual(["a@b.com", "c@d.com", "e@f.com"]);
            });

            it("extractEmailRecipientsFromSettings filters TODO placeholders and malformed entries", () => {
                expect(audit.extractEmailRecipientsFromSettings({
                    emailRecipients: ["TODO_FILL", "good@example.com", "not-an-email", "another@example.org"],
                })).toEqual(["good@example.com", "another@example.org"]);
            });

            it("extractEmailRecipientsFromSettings returns [] when the field is missing or wrong type", () => {
                expect(audit.extractEmailRecipientsFromSettings({})).toEqual([]);
                expect(audit.extractEmailRecipientsFromSettings({ emailRecipients: 42 })).toEqual([]);
                expect(audit.extractEmailRecipientsFromSettings()).toEqual([]);
            });

            it("extractBoxFolderIdFromSettings returns the folder ID when valid", () => {
                expect(audit.extractBoxFolderIdFromSettings({ boxFolderID: "folder-1" })).toBe("folder-1");
            });

            it("extractBoxFolderIdFromSettings returns null for empty string, TODO placeholder, or missing", () => {
                expect(audit.extractBoxFolderIdFromSettings({ boxFolderID: "" })).toBeNull();
                expect(audit.extractBoxFolderIdFromSettings({ boxFolderID: "TODO_FILL_THIS_IN" })).toBeNull();
                expect(audit.extractBoxFolderIdFromSettings({})).toBeNull();
                expect(audit.extractBoxFolderIdFromSettings()).toBeNull();
            });
        });
    });

    describe("runDataDestructionAudit", () => {
        const noParticipantsCollection = () => ({
            where: vi.fn().mockReturnThis(),
            get: vi.fn().mockResolvedValue(emptySnapshot),
        });

        it("records Box upload failure as a non-fatal warning on the summary", async () => {
            mocks.firestore.collection.mockImplementation((path) => {
                if (path === "participants") return noParticipantsCollection();
                return { where: vi.fn().mockReturnValue(queryReturning(emptySnapshot)) };
            });

            const uploadAuditArtifacts = vi.fn().mockRejectedValue(new Error("Box is down"));
            const emailAuditArtifacts = vi.fn().mockResolvedValue({
                recipients: ["a@b.com"],
                attachments: ["s.json", "p.ndjson"],
            });

            const result = await audit.runDataDestructionAudit({}, {
                uploadAuditArtifacts,
                emailAuditArtifacts,
                now: () => new Date("2026-05-14T05:00:00.000Z"),
            });

            expect(uploadAuditArtifacts).toHaveBeenCalledOnce();
            expect(emailAuditArtifacts).toHaveBeenCalledOnce();
            expect(result.summary.boxFiles).toBeNull();
            expect(result.summary.boxUploadError).toBe("Box is down");
            expect(result.summary.emailDelivery.error).toBeNull();
        });

        it("records email failure as a non-fatal warning on the summary", async () => {
            mocks.firestore.collection.mockImplementation((path) => {
                if (path === "participants") return noParticipantsCollection();
                return { where: vi.fn().mockReturnValue(queryReturning(emptySnapshot)) };
            });

            const uploadAuditArtifacts = vi.fn().mockResolvedValue({
                summaryFileName: "s.json",
                participantsFileName: "p.ndjson",
                summaryFileId: "s",
                participantsFileId: "p",
            });
            const emailAuditArtifacts = vi.fn().mockRejectedValue(new Error("no recipients"));

            const result = await audit.runDataDestructionAudit({}, {
                uploadAuditArtifacts,
                emailAuditArtifacts,
                now: () => new Date("2026-05-14T05:00:00.000Z"),
            });

            expect(result.summary.boxFiles).toEqual({
                summary: { fileName: "s.json", fileId: "s" },
                participants: { fileName: "p.ndjson", fileId: "p" },
            });
            expect(result.summary.emailDelivery.error).toBe("no recipients");
        });

        it("both Box and email failures still produce a 200 result", async () => {
            mocks.firestore.collection.mockImplementation((path) => {
                if (path === "participants") return noParticipantsCollection();
                return { where: vi.fn().mockReturnValue(queryReturning(emptySnapshot)) };
            });

            const result = await audit.runDataDestructionAudit({}, {
                uploadAuditArtifacts: vi.fn().mockRejectedValue(new Error("box")),
                emailAuditArtifacts: vi.fn().mockRejectedValue(new Error("email")),
                now: () => new Date("2026-05-14T05:00:00.000Z"),
            });

            expect(result.summary.boxUploadError).toBe("box");
            expect(result.summary.emailDelivery.error).toBe("email");
            expect(result.summary.participantCounts.total).toBe(0);
        });

        it("summary aggregates policy versions applied as a histogram", async () => {
            const participantDoc = destroyedParticipantDoc();
            const participantsSnapshot = {
                empty: false,
                size: 1,
                docs: [participantDoc],
            };
            mocks.firestore.collection.mockImplementation((path) => {
                if (path === "participants") {
                    return {
                        where: vi.fn().mockReturnThis(),
                        get: vi.fn().mockResolvedValue(participantsSnapshot),
                    };
                }
                return { where: vi.fn().mockReturnValue(queryReturning(emptySnapshot)) };
            });

            const result = await audit.runDataDestructionAudit({}, {
                uploadAuditArtifacts: vi.fn().mockResolvedValue({
                    summaryFileName: "s", participantsFileName: "p", summaryFileId: "s", participantsFileId: "p",
                }),
                emailAuditArtifacts: vi.fn().mockResolvedValue({ recipients: [], attachments: [] }),
                now: () => new Date("2026-05-14T05:00:00.000Z"),
            });

            expect(result.summary.policyVersionsApplied).toEqual({ v0: 1 });
        });

        it("audit-mode runId encodes the mode without the dry-run suffix", async () => {
            mocks.firestore.collection.mockImplementation((path) => {
                if (path === "participants") return noParticipantsCollection();
                return { where: vi.fn().mockReturnValue(queryReturning(emptySnapshot)) };
            });

            const result = await audit.runDataDestructionAudit({}, {
                uploadAuditArtifacts: vi.fn().mockResolvedValue({
                    summaryFileName: "s", participantsFileName: "p", summaryFileId: "s", participantsFileId: "p",
                }),
                emailAuditArtifacts: vi.fn().mockResolvedValue({ recipients: [], attachments: [] }),
                now: () => new Date("2026-05-14T05:00:00.000Z"),
            });

            expect(result.summary.runId).toMatch(/^\d{8}-audit-/);
            expect(result.summary.runId).not.toContain("dryrun");
        });

        it("dry-run cleanup runId encodes the dryrun suffix", async () => {
            mocks.firestore.collection.mockImplementation((path) => {
                if (path === "participants") return noParticipantsCollection();
                return { where: vi.fn().mockReturnValue(queryReturning(emptySnapshot)) };
            });

            const result = await audit.runDataDestructionAudit({
                mode: "cleanup",
                dryRun: true,
            }, {
                uploadAuditArtifacts: vi.fn().mockResolvedValue({
                    summaryFileName: "s", participantsFileName: "p", summaryFileId: "s", participantsFileId: "p",
                }),
                emailAuditArtifacts: vi.fn().mockResolvedValue({ recipients: [], attachments: [] }),
                now: () => new Date("2026-05-14T05:00:00.000Z"),
            });

            expect(result.summary.runId).toMatch(/^\d{8}-cleanup-dryrun-/);
            expect(result.summary.dryRun).toBe(true);
        });
    });
});
