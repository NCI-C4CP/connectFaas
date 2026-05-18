const fieldMapping = require("../../utils/fieldToConceptIdMapping");
const { defaultFlags } = require("../../utils/shared");
const {
    V0_DATA_DESTRUCTION_BASELINE,
    V0_DATA_DESTRUCTION_STUB_VARS,
    DATA_DESTRUCTION_POLICY_DELTAS,
    NESTED_PARENT_CID_MAP,
    resolvePolicyForDestruction,
    getCurrentPolicy,
    describeStubVariables,
    applyDelta,
    buildDataDestructionUpdate,
    validateDestroyedStub,
} = require("../../utils/dataDestructionPolicy");

const deleteSentinel = "__DELETE__";

const syntheticV1 = {
    version: "v1-test",
    effectiveFrom: {
        DEV:   "2026-05-15T00:00:00.000Z",
        STAGE: "2026-06-01T00:00:00.000Z",
        PROD:  null,
    },
    retainedFieldsNamed: {
        add:    { dateRevokedHIPAA: 664453818 },
        remove: { firebaseAuthenticationEmail: 421823980 },
    },
    nestedRetainedFieldsNamed: { add: {}, remove: {} },
    requiredAfterDestructionNamed: { add: {}, remove: {} },
    rationale: "test",
};

describe("dataDestructionPolicy — V0 baseline (named)", () => {
    it("retains historical pre-V1 named fields including firebaseAuthenticationEmail", () => {
        expect(V0_DATA_DESTRUCTION_BASELINE.retainedFieldsNamed.firebaseAuthenticationEmail).toBe(421823980);
        expect(V0_DATA_DESTRUCTION_BASELINE.retainedFieldsNamed.dateRevokedHIPAA).toBeUndefined();
        expect(V0_DATA_DESTRUCTION_BASELINE.retainedFieldsNamed.Connect_ID).toBe("Connect_ID");
        expect(V0_DATA_DESTRUCTION_BASELINE.retainedFieldsNamed.token).toBe("token");
    });

    it("requires Connect_ID, token, and the three destruction stamps by name", () => {
        expect(V0_DATA_DESTRUCTION_BASELINE.requiredAfterDestructionNamed).toEqual({
            Connect_ID: "Connect_ID",
            token: "token",
            dataHasBeenDestroyed: 861639549,
            dateTimeDataDestroyed: 652627623,
            participationStatus: 912301837,
        });
    });

    it("uses the same nested allowlist for query / state / physicalActivity", () => {
        const sub = V0_DATA_DESTRUCTION_BASELINE.nestedRetainedFieldsNamed.query;
        expect(V0_DATA_DESTRUCTION_BASELINE.nestedRetainedFieldsNamed.state).toEqual(sub);
        expect(V0_DATA_DESTRUCTION_BASELINE.nestedRetainedFieldsNamed.physicalActivity).toEqual(sub);
        expect(sub).toEqual({
            firstName: "firstName",
            lastName: "lastName",
            studyId: "studyId",
            uid: "uid",
            flagForReportUnreadViewedDeclined: 446235715,
            dateRoiPaReportFirstViewed: 749055145,
        });
    });

    it("maps physicalActivity parent name to its CID", () => {
        expect(NESTED_PARENT_CID_MAP.physicalActivity).toBe(686238347);
    });
});

// V0 Data Destruction lock-in.

describe("dataDestructionPolicy — V0 immutable snapshot lock-in", () => {
    it("V0_DATA_DESTRUCTION_STUB_VARS has the expected entry count and CID sum", () => {
        expect(Object.keys(V0_DATA_DESTRUCTION_STUB_VARS)).toHaveLength(136);
        const cidSum = Object.values(V0_DATA_DESTRUCTION_STUB_VARS)
            .reduce((acc, cid) => acc + cid, 0);
        expect(cidSum).toBe(72519393025);
    });

    it("V0_DATA_DESTRUCTION_STUB_VARS matches the locked-in name -> CID snapshot exactly", () => {
        expect(V0_DATA_DESTRUCTION_STUB_VARS).toEqual({
            consentFirstName: 471168198,
            consentMiddleName: 436680969,
            consentLastName: 736251808,
            consentSuffixName: 480305327,
            userProfileNameFirstName: 399159511,
            userProfileNameMiddleName: 231676651,
            userProfileNameLastName: 996038075,
            userProfileNameSuffixName: 506826178,
            dateOfBirth: 371067537,
            birthMonth: 564964481,
            birthDay: 795827569,
            birthYear: 544150384,
            healthcareProvider: 827220437,
            informedConsentFlag: 919254129,
            informedConsentVersion: 454205108,
            informedConsentDateSigned: 454445267,
            verificationStatus: 821247024,
            verificationDate: 914594314,
            hipaaAuthorizationFlag: 558435199,
            hipaaAuthorizationVersion: 412000022,
            hipaaAuthorizationDateSigned: 262613359,
            userProfileSubmittedFlag: 699625233,
            hipaaRevocationFlag: 773707518,
            withdrawalFlag: 747006172,
            dateOfWithdrawal: 659990606,
            whoRequestedWithdrawal: 299274441,
            whoRequestedWithdrawalOther: 457532784,
            reasonsForWithdrawal: 919699172,
            tooBusy: 141450621,
            incentiveTooLow: 576083042,
            tooSick: 431428747,
            unreliableInternetDevice: 121430614,
            worriedAboutStudyResults: 523768810,
            worriedStudyWillFindSomethingBad: 639172801,
            privacyConcerns: 175732191,
            doNotTrustGov: 150818546,
            doNotTrustRsrchrs: 624030581,
            doNotWantInfoSharedWithRsrchrs: 285488731,
            worriedInfoNotSecure: 596510649,
            worriedInsWillGetData: 866089092,
            worriedAboutDataBeingGivenToMyEmployerPotentialEmployer: 990579614,
            worriedAboutDiscriminationFromData: 131458944,
            worriedOthersWillProfitFromMyData: 372303208,
            otherPrivacyConcerns: 777719027,
            unableToCompleteOnlineActivities: 620696506,
            concernedAboutInfoOnline: 637147033,
            technicalProblems: 440351122,
            doNotLikeThingsOnline: 352891568,
            covidConcerns: 958588520,
            partNowUnableToParticipate: 875010152,
            partIncarcerated: 404289911,
            reasonNotGiven: 538619788,
            otherReasonsSpecify: 715390138,
            otherReasons: 734828170,
            hipaaRevocationFormHasBeenSigned: 153713899,
            hipaaRevocationCategorical: 577794331,
            dateOfSignatureOnHIPAARevocationForm: 613641698,
            versionOfTheHIPAARevocationForm: 407743866,
            nameExtractedFromSignedHIPAARevocationFormFirstName: 765336427,
            nameExtractedFromSignedHIPAARevocationFormMiddleName: 826240317,
            nameExtractedFromSignedHIPAARevocationFormLastName: 479278368,
            nameExtractedFromSignedHIPAARevocationFormSuffix: 693626233,
            dataDestructionRequest: 831041022,
            dateOfDestructionRequest: 269050420,
            whoRequestedDataDestruction: 524352591,
            whoRequestedDataDestructionOther: 902332801,
            dataDestructionRequestFormHasBeenSigned: 359404406,
            dataDestructionCategoricalFlag: 883668444,
            dateOfSignatureOnDataDestructionRequestForm: 119449326,
            versionOfTheDataDestructionRequestForm: 304438543,
            nameExtractedFromSignedDataDestructionFormFirstName: 104278817,
            nameExtractedFromSignedDataDestructionFormMiddleName: 268665918,
            nameExtractedFromSignedDataDestructionFormLastName: 744604255,
            nameExtractedFromSignedDataDestructionFormSuffixName: 592227431,
            dataHasBeenDestroyedFlag: 861639549,
            dateTimeDataDestroyed: 652627623,
            participationStatus: 912301837,
            incentive: 130371375,
            incentiveEligible: 731498909,
            menstrualSurveyEligible: 289750687,
            dateIncentiveEligible: 787567527,
            norcIncentiveEligible: 222373868,
            incentiveIssued: 648936790,
            dateIncentiveIssued: 297462035,
            incentiveRefused: 648228701,
            dateIncentiveRefused: 438636757,
            norcCaseNumber: 320023644,
            bloodUrineMouthwashCombinedResearchSurveyFlag: 265193023,
            autogeneratedDateTimeWhenBloodUrineMouthwashResearchSurveyCompleted: 222161762,
            bloodUrineSurveyCompletionFlag: 253883960,
            autogeneratedDateTimeWhenBloodUrineSurveyCompleted: 764863765,
            mouthwashSurveyCompletionFlag: 547363263,
            autogeneratedDateTimeWhenMouthwashSurveyCompleted: 195145666,
            menstrualCycleSurveyCompletionFlag: 459098666,
            autogeneratedDateTimeWhenMenstrualCycleSurveyCompleted: 217640691,
            covid19SurveyCompletionFlag: 220186468,
            autogeneratedDateTimeWhenCOVID19SurveyCompleted: 784810139,
            surveyCompletionFlag: 320303124,
            autogeneratedDateTimeWhenPROMISSurveyCompleted: 843688458,
            baselineSurveyStatusModuleBackgroundAndOverallHealthFlag: 949302066,
            autogeneratedDateTimeStampForCompletionOfModuleBackgroundAndOverallHealth: 517311251,
            baselineSurveyStatusModuleMedications: 536735468,
            autogeneratedDateTimeStampForCompletionOfModuleMedications: 832139544,
            baselineSurveyStatusModuleSmoking: 976570371,
            autogeneratedDateTimeStampForCompletionOfModuleSmoking: 770257102,
            baselineSurveyStatusModuleWhereYouLiveAndWorkFlag: 663265240,
            autogeneratedDateTimeStampForCompletionOfModuleWhereYouLiveAndWork: 264644252,
            snsnSurveyFlag: 126331570,
            autogeneratedDateTimeWhenSSNSurveyCompleted: 315032037,
            allBiospecimenCollectionDetails: 173836415,
            baselineUrineCollected: 167958071,
            baselineBloodAndUrineIsRefused: 526455436,
            baselineMouthwashCollected: 684635302,
            baselineBloodSampleCollected: 878865966,
            allBaselineSamplesCollected: 254109640,
            bioSpmVisitV1r0: 331584571,
            allBaselineSurveysCompleted: 100767870,
            firebaseAuthenticationEmail: 421823980,
            threeMonthQualityOfLifePromisSurveyCompletion: 320303124,
            autogeneratedDateTimeWhenPromisSurveyCompleted: 843688458,
            connectExperienceSurveyStatus: 956490759,
            autogeneratedDateimeStampForCompletionOf2024ConnectExperienceSurvey: 199471989,
            anyRefusalOrWithdrawal: 451953807,
            cancerScreeningHistorySurveyStatus: 176068627,
            cancerScreeningHistorySurveyCompletionTime: 389890053,
            dhq3SurveyStatus: 692560814,
            dhq3SurveyCompletionTime: 610227793,
            dhq3HEIReportStatusInternal: 542983589,
            dhq3HEIReportStatusExternal: 892697201,
            dhq3HEIReportFirstViewedISOTime: 600958089,
            numberOfAvailableReports: 794047378,
            physicalActivity: 686238347,
            flagForReportUnreadViewedDeclined: 446235715,
            dateRoiPaReportFirstViewed: 749055145,
            preference2026SurveyStatus: 278023676,
            preference2026SurveyCompletionTime: 543379310,
        });
    });
});


describe("dataDestructionPolicy — V1 delta (named)", () => {
    it("adds dateRevokedHIPAA and removes firebaseAuthenticationEmail by name", () => {
        const v1 = DATA_DESTRUCTION_POLICY_DELTAS.find((d) => d.version === "v1");
        expect(v1).toBeDefined();
        expect(v1.retainedFieldsNamed.add).toEqual({ dateRevokedHIPAA: 664453818 });
        expect(v1.retainedFieldsNamed.remove).toEqual({ firebaseAuthenticationEmail: 421823980 });
        expect(v1.nestedRetainedFieldsNamed).toEqual({ add: {}, remove: {} });
        expect(v1.requiredAfterDestructionNamed).toEqual({ add: {}, remove: {} });
        expect(v1.rationale).toMatch(/dateRevokedHIPAA/);
    });

    it("has per-tier effective dates pinned to the May 2026 rollout", () => {
        const v1 = DATA_DESTRUCTION_POLICY_DELTAS.find((d) => d.version === "v1");
        expect(v1.effectiveFrom).toEqual({
            DEV:   "2026-05-18T04:00:00.000Z",
            STAGE: "2026-05-25T04:00:00.000Z",
            PROD:  "2026-05-28T02:00:00.000Z",
        });
    });
});

describe("dataDestructionPolicy — resolver", () => {
    const versions = [syntheticV1];

    it("returns V0 when destructionIso is missing", () => {
        const view = resolvePolicyForDestruction(null, "DEV", versions);
        expect(view.version).toBe("v0");
        expect(view.appliedDeltas).toEqual([]);
        expect(view.retainedFieldsNamed.firebaseAuthenticationEmail).toBe(421823980);
        expect(view.retainedFieldsNamed.dateRevokedHIPAA).toBeUndefined();
        expect(view.retainedTopLevelFields).toContain("421823980");
        expect(view.retainedTopLevelFields).not.toContain("664453818");
    });

    it("returns V0 when destructionIso is unparseable", () => {
        const view = resolvePolicyForDestruction("not-a-date", "DEV", versions);
        expect(view.version).toBe("v0");
    });

    it("returns V0 when destructionIso pre-dates V1 effective on this tier", () => {
        const view = resolvePolicyForDestruction("2026-04-01T00:00:00.000Z", "DEV", versions);
        expect(view.version).toBe("v0");
        expect(view.retainedTopLevelFields).toContain("421823980");
    });

    it("applies V1 when destructionIso is after V1 effective on this tier", () => {
        const view = resolvePolicyForDestruction("2026-06-01T00:00:00.000Z", "DEV", versions);
        expect(view.version).toBe("v1-test");
        expect(view.appliedDeltas).toEqual(["v1-test"]);
        expect(view.effectiveFrom).toBe("2026-05-15T00:00:00.000Z");
        expect(view.retainedFieldsNamed.dateRevokedHIPAA).toBe(664453818);
        expect(view.retainedFieldsNamed.firebaseAuthenticationEmail).toBeUndefined();
        expect(view.retainedTopLevelFields).toContain("664453818");
        expect(view.retainedTopLevelFields).not.toContain("421823980");
    });

    it("skips V1 on a tier where effectiveFrom is null even if destructionIso is recent", () => {
        const view = resolvePolicyForDestruction("2027-01-01T00:00:00.000Z", "PROD", versions);
        expect(view.version).toBe("v0");
        expect(view.appliedDeltas).toEqual([]);
    });

    it("evaluates each tier independently", () => {
        const stageEarly = resolvePolicyForDestruction("2026-05-20T00:00:00.000Z", "STAGE", versions);
        const stageLate  = resolvePolicyForDestruction("2026-06-15T00:00:00.000Z", "STAGE", versions);
        expect(stageEarly.version).toBe("v0");
        expect(stageLate.version).toBe("v1-test");
    });

    it("materializes nestedRetainedFields under the CID-form parent key", () => {
        const view = resolvePolicyForDestruction(null, "DEV", []);
        // physicalActivity parent renders as its CID 686238347 in the doc-shape map
        expect(view.nestedRetainedFields["686238347"]).toEqual(expect.arrayContaining([
            "firstName", "lastName", "studyId", "uid", "446235715", "749055145",
        ]));
        // builtin parents stay as literal names
        expect(view.nestedRetainedFields.query).toEqual(expect.arrayContaining(["firstName"]));
    });

    it("recomputes defaultFieldsRetainedIfPresent against the resolved retained set", () => {
        const v0View = resolvePolicyForDestruction(null, "DEV", versions);
        const v1View = resolvePolicyForDestruction("2026-06-01T00:00:00.000Z", "DEV", versions);
        v0View.defaultFieldsRetainedIfPresent.forEach((f) => {
            expect(v0View.retainedTopLevelFields).toContain(f);
            expect(Object.keys(defaultFlags)).toContain(f);
        });
        v1View.defaultFieldsRetainedIfPresent.forEach((f) => {
            expect(v1View.retainedTopLevelFields).toContain(f);
        });
    });

    it("post-V1 retained set is V0 + V1 delta exactly", () => {
        const v0 = resolvePolicyForDestruction(null, "DEV", versions);
        const v1 = resolvePolicyForDestruction("2026-06-01T00:00:00.000Z", "DEV", versions);
        const v0Set = new Set(v0.retainedTopLevelFields);
        const v1Set = new Set(v1.retainedTopLevelFields);
        for (const cid of v0Set) {
            if (cid !== "421823980") expect(v1Set.has(cid)).toBe(true);
        }
        expect(v1Set.has("664453818")).toBe(true);
        expect(v1Set.has("421823980")).toBe(false);
    });
});

describe("dataDestructionPolicy — applyDelta (named)", () => {
    it("removes then adds by name at the top level", () => {
        const state = {
            retainedFieldsNamed: { a: 1, b: 2, c: 3 },
            nestedRetainedFieldsNamed: { query: { x: "x" } },
            requiredAfterDestructionNamed: { Connect_ID: "Connect_ID" },
        };
        applyDelta(state, {
            retainedFieldsNamed: { add: { d: 4 }, remove: { b: 2 } },
            nestedRetainedFieldsNamed: { add: { query: { y: "y" } }, remove: { query: { x: "x" } } },
            requiredAfterDestructionNamed: { add: { newReq: 999 }, remove: { Connect_ID: "Connect_ID" } },
        });
        expect(state.retainedFieldsNamed).toEqual({ a: 1, c: 3, d: 4 });
        expect(state.nestedRetainedFieldsNamed.query).toEqual({ y: "y" });
        expect(state.requiredAfterDestructionNamed).toEqual({ newReq: 999 });
    });

    it("tolerates missing delta sections", () => {
        const state = {
            retainedFieldsNamed: { a: 1 },
            nestedRetainedFieldsNamed: {},
            requiredAfterDestructionNamed: {},
        };
        expect(() => applyDelta(state, {})).not.toThrow();
        expect(state.retainedFieldsNamed).toEqual({ a: 1 });
    });
});

describe("dataDestructionPolicy — describeStubVariables", () => {
    it("returns a name-keyed view of the resolved policy", () => {
        const description = describeStubVariables(resolvePolicyForDestruction(null, "DEV", []));
        expect(description.version).toBe("v0");
        expect(description.effectiveFrom).toBeNull();
        expect(description.retainedTopLevel.firebaseAuthenticationEmail).toBe(421823980);
        expect(description.retainedNested.query.firstName).toBe("firstName");
        expect(description.requiredAfterDestruction.dataHasBeenDestroyed).toBe(861639549);
    });

    it("defaults to the current policy when no argument is passed", () => {
        const description = describeStubVariables();
        // After V1's tier effectiveFrom is in the past, the current policy is V1.
        // The exact version this returns depends on developmentTier and clock,
        // so we just assert it's one of the known versions and has a name→CID map.
        expect(["v0", "v1"]).toContain(description.version);
        expect(typeof description.retainedTopLevel).toBe("object");
    });
});

describe("dataDestructionPolicy — getCurrentPolicy", () => {
    it("returns a known policy version for the active tier", () => {
        const view = getCurrentPolicy();
        expect(["v0", "v1"]).toContain(view.version);
        // Whichever version is current, the retained CIDs should be a non-empty array.
        expect(Array.isArray(view.retainedTopLevelFields)).toBe(true);
        expect(view.retainedTopLevelFields.length).toBeGreaterThan(0);
    });
});

describe("dataDestructionPolicy — buildDataDestructionUpdate", () => {
    const v0 = resolvePolicyForDestruction(null, "DEV", []);

    it("deletes non-retained top-level and nested fields against V0", () => {
        const physicalActivity = "686238347";
        const update = buildDataDestructionUpdate({
            Connect_ID: 123,
            token: "token-1",
            pin: "123456",
            query: { firstName: "Jane", extraQuery: "remove" },
            state: { uid: "uid-1", extraState: "remove" },
            [physicalActivity]: { "446235715": 1, extraActivity: "remove" },
            "123456789": "remove",
        }, v0, { deleteFieldValue: () => deleteSentinel });

        expect(update.updateData["123456789"]).toBe(deleteSentinel);
        expect(update.updateData["query.extraQuery"]).toBe(deleteSentinel);
        expect(update.updateData["state.extraState"]).toBe(deleteSentinel);
        expect(update.updateData[`${physicalActivity}.extraActivity`]).toBe(deleteSentinel);
        expect(update.updateData).not.toHaveProperty("Connect_ID");
        expect(update.updateData).not.toHaveProperty("token");
    });

    it("throws when deleteFieldValue option is missing", () => {
        expect(() => buildDataDestructionUpdate({ Connect_ID: 1 }, v0, {}))
            .toThrow(/deleteFieldValue/);
    });

    it("throws when participantBefore is not an object", () => {
        expect(() => buildDataDestructionUpdate(null, v0, { deleteFieldValue: () => deleteSentinel }))
            .toThrow(/participantBefore/);
    });

    it("throws when policy is missing retainedTopLevelFields", () => {
        expect(() => buildDataDestructionUpdate({ Connect_ID: 1 }, {}, { deleteFieldValue: () => deleteSentinel }))
            .toThrow(/retainedTopLevelFields/);
    });
});

describe("dataDestructionPolicy — validateDestroyedStub", () => {
    const v0 = resolvePolicyForDestruction(null, "DEV", []);
    const v1 = resolvePolicyForDestruction("2026-06-01T00:00:00.000Z", "DEV", [syntheticV1]);

    it("passes a clean V0 stub", () => {
        const validation = validateDestroyedStub({
            Connect_ID: 123,
            token: "token-1",
            [fieldMapping.participantMap.dataHasBeenDestroyed.toString()]: fieldMapping.yes,
            [fieldMapping.participantMap.dateTimeDataDestroyed.toString()]: "2026-05-14T04:00:00.000Z",
            [fieldMapping.participationStatus]: fieldMapping.participantMap.dataDestroyedStatus,
            "421823980": "auth-email-retained-under-v0",
        }, v0);

        expect(validation.status).toBe("pass");
        expect(validation.policyVersion).toBe("v0");
    });

    it("flags 421823980 as unexpected once V1 is the resolved policy", () => {
        const validation = validateDestroyedStub({
            Connect_ID: 123,
            token: "token-1",
            [fieldMapping.participantMap.dataHasBeenDestroyed.toString()]: fieldMapping.yes,
            [fieldMapping.participantMap.dateTimeDataDestroyed.toString()]: "2026-06-01T00:00:00.000Z",
            [fieldMapping.participationStatus]: fieldMapping.participantMap.dataDestroyedStatus,
            "421823980": "auth-email-should-not-survive-under-v1",
        }, v1);

        expect(validation.status).toBe("fail");
        expect(validation.unexpectedStubFields).toContain("421823980");
        expect(validation.policyVersion).toBe("v1-test");
    });

    it("flags unexpected top-level and nested fields", () => {
        const validation = validateDestroyedStub({
            Connect_ID: 123,
            token: "token-1",
            query: { firstName: "Jane", extraQuery: "remove" },
            [fieldMapping.participantMap.dataHasBeenDestroyed.toString()]: fieldMapping.yes,
            [fieldMapping.participantMap.dateTimeDataDestroyed.toString()]: "2026-05-14T04:00:00.000Z",
            [fieldMapping.participationStatus]: fieldMapping.participantMap.dataDestroyedStatus,
            unexpected: true,
        }, v0);

        expect(validation.status).toBe("fail");
        expect(validation.unexpectedStubFields).toContain("unexpected");
        expect(validation.unexpectedNestedFields).toContain("query.extraQuery");
    });

    it("flags missing required fields", () => {
        const validation = validateDestroyedStub({
            Connect_ID: 123,
            token: "token-1",
        }, v0);

        expect(validation.status).toBe("fail");
        expect(validation.missingRequiredStubFields).toEqual(expect.arrayContaining([
            "861639549", "652627623", "912301837",
        ]));
    });

    it("warns when defaults that were present before are missing after", () => {
        const presentDefault = v0.defaultFieldsRetainedIfPresent[0];
        expect(presentDefault).toBeDefined();

        const validation = validateDestroyedStub(
            {
                Connect_ID: 123,
                token: "token-1",
                [fieldMapping.participantMap.dataHasBeenDestroyed.toString()]: fieldMapping.yes,
                [fieldMapping.participantMap.dateTimeDataDestroyed.toString()]: "2026-05-14T04:00:00.000Z",
                [fieldMapping.participationStatus]: fieldMapping.participantMap.dataDestroyedStatus,
            },
            v0,
            { participantBefore: { [presentDefault]: 1 } },
        );

        expect(validation.status).toBe("warn");
        expect(validation.missingDefaultRetainedFields).toContain(presentDefault);
    });

    it("does not check defaults when no participantBefore is supplied", () => {
        const validation = validateDestroyedStub(
            {
                Connect_ID: 123,
                token: "token-1",
                [fieldMapping.participantMap.dataHasBeenDestroyed.toString()]: fieldMapping.yes,
                [fieldMapping.participantMap.dateTimeDataDestroyed.toString()]: "2026-05-14T04:00:00.000Z",
                [fieldMapping.participationStatus]: fieldMapping.participantMap.dataDestroyedStatus,
            },
            v0,
        );
        expect(validation.status).toBe("pass");
        expect(validation.missingDefaultRetainedFields).toEqual([]);
    });
});
