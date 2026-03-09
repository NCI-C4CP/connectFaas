const uuid = require('uuid');
const { setupTestSuite } = require('../shared/testHelpers');
const fieldToConceptIdMapping = require('../../utils/fieldToConceptIdMapping');

let validation;
let firestore;

beforeAll(() => {
    setupTestSuite({
        setupConsole: false,
        setupModuleMocks: true,
    });

    firestore = require('../../utils/firestore');
    validation = require('../../utils/validation');
});

afterEach(() => {
    vi.restoreAllMocks();
});

describe('Validation Derived Variable Helpers', () => {
    describe('processMouthwashEligibility', () => {
        const buildEligibleData = () => ({
            [fieldToConceptIdMapping.withdrawConsent]: fieldToConceptIdMapping.no,
            [fieldToConceptIdMapping.participantDeceasedNORC]: fieldToConceptIdMapping.no,
            [fieldToConceptIdMapping.activityParticipantRefusal]: {
                [fieldToConceptIdMapping.baselineMouthwashSample]: fieldToConceptIdMapping.no,
            },
            [fieldToConceptIdMapping.collectionDetails]: {
                [fieldToConceptIdMapping.baseline]: {
                    [fieldToConceptIdMapping.bloodOrUrineCollected]: fieldToConceptIdMapping.yes,
                    [fieldToConceptIdMapping.bloodOrUrineCollectedTimestamp]: '2024-09-27T00:00:00.000Z',
                },
            },
        });

        it('should set kitStatus to initialized with missing bioKitMouthwash object', () => {
            const data = buildEligibleData();
            const updates = validation.processMouthwashEligibility(data);

            expect(
                updates[
                    `${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwash}.${fieldToConceptIdMapping.kitStatus}`
                ],
            ).toBe(fieldToConceptIdMapping.initialized);
        });

        it('should set kitStatus to initialized with bioKitMouthwash object present and no kitStatus', () => {
            const data = buildEligibleData();
            data[fieldToConceptIdMapping.collectionDetails][fieldToConceptIdMapping.baseline][fieldToConceptIdMapping.bioKitMouthwash] = {};

            const updates = validation.processMouthwashEligibility(data);

            expect(
                updates[
                    `${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwash}.${fieldToConceptIdMapping.kitStatus}`
                ],
            ).toBe(fieldToConceptIdMapping.initialized);
        });

        it('should set kitStatus to initialized with bioKitMouthwash object absent', () => {
            const data = buildEligibleData();
            const updates = validation.processMouthwashEligibility(data);

            expect(
                updates[
                    `${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwash}.${fieldToConceptIdMapping.kitStatus}`
                ],
            ).toBe(fieldToConceptIdMapping.initialized);
        });

        it('should not set kitStatus when participant withdrew consent', () => {
            const data = buildEligibleData();
            data[fieldToConceptIdMapping.withdrawConsent] = fieldToConceptIdMapping.yes;

            const updates = validation.processMouthwashEligibility(data);
            expect(Object.keys(updates)).toHaveLength(0);
        });

        it('should not set kitStatus when participant is deceased', () => {
            const data = buildEligibleData();
            data[fieldToConceptIdMapping.participantDeceasedNORC] = fieldToConceptIdMapping.yes;

            const updates = validation.processMouthwashEligibility(data);
            expect(Object.keys(updates)).toHaveLength(0);
        });

        it('should not set kitStatus when participant refused baseline mouthwash', () => {
            const data = buildEligibleData();
            data[fieldToConceptIdMapping.activityParticipantRefusal][fieldToConceptIdMapping.baselineMouthwashSample] = fieldToConceptIdMapping.yes;

            const updates = validation.processMouthwashEligibility(data);
            expect(Object.keys(updates)).toHaveLength(0);
        });

        it('should not set kitStatus when blood or urine was not collected', () => {
            const data = buildEligibleData();
            data[fieldToConceptIdMapping.collectionDetails][fieldToConceptIdMapping.baseline][fieldToConceptIdMapping.bloodOrUrineCollected] = fieldToConceptIdMapping.no;

            const updates = validation.processMouthwashEligibility(data);
            expect(Object.keys(updates)).toHaveLength(0);
        });

        it('should not set kitStatus when blood or urine was collected before April 1 2024', () => {
            const data = buildEligibleData();
            data[fieldToConceptIdMapping.collectionDetails][fieldToConceptIdMapping.baseline][fieldToConceptIdMapping.bloodOrUrineCollectedTimestamp] = '2023-09-27T00:00:00.000Z';

            const updates = validation.processMouthwashEligibility(data);
            expect(Object.keys(updates)).toHaveLength(0);
        });

        it('should set kitStatus to addressUndeliverable when participant has PO box', () => {
            const data = buildEligibleData();
            data[fieldToConceptIdMapping.collectionDetails][fieldToConceptIdMapping.baseline][fieldToConceptIdMapping.bioKitMouthwash] = {
                [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.initialized,
            };
            data[fieldToConceptIdMapping.address1] = 'PO Box 1033';

            const updates = validation.processMouthwashEligibility(data);

            expect(
                updates[
                    `${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwash}.${fieldToConceptIdMapping.kitStatus}`
                ],
            ).toBe(fieldToConceptIdMapping.addressUndeliverable);
        });

        it('should not set kitStatus when participant kit has already shipped', () => {
            const data = buildEligibleData();
            data[fieldToConceptIdMapping.collectionDetails][fieldToConceptIdMapping.baseline][fieldToConceptIdMapping.bioKitMouthwash] = {
                [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.shipped,
            };

            const updates = validation.processMouthwashEligibility(data);
            expect(updates).toEqual({});
        });

        it('should set BL1 initialized kit to addressUndeliverable when address becomes invalid', () => {
            const data = buildEligibleData();
            data[fieldToConceptIdMapping.collectionDetails][fieldToConceptIdMapping.baseline][fieldToConceptIdMapping.bloodOrUrineCollected] = fieldToConceptIdMapping.no;
            data[fieldToConceptIdMapping.collectionDetails][fieldToConceptIdMapping.baseline][fieldToConceptIdMapping.bioKitMouthwashBL1] = {
                [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.initialized,
            };
            data[fieldToConceptIdMapping.address1] = 'PO Box 99';

            const updates = validation.processMouthwashEligibility(data);

            expect(
                updates[
                    `${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwashBL1}.${fieldToConceptIdMapping.kitStatus}`
                ],
            ).toBe(fieldToConceptIdMapping.addressUndeliverable);
        });

        it('should set BL2 initialized kit to addressUndeliverable when address becomes invalid', () => {
            const data = buildEligibleData();
            data[fieldToConceptIdMapping.collectionDetails][fieldToConceptIdMapping.baseline][fieldToConceptIdMapping.bloodOrUrineCollected] = fieldToConceptIdMapping.no;
            data[fieldToConceptIdMapping.collectionDetails][fieldToConceptIdMapping.baseline][fieldToConceptIdMapping.bioKitMouthwashBL2] = {
                [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.initialized,
            };
            data[fieldToConceptIdMapping.address1] = 'PO Box 100';

            const updates = validation.processMouthwashEligibility(data);

            expect(
                updates[
                    `${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwashBL2}.${fieldToConceptIdMapping.kitStatus}`
                ],
            ).toBe(fieldToConceptIdMapping.addressUndeliverable);
        });

        it('should not update initialized kit status when participant remains address-eligible', () => {
            const data = buildEligibleData();
            data[fieldToConceptIdMapping.collectionDetails][fieldToConceptIdMapping.baseline][fieldToConceptIdMapping.bloodOrUrineCollected] = fieldToConceptIdMapping.no;
            data[fieldToConceptIdMapping.collectionDetails][fieldToConceptIdMapping.baseline][fieldToConceptIdMapping.bioKitMouthwash] = {
                [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.initialized,
            };
            data[fieldToConceptIdMapping.address1] = '123 Main St';

            const updates = validation.processMouthwashEligibility(data);
            expect(updates).toEqual({});
        });

        it('should not initialize mouthwash kit when baseline mouthwash is already collected', () => {
            const data = buildEligibleData();
            data[fieldToConceptIdMapping.baselineMouthwashCollected] = fieldToConceptIdMapping.yes;

            const updates = validation.processMouthwashEligibility(data);
            expect(updates).toEqual({});
        });
    });

    describe('checkDerivedVariables', () => {
        it('should return early when participant does not exist', async () => {
            const getParticipantDataSpy = vi.spyOn(firestore, 'getParticipantData').mockResolvedValue(false);
            const getSpecimenCollectionsSpy = vi.spyOn(firestore, 'getSpecimenCollections');
            const retrieveUserSurveysSpy = vi.spyOn(firestore, 'retrieveUserSurveys');
            const updateParticipantDataSpy = vi.spyOn(firestore, 'updateParticipantData');

            await validation.checkDerivedVariables('missing-token', 'missing-site');

            expect(getParticipantDataSpy).toHaveBeenCalledWith('missing-token', 'missing-site');
            expect(getSpecimenCollectionsSpy).not.toHaveBeenCalled();
            expect(retrieveUserSurveysSpy).not.toHaveBeenCalled();
            expect(updateParticipantDataSpy).not.toHaveBeenCalled();
        });

        it('should return early when participant has no uid in state', async () => {
            vi.spyOn(firestore, 'getParticipantData').mockResolvedValue({
                id: 'participant-doc',
                data: {
                    state: {},
                },
            });
            const getSpecimenCollectionsSpy = vi.spyOn(firestore, 'getSpecimenCollections').mockResolvedValue([]);
            const retrieveUserSurveysSpy = vi.spyOn(firestore, 'retrieveUserSurveys');
            const updateParticipantDataSpy = vi.spyOn(firestore, 'updateParticipantData');

            await validation.checkDerivedVariables('token', 'site');

            expect(getSpecimenCollectionsSpy).toHaveBeenCalledTimes(1);
            expect(retrieveUserSurveysSpy).not.toHaveBeenCalled();
            expect(updateParticipantDataSpy).not.toHaveBeenCalled();
        });

        it('should not write participant updates when derived updates are empty', async () => {
            vi.spyOn(firestore, 'getParticipantData').mockResolvedValue({
                id: 'participant-doc',
                data: {
                    [fieldToConceptIdMapping.dataDestruction.incentive]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.dataDestruction.incentiveEligible]: fieldToConceptIdMapping.yes,
                        },
                    },
                    [fieldToConceptIdMapping.dataDestruction.menstrualSurveyEligible]: fieldToConceptIdMapping.yes,
                    [fieldToConceptIdMapping.dataDestruction.allBaselineSurveysCompleted]: fieldToConceptIdMapping.yes,
                    [fieldToConceptIdMapping.baselineBloodAndUrineIsRefused]: fieldToConceptIdMapping.yes,
                    [fieldToConceptIdMapping.collectionDetails]: {},
                    [fieldToConceptIdMapping.dataDestruction.anyRefusalOrWithdrawal]: fieldToConceptIdMapping.yes,
                    [fieldToConceptIdMapping.activityParticipantRefusal]: {},
                    state: {
                        uid: 'participant-uid',
                    },
                },
            });
            vi.spyOn(firestore, 'getSpecimenCollections').mockResolvedValue([]);
            vi.spyOn(firestore, 'retrieveUserSurveys').mockResolvedValue({});
            const updateParticipantDataSpy = vi.spyOn(firestore, 'updateParticipantData');

            await validation.checkDerivedVariables('token', 'site');

            expect(updateParticipantDataSpy).not.toHaveBeenCalled();
        });

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
                    state: {
                        uid: uuid.v4()
                    }
                },
                specimens: [],
                surveys: {},
                updatesHolder: undefined,
                skipDateComparison: true,
                expected: {
                    [fieldToConceptIdMapping.dataDestruction.anyRefusalOrWithdrawal]: fieldToConceptIdMapping.yes,
                    [fieldToConceptIdMapping.baselineBloodAndUrineIsRefused]: fieldToConceptIdMapping.yes
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
                    state: {
                        uid: uuid.v4()
                    }
                },
                specimens: [],
                surveys: {},
                updatesHolder: undefined,
                skipDateComparison: true,
                expected: {
                    [fieldToConceptIdMapping.dataDestruction.anyRefusalOrWithdrawal]: fieldToConceptIdMapping.yes
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
                    state: {
                        uid: uuid.v4()
                    }
                },
                specimens: [],
                surveys: {},
                updatesHolder: undefined,
                skipDateComparison: true,
                expected: {
                    [fieldToConceptIdMapping.dataDestruction.anyRefusalOrWithdrawal]: fieldToConceptIdMapping.yes
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
                    state: {
                        uid: uuid.v4()
                    }
                },
                specimens: [],
                surveys: {},
                updatesHolder: undefined,
                skipDateComparison: true,
                expected: {
                    [fieldToConceptIdMapping.dataDestruction.anyRefusalOrWithdrawal]: fieldToConceptIdMapping.yes
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

        const incentiveEligibleTimestampKey = `${fieldToConceptIdMapping.dataDestruction.incentive}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.dataDestruction.dateIncentiveEligible}`;

        for (let i = 0; i < testInfo.length; i += 1) {
            const thisTest = testInfo[i];
            it(`${i}: ${thisTest.label}`, async () => {
                let updatesHolder;

                vi.spyOn(firestore, 'getParticipantData').mockImplementation(() => ({
                    data: thisTest.participantInfo,
                    id: thisTest.participantInfo.state.uid,
                }));
                vi.spyOn(firestore, 'getSpecimenCollections').mockImplementation(() => thisTest.specimens);
                vi.spyOn(firestore, 'retrieveUserSurveys').mockImplementation(() => thisTest.surveys);
                vi.spyOn(firestore, 'updateParticipantData').mockImplementation((doc, updates) => {
                    updatesHolder = updates;
                });

                await validation.checkDerivedVariables('fake', 'fake');

                expect(updatesHolder).not.toBe(undefined);

                const clonedUpdatesHolder = { ...updatesHolder };
                delete clonedUpdatesHolder[incentiveEligibleTimestampKey];

                expect(clonedUpdatesHolder).toEqual(thisTest.expected);

                if (!thisTest.skipDateComparison) {
                    expect(Math.abs(new Date(updatesHolder[incentiveEligibleTimestampKey]).getTime() - Date.now())).toBeLessThanOrEqual(60000);
                }
            });
        }
    });
});
