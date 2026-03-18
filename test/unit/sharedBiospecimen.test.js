const shared = require('../../utils/shared');
const fieldToConceptIdMapping = require('../../utils/fieldToConceptIdMapping');

const expectRecentTimestamp = (timestamp) => {
    expect(Math.abs(new Date(timestamp).getTime() - Date.now())).toBeLessThanOrEqual(60000);
};

describe('Shared Biospecimen Helpers', () => {
    describe('processParticipantHomeMouthwashKitData', () => {
        const {
            collectionDetails,
            baseline,
            bioKitMouthwash,
            firstName,
            lastName,
            isPOBox,
            isIntlAddr,
            address1,
            address2,
            physicalAddress1,
            physicalAddress2,
            city,
            state,
            zip,
            physicalCity,
            physicalState,
            physicalZip,
            physicalAddrIntl,
            yes,
            no,
        } = fieldToConceptIdMapping;

        it('should return null for PO boxes', () => {
            const result1 = shared.processParticipantHomeMouthwashKitData({ [address1]: 'PO Box 1033' }, false);
            const result2 = shared.processParticipantHomeMouthwashKitData({ [address1]: 'P.O. Box 1033' }, false);
            const result3 = shared.processParticipantHomeMouthwashKitData({ [address1]: 'po box 1033' }, false);
            const result4 = shared.processParticipantHomeMouthwashKitData({ [address1]: 'p.o. Box 1033' }, false);
            const result5 = shared.processParticipantHomeMouthwashKitData({ [address1]: 'Post Office Box 1033' }, false);
            const result6 = shared.processParticipantHomeMouthwashKitData({ [address1]: 'post office box 1033' }, false);

            expect(result1).toBe(null);
            expect(result2).toBe(null);
            expect(result3).toBe(null);
            expect(result4).toBe(null);
            expect(result5).toBe(null);
            expect(result6).toBe(null);
        });

        it('should permit PO boxes if flag is included', () => {
            const record = {
                [firstName]: 'First',
                [lastName]: 'Last',
                [address1]: 'post office box 1033',
                [city]: 'City',
                [state]: 'PA',
                [zip]: '19104',
                Connect_ID: 123456789,
                [collectionDetails]: {
                    [baseline]: {
                        [bioKitMouthwash]: undefined,
                    },
                },
            };

            const result = shared.processParticipantHomeMouthwashKitData(record, true, true);

            expect(result).toEqual({
                first_name: 'First',
                last_name: 'Last',
                connect_id: 123456789,
                visit: 'BL',
                address_1: 'post office box 1033',
                address_2: '',
                city: 'City',
                state: 'PA',
                zip_code: '19104',
                requestDate: undefined,
            });
        });

        it('should return empty array if printLabel is false and record does not have mouthwash', () => {
            const result = shared.processParticipantHomeMouthwashKitData(
                {
                    [address1]: '123 Fake Street',
                    [collectionDetails]: {
                        [baseline]: {
                            [bioKitMouthwash]: undefined,
                        },
                    },
                },
                false,
            );

            expect(Array.isArray(result)).toBe(true);
            expect(result).toHaveLength(0);
        });

        it('should return record if record has no mouthwash but printLabel is true', () => {
            const record = {
                [firstName]: 'First',
                [lastName]: 'Last',
                [address1]: '123 Fake Street',
                [city]: 'City',
                [state]: 'PA',
                [zip]: '19104',
                Connect_ID: 123456789,
                [collectionDetails]: {
                    [baseline]: {
                        [bioKitMouthwash]: undefined,
                    },
                },
            };

            const result = shared.processParticipantHomeMouthwashKitData(record, true);

            expect(result.first_name).toBe(record[firstName]);
            expect(result.last_name).toBe(record[lastName]);
            expect(result.address_1).toBe(record[address1]);
            expect(result.address_2).toBe('');
            expect(result.city).toBe(record[city]);
            expect(result.state).toBe(record[state]);
            expect(result.zip_code).toBe(record[zip]);
            expect(result.connect_id).toBe(record.Connect_ID);
        });

        it('should return record if printLabel is false but record has mouthwash', () => {
            const record = {
                [firstName]: 'First',
                [lastName]: 'Last',
                [address1]: '123 Fake Street',
                [city]: 'City',
                [state]: 'PA',
                [zip]: '19104',
                Connect_ID: 123456789,
                [collectionDetails]: {
                    [baseline]: {
                        [bioKitMouthwash]: fieldToConceptIdMapping.yes,
                    },
                },
            };

            const result = shared.processParticipantHomeMouthwashKitData(record, false);

            expect(result.first_name).toBe(record[firstName]);
            expect(result.last_name).toBe(record[lastName]);
            expect(result.address_1).toBe(record[address1]);
            expect(result.address_2).toBe('');
            expect(result.city).toBe(record[city]);
            expect(result.state).toBe(record[state]);
            expect(result.zip_code).toBe(record[zip]);
            expect(result.connect_id).toBe(record.Connect_ID);
        });

        it('should return record if printLabel is true and record has mouthwash', () => {
            const record = {
                [firstName]: 'First',
                [lastName]: 'Last',
                [address1]: '123 Fake Street',
                [city]: 'City',
                [state]: 'PA',
                [zip]: '19104',
                Connect_ID: 123456789,
                [collectionDetails]: {
                    [baseline]: {
                        [bioKitMouthwash]: fieldToConceptIdMapping.yes,
                    },
                },
            };

            const result = shared.processParticipantHomeMouthwashKitData(record, true);

            expect(result.first_name).toBe(record[firstName]);
            expect(result.last_name).toBe(record[lastName]);
            expect(result.address_1).toBe(record[address1]);
            expect(result.address_2).toBe('');
            expect(result.city).toBe(record[city]);
            expect(result.state).toBe(record[state]);
            expect(result.zip_code).toBe(record[zip]);
            expect(result.connect_id).toBe(record.Connect_ID);
        });

        it('should use physical address if physical address is provided even if mailing address is not a PO box', () => {
            const result = shared.processParticipantHomeMouthwashKitData(
                {
                    [firstName]: 'First',
                    [lastName]: 'Last',
                    [isPOBox]: no,
                    [address1]: '321 Physical Street',
                    [physicalAddress1]: '123 Fake St',
                    [physicalCity]: 'City',
                    [physicalState]: 'PA',
                    [physicalZip]: '19104',
                    Connect_ID: 123456789,
                    [collectionDetails]: {
                        [baseline]: {
                            [bioKitMouthwash]: undefined,
                        },
                    },
                },
                true,
            );

            expect(result).toEqual({
                first_name: 'First',
                last_name: 'Last',
                requestDate: undefined,
                visit: 'BL',
                connect_id: 123456789,
                address_1: '123 Fake St',
                address_2: '',
                city: 'City',
                state: 'PA',
                zip_code: '19104',
            });
        });

        it('should use physical address if primary address is marked as PO box', () => {
            const result = shared.processParticipantHomeMouthwashKitData(
                {
                    [firstName]: 'First',
                    [lastName]: 'Last',
                    [isPOBox]: yes,
                    [address1]: 'Pno Box 1033',
                    [physicalAddress1]: '123 Fake St',
                    [physicalCity]: 'City',
                    [physicalState]: 'PA',
                    [physicalZip]: '19104',
                    Connect_ID: 123456789,
                    [collectionDetails]: {
                        [baseline]: {
                            [bioKitMouthwash]: undefined,
                        },
                    },
                },
                true,
            );

            expect(result).toEqual({
                first_name: 'First',
                last_name: 'Last',
                requestDate: undefined,
                visit: 'BL',
                connect_id: 123456789,
                address_1: '123 Fake St',
                address_2: '',
                city: 'City',
                state: 'PA',
                zip_code: '19104',
            });
        });

        it('should use physical address if primary address matches PO box pattern', () => {
            const result = shared.processParticipantHomeMouthwashKitData(
                {
                    [firstName]: 'First',
                    [lastName]: 'Last',
                    requestDate: undefined,
                    visit: 'BL',
                    [address1]: 'PO Box 1033',
                    [physicalAddress1]: '123 Fake St',
                    [physicalCity]: 'City',
                    [physicalState]: 'PA',
                    [physicalZip]: '19104',
                    Connect_ID: 123456789,
                    [collectionDetails]: {
                        [baseline]: {
                            [bioKitMouthwash]: undefined,
                        },
                    },
                },
                true,
            );

            expect(result).toEqual({
                first_name: 'First',
                last_name: 'Last',
                requestDate: undefined,
                visit: 'BL',
                connect_id: 123456789,
                address_1: '123 Fake St',
                address_2: '',
                city: 'City',
                state: 'PA',
                zip_code: '19104',
            });
        });

        it('should use mailing address if physical address is a PO box and mailing is not', () => {
            const result = shared.processParticipantHomeMouthwashKitData(
                {
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
                    Connect_ID: 123456789,
                    [collectionDetails]: {
                        [baseline]: {
                            [bioKitMouthwash]: undefined,
                        },
                    },
                },
                true,
            );

            expect(result).toEqual({
                first_name: 'First',
                last_name: 'Last',
                requestDate: undefined,
                visit: 'BL',
                connect_id: 123456789,
                address_1: '123 Fake St',
                address_2: '',
                city: 'City',
                state: 'PA',
                zip_code: '19104',
            });
        });

        it('should return null if physical and mailing addresses are PO boxes', () => {
            const result1 = shared.processParticipantHomeMouthwashKitData(
                {
                    [firstName]: 'First',
                    [lastName]: 'Last',
                    [address1]: 'PO Box 1033',
                    [physicalAddress1]: 'PO Box 1033',
                    [physicalCity]: 'City',
                    [physicalState]: 'PA',
                    [physicalZip]: '19104',
                    Connect_ID: 123456789,
                    [collectionDetails]: {
                        [baseline]: {
                            [bioKitMouthwash]: undefined,
                        },
                    },
                },
                true,
            );
            const result2 = shared.processParticipantHomeMouthwashKitData(
                {
                    [firstName]: 'First',
                    [lastName]: 'Last',
                    [isPOBox]: yes,
                    [address1]: 'PznO Box 1033',
                    [physicalAddress1]: 'PO Box 1033',
                    [physicalCity]: 'City',
                    [physicalState]: 'PA',
                    [physicalZip]: '19104',
                    Connect_ID: 123456789,
                    [collectionDetails]: {
                        [baseline]: {
                            [bioKitMouthwash]: undefined,
                        },
                    },
                },
                true,
            );

            expect(result1).toBe(null);
            expect(result2).toBe(null);
        });

        it('should use mailing address if physical address is international and mailing is not', () => {
            const result = shared.processParticipantHomeMouthwashKitData(
                {
                    [firstName]: 'First',
                    [lastName]: 'Last',
                    [isPOBox]: no,
                    [address1]: '123 Fake St',
                    [city]: 'City',
                    [state]: 'PA',
                    [zip]: '19104',
                    [physicalAddress1]: '987 False Road',
                    [physicalCity]: 'City',
                    [physicalState]: 'PA',
                    [physicalZip]: '17102',
                    [physicalAddrIntl]: yes,
                    Connect_ID: 123456789,
                    [collectionDetails]: {
                        [baseline]: {
                            [bioKitMouthwash]: undefined,
                        },
                    },
                },
                true,
            );

            expect(result).toEqual({
                first_name: 'First',
                last_name: 'Last',
                requestDate: undefined,
                visit: 'BL',
                connect_id: 123456789,
                address_1: '123 Fake St',
                address_2: '',
                city: 'City',
                state: 'PA',
                zip_code: '19104',
            });
        });

        it('should return null if mailing address is international and there is no physical address', () => {
            const result = shared.processParticipantHomeMouthwashKitData(
                {
                    [firstName]: 'First',
                    [lastName]: 'Last',
                    [address1]: '123 Fake St',
                    [isIntlAddr]: yes,
                    Connect_ID: 123456789,
                    [collectionDetails]: {
                        [baseline]: {
                            [bioKitMouthwash]: undefined,
                        },
                    },
                },
                true,
            );

            expect(result).toBe(null);
        });

        it('should return null if mailing address is PO box and physical is international', () => {
            const result1 = shared.processParticipantHomeMouthwashKitData(
                {
                    [firstName]: 'First',
                    [lastName]: 'Last',
                    [address1]: 'PO Box 1033',
                    [physicalAddress1]: '123 Fake St',
                    [physicalCity]: 'City',
                    [physicalState]: 'PA',
                    [physicalZip]: '19104',
                    [physicalAddrIntl]: yes,
                    Connect_ID: 123456789,
                    [collectionDetails]: {
                        [baseline]: {
                            [bioKitMouthwash]: undefined,
                        },
                    },
                },
                true,
            );
            const result2 = shared.processParticipantHomeMouthwashKitData(
                {
                    [firstName]: 'First',
                    [lastName]: 'Last',
                    [isPOBox]: yes,
                    [address1]: 'PznO Box 1033',
                    [physicalAddress1]: '123 Fake St',
                    [physicalCity]: 'City',
                    [physicalState]: 'PA',
                    [physicalZip]: '19104',
                    [physicalAddrIntl]: yes,
                    Connect_ID: 123456789,
                    [collectionDetails]: {
                        [baseline]: {
                            [bioKitMouthwash]: undefined,
                        },
                    },
                },
                true,
            );

            expect(result1).toBe(null);
            expect(result2).toBe(null);
        });

        it('should return null if both physical and mailing addresses are international', () => {
            const result = shared.processParticipantHomeMouthwashKitData(
                {
                    [firstName]: 'First',
                    [lastName]: 'Last',
                    [address1]: '987 False Rd',
                    [isIntlAddr]: yes,
                    [physicalAddress1]: '123 Fake St',
                    [physicalCity]: 'City',
                    [physicalState]: 'PA',
                    [physicalZip]: '19104',
                    [physicalAddrIntl]: yes,
                    Connect_ID: 123456789,
                    [collectionDetails]: {
                        [baseline]: {
                            [bioKitMouthwash]: undefined,
                        },
                    },
                },
                true,
            );

            expect(result).toBe(null);
        });
    });

    describe('getHomeMWKitData', () => {
        describe('first replacement kit', () => {
            it('should set a first replacement kit', () => {
                const statuses = [fieldToConceptIdMapping.shipped, fieldToConceptIdMapping.received];
                const baseObj = {
                    [fieldToConceptIdMapping.firstName]: 'First',
                    [fieldToConceptIdMapping.lastName]: 'Last',
                    [fieldToConceptIdMapping.address1]: '123 Fake Street',
                    [fieldToConceptIdMapping.city]: 'City',
                    [fieldToConceptIdMapping.state]: 'PA',
                    [fieldToConceptIdMapping.zip]: '19104',
                    Connect_ID: 123456789,
                };

                statuses.forEach((status) => {
                    const data = {
                        ...baseObj,
                        [fieldToConceptIdMapping.collectionDetails]: {
                            [fieldToConceptIdMapping.baseline]: {
                                [fieldToConceptIdMapping.bioKitMouthwash]: {
                                    [fieldToConceptIdMapping.kitStatus]: status,
                                },
                            },
                        },
                    };
                    const updates = shared.getHomeMWKitData(data);
                    const path = `${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwashBL1}.${fieldToConceptIdMapping.dateKitRequested}`;
                    const clonedUpdates = { ...updates };
                    delete clonedUpdates[path];

                    expectRecentTimestamp(updates[path]);
                    expect(clonedUpdates).toEqual({
                        [`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwashBL1}.${fieldToConceptIdMapping.kitType}`]: fieldToConceptIdMapping.mouthwashKit,
                        [`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwashBL1}.${fieldToConceptIdMapping.kitStatus}`]: fieldToConceptIdMapping.initialized,
                    });
                });
            });

            it('should request an initial kit for a participant with no collection details', () => {
                const data = {
                    [fieldToConceptIdMapping.firstName]: 'First',
                    [fieldToConceptIdMapping.lastName]: 'Last',
                    [fieldToConceptIdMapping.address1]: '123 Fake Street',
                    [fieldToConceptIdMapping.city]: 'City',
                    [fieldToConceptIdMapping.state]: 'PA',
                    [fieldToConceptIdMapping.zip]: '19104',
                    Connect_ID: 123456789,
                };

                const updates = shared.getHomeMWKitData(data);
                const path = `${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwash}.${fieldToConceptIdMapping.dateKitRequested}`;
                const clonedUpdates = { ...updates };
                delete clonedUpdates[path];

                expectRecentTimestamp(updates[path]);
                expect(clonedUpdates).toEqual({
                    [`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwash}.${fieldToConceptIdMapping.kitType}`]: fieldToConceptIdMapping.mouthwashKit,
                    [`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwash}.${fieldToConceptIdMapping.kitStatus}`]: fieldToConceptIdMapping.initialized,
                    [`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bloodOrUrineCollectedTimestamp}`]: null,
                });
            });

            it('should request an initial kit for a participant with a pending or nonexistent home MW kit', () => {
                const statuses = [fieldToConceptIdMapping.pending, undefined, null];
                const baseObj = {
                    [fieldToConceptIdMapping.firstName]: 'First',
                    [fieldToConceptIdMapping.lastName]: 'Last',
                    [fieldToConceptIdMapping.address1]: '123 Fake Street',
                    [fieldToConceptIdMapping.city]: 'City',
                    [fieldToConceptIdMapping.state]: 'PA',
                    [fieldToConceptIdMapping.zip]: '19104',
                    Connect_ID: 123456789,
                    [fieldToConceptIdMapping.collectionDetails]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.bioKitMouthwash]: {
                                [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.pending,
                            },
                        },
                    },
                };

                statuses.forEach((status) => {
                    const data = {
                        ...baseObj,
                        [fieldToConceptIdMapping.collectionDetails]: {
                            [fieldToConceptIdMapping.baseline]: {
                                [fieldToConceptIdMapping.bioKitMouthwash]: {
                                    [fieldToConceptIdMapping.kitStatus]: status,
                                },
                            },
                        },
                    };

                    const updates = shared.getHomeMWKitData(data);
                    const path = `${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwash}.${fieldToConceptIdMapping.dateKitRequested}`;
                    const clonedUpdates = { ...updates };
                    delete clonedUpdates[path];

                    expectRecentTimestamp(updates[path]);
                    expect(clonedUpdates).toEqual({
                        [`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwash}.${fieldToConceptIdMapping.kitType}`]: fieldToConceptIdMapping.mouthwashKit,
                        [`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwash}.${fieldToConceptIdMapping.kitStatus}`]: fieldToConceptIdMapping.initialized,
                        [`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bloodOrUrineCollectedTimestamp}`]: null,
                    });
                });
            });

            it('should prevent participant whose initial home MW kit has not been sent from obtaining a replacement', () => {
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
                    Connect_ID: 123456789,
                };

                statuses.forEach((status) => {
                    const data = {
                        ...baseObj,
                        [fieldToConceptIdMapping.collectionDetails]: {
                            [fieldToConceptIdMapping.baseline]: {
                                [fieldToConceptIdMapping.bioKitMouthwash]: {
                                    [fieldToConceptIdMapping.kitStatus]: status,
                                },
                            },
                        },
                    };

                    expect(() => shared.getHomeMWKitData(data)).toThrow(/This participant's initial home mouthwash kit has not been sent/);
                });
            });

            it('should update initial kitStatus addressUndeliverable to initialized', () => {
                const data = {
                    [fieldToConceptIdMapping.firstName]: 'First',
                    [fieldToConceptIdMapping.lastName]: 'Last',
                    [fieldToConceptIdMapping.address1]: '123 Fake Street',
                    [fieldToConceptIdMapping.city]: 'City',
                    [fieldToConceptIdMapping.state]: 'PA',
                    [fieldToConceptIdMapping.zip]: '19104',
                    Connect_ID: 123456789,
                    [fieldToConceptIdMapping.collectionDetails]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.bioKitMouthwash]: {
                                [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.addressUndeliverable,
                            },
                        },
                    },
                };

                const updates = shared.getHomeMWKitData(data);
                const path = `${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwash}.${fieldToConceptIdMapping.dateKitRequested}`;
                const clonedUpdates = { ...updates };
                delete clonedUpdates[path];

                expectRecentTimestamp(updates[path]);
                expect(clonedUpdates).toEqual({
                    [`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwash}.${fieldToConceptIdMapping.kitType}`]: fieldToConceptIdMapping.mouthwashKit,
                    [`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwash}.${fieldToConceptIdMapping.kitStatus}`]: fieldToConceptIdMapping.initialized,
                });
            });

            it('should throw error on unrecognized initial home MW kitStatus', () => {
                const data = {
                    [fieldToConceptIdMapping.firstName]: 'First',
                    [fieldToConceptIdMapping.lastName]: 'Last',
                    [fieldToConceptIdMapping.address1]: '123 Fake Street',
                    [fieldToConceptIdMapping.city]: 'City',
                    [fieldToConceptIdMapping.state]: 'PA',
                    [fieldToConceptIdMapping.zip]: '19104',
                    Connect_ID: 123456789,
                    [fieldToConceptIdMapping.collectionDetails]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.bioKitMouthwash]: {
                                [fieldToConceptIdMapping.kitStatus]: 'fake',
                            },
                        },
                    },
                };

                expect(() => shared.getHomeMWKitData(data)).toThrow(/Unrecognized kit status fake/);
            });
        });

        describe('second replacement kit', () => {
            it('should set a second replacement kit', () => {
                const statuses = [fieldToConceptIdMapping.shipped, fieldToConceptIdMapping.received];
                const baseObj = {
                    [fieldToConceptIdMapping.firstName]: 'First',
                    [fieldToConceptIdMapping.lastName]: 'Last',
                    [fieldToConceptIdMapping.address1]: '123 Fake Street',
                    [fieldToConceptIdMapping.city]: 'City',
                    [fieldToConceptIdMapping.state]: 'PA',
                    [fieldToConceptIdMapping.zip]: '19104',
                    Connect_ID: 123456789,
                };

                statuses.forEach((status) => {
                    const data = {
                        ...baseObj,
                        [fieldToConceptIdMapping.collectionDetails]: {
                            [fieldToConceptIdMapping.baseline]: {
                                [fieldToConceptIdMapping.bioKitMouthwash]: {
                                    [fieldToConceptIdMapping.kitStatus]: status,
                                },
                                [fieldToConceptIdMapping.bioKitMouthwashBL1]: {
                                    [fieldToConceptIdMapping.kitStatus]: status,
                                },
                            },
                        },
                    };

                    const updates = shared.getHomeMWKitData(data);
                    const path = `${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwashBL2}.${fieldToConceptIdMapping.dateKitRequested}`;
                    const clonedUpdates = { ...updates };
                    delete clonedUpdates[path];

                    expectRecentTimestamp(updates[path]);
                    expect(clonedUpdates).toEqual({
                        [`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwashBL2}.${fieldToConceptIdMapping.kitType}`]: fieldToConceptIdMapping.mouthwashKit,
                        [`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwashBL2}.${fieldToConceptIdMapping.kitStatus}`]: fieldToConceptIdMapping.initialized,
                    });
                });
            });

            it('should prevent participant with pending replacement home MW kit from obtaining a second replacement', () => {
                const data = {
                    [fieldToConceptIdMapping.firstName]: 'First',
                    [fieldToConceptIdMapping.lastName]: 'Last',
                    [fieldToConceptIdMapping.address1]: '123 Fake Street',
                    [fieldToConceptIdMapping.city]: 'City',
                    [fieldToConceptIdMapping.state]: 'PA',
                    [fieldToConceptIdMapping.zip]: '19104',
                    Connect_ID: 123456789,
                    [fieldToConceptIdMapping.collectionDetails]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.bioKitMouthwash]: {
                                [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.addressPrinted,
                            },
                            [fieldToConceptIdMapping.bioKitMouthwashBL1]: {
                                [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.pending,
                            },
                        },
                    },
                };

                expect(() => shared.getHomeMWKitData(data)).toThrow(/This participant is not eligible for a second replacement home mouthwash kit/gi);
            });

            it('should prevent participant whose first replacement home MW kit has not been sent from obtaining a second replacement', () => {
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
                    Connect_ID: 123456789,
                };

                statuses.forEach((status) => {
                    const data = {
                        ...baseObj,
                        [fieldToConceptIdMapping.collectionDetails]: {
                            [fieldToConceptIdMapping.baseline]: {
                                [fieldToConceptIdMapping.bioKitMouthwash]: {
                                    [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.addressPrinted,
                                },
                                [fieldToConceptIdMapping.bioKitMouthwashBL1]: {
                                    [fieldToConceptIdMapping.kitStatus]: status,
                                },
                            },
                        },
                    };

                    expect(() => shared.getHomeMWKitData(data)).toThrow(/This participant's first replacement home mouthwash kit has not been sent/);
                });
            });

            it('should update R1 kitStatus addressUndeliverable to initialized', () => {
                const data = {
                    [fieldToConceptIdMapping.firstName]: 'First',
                    [fieldToConceptIdMapping.lastName]: 'Last',
                    [fieldToConceptIdMapping.address1]: '123 Fake Street',
                    [fieldToConceptIdMapping.city]: 'City',
                    [fieldToConceptIdMapping.state]: 'PA',
                    [fieldToConceptIdMapping.zip]: '19104',
                    Connect_ID: 123456789,
                    [fieldToConceptIdMapping.collectionDetails]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.bioKitMouthwash]: {
                                [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.addressPrinted,
                            },
                            [fieldToConceptIdMapping.bioKitMouthwashBL1]: {
                                [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.addressUndeliverable,
                            },
                        },
                    },
                };

                const updates = shared.getHomeMWKitData(data);
                const path = `${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwashBL1}.${fieldToConceptIdMapping.dateKitRequested}`;
                const clonedUpdates = { ...updates };
                delete clonedUpdates[path];

                expectRecentTimestamp(updates[path]);
                expect(clonedUpdates).toEqual({
                    [`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwashBL1}.${fieldToConceptIdMapping.kitType}`]: fieldToConceptIdMapping.mouthwashKit,
                    [`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwashBL1}.${fieldToConceptIdMapping.kitStatus}`]: fieldToConceptIdMapping.initialized,
                });
            });

            it('should throw error on unrecognized first replacement home MW kitStatus', () => {
                const data = {
                    [fieldToConceptIdMapping.firstName]: 'First',
                    [fieldToConceptIdMapping.lastName]: 'Last',
                    [fieldToConceptIdMapping.address1]: '123 Fake Street',
                    [fieldToConceptIdMapping.city]: 'City',
                    [fieldToConceptIdMapping.state]: 'PA',
                    [fieldToConceptIdMapping.zip]: '19104',
                    Connect_ID: 123456789,
                    [fieldToConceptIdMapping.collectionDetails]: {
                        [fieldToConceptIdMapping.baseline]: {
                            [fieldToConceptIdMapping.bioKitMouthwash]: {
                                [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.addressPrinted,
                            },
                            [fieldToConceptIdMapping.bioKitMouthwashBL1]: {
                                [fieldToConceptIdMapping.kitStatus]: 'fake',
                            },
                        },
                    },
                };

                expect(() => shared.getHomeMWKitData(data)).toThrow(/Unrecognized kit status fake/);
            });
        });

        it('should update R2 kitStatus addressUndeliverable to initialized', () => {
            const data = {
                [fieldToConceptIdMapping.firstName]: 'First',
                [fieldToConceptIdMapping.lastName]: 'Last',
                [fieldToConceptIdMapping.address1]: '123 Fake Street',
                [fieldToConceptIdMapping.city]: 'City',
                [fieldToConceptIdMapping.state]: 'PA',
                [fieldToConceptIdMapping.zip]: '19104',
                Connect_ID: 123456789,
                [fieldToConceptIdMapping.collectionDetails]: {
                    [fieldToConceptIdMapping.baseline]: {
                        [fieldToConceptIdMapping.bioKitMouthwash]: {
                            [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.addressPrinted,
                        },
                        [fieldToConceptIdMapping.bioKitMouthwashBL1]: {
                            [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.received,
                        },
                        [fieldToConceptIdMapping.bioKitMouthwashBL2]: {
                            [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.addressUndeliverable,
                        },
                    },
                },
            };

            const updates = shared.getHomeMWKitData(data);
            const path = `${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwashBL2}.${fieldToConceptIdMapping.dateKitRequested}`;
            const clonedUpdates = { ...updates };
            delete clonedUpdates[path];

            expectRecentTimestamp(updates[path]);
            expect(clonedUpdates).toEqual({
                [`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwashBL2}.${fieldToConceptIdMapping.kitType}`]: fieldToConceptIdMapping.mouthwashKit,
                [`${fieldToConceptIdMapping.collectionDetails}.${fieldToConceptIdMapping.baseline}.${fieldToConceptIdMapping.bioKitMouthwashBL2}.${fieldToConceptIdMapping.kitStatus}`]: fieldToConceptIdMapping.initialized,
            });
        });

        it('should prevent participant with second replacement kit from obtaining another', () => {
            const data = {
                [fieldToConceptIdMapping.firstName]: 'First',
                [fieldToConceptIdMapping.lastName]: 'Last',
                [fieldToConceptIdMapping.address1]: '123 Fake Street',
                [fieldToConceptIdMapping.city]: 'City',
                [fieldToConceptIdMapping.state]: 'PA',
                [fieldToConceptIdMapping.zip]: '19104',
                Connect_ID: 123456789,
                [fieldToConceptIdMapping.collectionDetails]: {
                    [fieldToConceptIdMapping.baseline]: {
                        [fieldToConceptIdMapping.bioKitMouthwash]: {
                            [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.addressPrinted,
                        },
                        [fieldToConceptIdMapping.bioKitMouthwashBL1]: {
                            [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.addressPrinted,
                        },
                        [fieldToConceptIdMapping.bioKitMouthwashBL2]: {
                            [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.initialized,
                        },
                    },
                },
            };

            expect(() => shared.getHomeMWKitData(data)).toThrow(/Participant has exceeded supported number of replacement kits\./);
        });

        it('should prevent participant with invalid address from obtaining a replacement kit', () => {
            const data = {
                [fieldToConceptIdMapping.firstName]: 'First',
                [fieldToConceptIdMapping.lastName]: 'Last',
                [fieldToConceptIdMapping.address1]: 'P.O. Box 1234',
                [fieldToConceptIdMapping.city]: 'City',
                [fieldToConceptIdMapping.state]: 'PA',
                [fieldToConceptIdMapping.zip]: '19104',
                Connect_ID: 123456789,
                [fieldToConceptIdMapping.collectionDetails]: {
                    [fieldToConceptIdMapping.baseline]: {
                        [fieldToConceptIdMapping.bioKitMouthwash]: {
                            [fieldToConceptIdMapping.kitStatus]: fieldToConceptIdMapping.addressPrinted,
                        },
                    },
                },
            };

            expect(() => shared.getHomeMWKitData(data)).toThrow(/Participant address information is invalid\./);
        });
    });

    describe('updateBaselineData', () => {
        it('should not update if visit is neither baseline nor clinical', () => {
            const participantUpdates = shared.updateBaselineData({}, {}, []);
            expect(participantUpdates).toEqual({});
        });

        it('should not update if visit is baseline but collectionSetting is missing', () => {
            const biospecimenData = {
                [fieldToConceptIdMapping.collectionSelectedVisit]: fieldToConceptIdMapping.baseline,
            };
            const participantUpdates = shared.updateBaselineData(biospecimenData, {}, []);
            expect(participantUpdates).toEqual({});
        });

        it('should not update if setting is clinical but selected visit is missing', () => {
            const biospecimenData = {
                [fieldToConceptIdMapping.collectionSetting]: fieldToConceptIdMapping.clinical,
            };
            const participantUpdates = shared.updateBaselineData(biospecimenData, {}, []);
            expect(participantUpdates).toEqual({});
        });

        it('should update baseline fields for a clinical collection with blood and urine', () => {
            const siteTubesList = [
                { concept: 'bloodTubeA', tubeType: 'Blood tube' },
                { concept: 'urineTubeA', tubeType: 'Urine' },
                { concept: 'mouthwashTubeA', tubeType: 'Mouthwash' },
            ];
            const scannedTime = '2025-01-02T10:30:00.000Z';
            const biospecimenData = {
                [fieldToConceptIdMapping.collectionSelectedVisit]: fieldToConceptIdMapping.baseline,
                [fieldToConceptIdMapping.collectionSetting]: fieldToConceptIdMapping.clinical,
                [fieldToConceptIdMapping.collectionScannedTime]: scannedTime,
                [fieldToConceptIdMapping.collectionDateTimeStamp]: scannedTime,
                bloodTubeA: {
                    [fieldToConceptIdMapping.tubeIsCollected]: fieldToConceptIdMapping.yes,
                },
                urineTubeA: {
                    [fieldToConceptIdMapping.tubeIsCollected]: fieldToConceptIdMapping.yes,
                },
                mouthwashTubeA: {
                    [fieldToConceptIdMapping.tubeIsCollected]: fieldToConceptIdMapping.no,
                },
            };

            const participantUpdates = shared.updateBaselineData(biospecimenData, {}, siteTubesList);
            const baselineSettings = participantUpdates[fieldToConceptIdMapping.collectionDetails][fieldToConceptIdMapping.baseline];

            expect(baselineSettings[fieldToConceptIdMapping.bloodCollectionSetting]).toBe(fieldToConceptIdMapping.clinical);
            expect(baselineSettings[fieldToConceptIdMapping.urineCollectionSetting]).toBe(fieldToConceptIdMapping.clinical);
            expect(baselineSettings[fieldToConceptIdMapping.clinicalBloodCollected]).toBe(fieldToConceptIdMapping.yes);
            expect(baselineSettings[fieldToConceptIdMapping.clinicalUrineCollected]).toBe(fieldToConceptIdMapping.yes);
            expect(baselineSettings[fieldToConceptIdMapping.anySpecimenCollected]).toBe(fieldToConceptIdMapping.yes);
            expect(baselineSettings[fieldToConceptIdMapping.anySpecimenCollectedTime]).toBe(scannedTime);

            expect(participantUpdates[fieldToConceptIdMapping.baselineBloodSampleCollected]).toBe(fieldToConceptIdMapping.yes);
            expect(participantUpdates[fieldToConceptIdMapping.baselineUrineCollected]).toBe(fieldToConceptIdMapping.yes);
            expect(participantUpdates[fieldToConceptIdMapping.baselineMouthwashCollected]).toBe(fieldToConceptIdMapping.no);
            expect(participantUpdates[fieldToConceptIdMapping.allBaselineSamplesCollected]).toBe(fieldToConceptIdMapping.yes);
        });

        it('should update baseline fields for a research collection with blood, urine, and mouthwash', () => {
            const siteTubesList = [
                { concept: 'bloodTubeB', tubeType: 'Blood tube' },
                { concept: 'urineTubeB', tubeType: 'Urine' },
                { concept: 'mouthwashTubeB', tubeType: 'Mouthwash' },
            ];
            const collectionTime = '2025-01-03T11:45:00.000Z';
            const biospecimenData = {
                [fieldToConceptIdMapping.collectionSelectedVisit]: fieldToConceptIdMapping.baseline,
                [fieldToConceptIdMapping.collectionSetting]: fieldToConceptIdMapping.research,
                [fieldToConceptIdMapping.collectionDateTimeStamp]: collectionTime,
                [fieldToConceptIdMapping.collectionScannedTime]: collectionTime,
                bloodTubeB: {
                    [fieldToConceptIdMapping.tubeIsCollected]: fieldToConceptIdMapping.yes,
                },
                urineTubeB: {
                    [fieldToConceptIdMapping.tubeIsCollected]: fieldToConceptIdMapping.yes,
                },
                mouthwashTubeB: {
                    [fieldToConceptIdMapping.tubeIsCollected]: fieldToConceptIdMapping.yes,
                },
            };

            const participantUpdates = shared.updateBaselineData(biospecimenData, {}, siteTubesList);
            const baselineSettings = participantUpdates[fieldToConceptIdMapping.collectionDetails][fieldToConceptIdMapping.baseline];

            expect(baselineSettings[fieldToConceptIdMapping.bloodCollectionSetting]).toBe(fieldToConceptIdMapping.research);
            expect(baselineSettings[fieldToConceptIdMapping.urineCollectionSetting]).toBe(fieldToConceptIdMapping.research);
            expect(baselineSettings[fieldToConceptIdMapping.mouthwashCollectionSetting]).toBe(fieldToConceptIdMapping.research);
            expect(baselineSettings[fieldToConceptIdMapping.baselineBloodCollectedTime]).toBe(collectionTime);
            expect(baselineSettings[fieldToConceptIdMapping.baselineUrineCollectedTime]).toBe(collectionTime);
            expect(baselineSettings[fieldToConceptIdMapping.baselineMouthwashCollectedTime]).toBe(collectionTime);

            expect(participantUpdates[fieldToConceptIdMapping.baselineBloodSampleCollected]).toBe(fieldToConceptIdMapping.yes);
            expect(participantUpdates[fieldToConceptIdMapping.baselineUrineCollected]).toBe(fieldToConceptIdMapping.yes);
            expect(participantUpdates[fieldToConceptIdMapping.baselineMouthwashCollected]).toBe(fieldToConceptIdMapping.yes);
            expect(participantUpdates[fieldToConceptIdMapping.allBaselineSamplesCollected]).toBe(fieldToConceptIdMapping.yes);
        });

        it('should preserve first baseline blood collection timestamp when one already exists', () => {
            const siteTubesList = [
                { concept: 'bloodTubeC', tubeType: 'Blood tube' },
            ];
            const existingTime = '2024-05-10T08:00:00.000Z';
            const incomingTime = '2025-01-04T12:00:00.000Z';
            const biospecimenData = {
                [fieldToConceptIdMapping.collectionSelectedVisit]: fieldToConceptIdMapping.baseline,
                [fieldToConceptIdMapping.collectionSetting]: fieldToConceptIdMapping.research,
                [fieldToConceptIdMapping.collectionDateTimeStamp]: incomingTime,
                [fieldToConceptIdMapping.collectionScannedTime]: incomingTime,
                bloodTubeC: {
                    [fieldToConceptIdMapping.tubeIsCollected]: fieldToConceptIdMapping.yes,
                },
            };
            const participantData = {
                [fieldToConceptIdMapping.collectionDetails]: {
                    [fieldToConceptIdMapping.baseline]: {
                        [fieldToConceptIdMapping.baselineBloodCollectedTime]: existingTime,
                    },
                },
            };

            const participantUpdates = shared.updateBaselineData(biospecimenData, participantData, siteTubesList);
            const baselineSettings = participantUpdates[fieldToConceptIdMapping.collectionDetails][fieldToConceptIdMapping.baseline];

            expect(baselineSettings[fieldToConceptIdMapping.baselineBloodCollectedTime]).toBe(existingTime);
        });

        it('should throw when visit is a prototype-polluting key', () => {
            const biospecimenData = {
                [fieldToConceptIdMapping.collectionSelectedVisit]: '__proto__',
            };

            expect(() => shared.updateBaselineData(biospecimenData, {}, [])).toThrow(
                /prototype pollution/i,
            );
        });
    });
});
