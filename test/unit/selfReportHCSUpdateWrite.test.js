/**
 * Firestore write-layer tests for the Self-Report Health Care System Update helpers (issue #1658).
 * Drives the real firestore helpers with the module-mock's collection/add/where (mirroring
 * selfReportCancerDxWrite.test.js).
 */
const { setupTestSuite } = require('../shared/testHelpers');

let mocks;
let firestore;

beforeAll(() => {
    const mockSystem = setupTestSuite({ setupConsole: false, setupModuleMocks: true });
    mocks = mockSystem.mocks;
    firestore = require('../../utils/firestore');
});

afterEach(() => {
    vi.restoreAllMocks();
});

describe('addSelfReportHCSUpdateDoc', () => {
    it('appends the doc to selfReportHCSUpdates (add, never set/merge)', async () => {
        const addSpy = vi.fn().mockResolvedValue(undefined);
        mocks.firestore.collection.mockReturnValue({ add: addSpy });
        const doc = { D_624974556: 'Sibley Memorial Hospital', D_223569179: '2026-07-07T00:00:00.000Z', uid: 'u1' };
        await firestore.addSelfReportHCSUpdateDoc(doc);
        expect(mocks.firestore.collection).toHaveBeenCalledWith('selfReportHCSUpdates');
        expect(addSpy).toHaveBeenCalledWith(doc);
    });

    it('wraps and rethrows write failures', async () => {
        vi.spyOn(console, 'error').mockImplementation(() => {});
        mocks.firestore.collection.mockReturnValue({ add: vi.fn().mockRejectedValue(new Error('firestore down')) });
        await expect(firestore.addSelfReportHCSUpdateDoc({})).rejects.toThrow(/Write Self-Report HCS Update failed/);
    });
});

describe('getSelfReportHCSUpdateDocs', () => {
    it('queries by uid and returns the raw document data array', async () => {
        const rows = [
            { D_624974556: 'Older Facility', D_223569179: '2025-11-20T00:00:00.000Z' },
            { D_624974556: 'Newest Facility', D_223569179: '2026-07-06T00:00:00.000Z' },
        ];
        const whereSpy = vi.fn().mockReturnValue({
            get: vi.fn().mockResolvedValue({ size: rows.length, docs: rows.map((r) => ({ data: () => r })) }),
        });
        mocks.firestore.collection.mockReturnValue({ where: whereSpy });
        const result = await firestore.getSelfReportHCSUpdateDocs('uid-1');
        expect(mocks.firestore.collection).toHaveBeenCalledWith('selfReportHCSUpdates');
        expect(whereSpy).toHaveBeenCalledWith('uid', '==', 'uid-1');
        expect(result).toEqual(rows);
    });

    it('returns an empty array when the participant has no updates', async () => {
        mocks.firestore.collection.mockReturnValue({
            where: vi.fn().mockReturnValue({ get: vi.fn().mockResolvedValue({ size: 0, docs: [] }) }),
        });
        expect(await firestore.getSelfReportHCSUpdateDocs('uid-1')).toEqual([]);
    });
});
