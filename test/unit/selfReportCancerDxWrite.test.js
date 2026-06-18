/**
 * Drives the real firestore helper with the module-mock's runTransaction/doc (mirroring resetParticipantHelper.test.js).
 */
const { setupTestSuite } = require('../shared/testHelpers');

let mocks;
let firestore;
const DX_NUMBER_KEY = 'D_480939157';
const PROSTATE_DXDT_KEY = 'D_199928758';

beforeAll(() => {
    const mockSystem = setupTestSuite({ setupConsole: false, setupModuleMocks: true });
    mocks = mockSystem.mocks;
    firestore = require('../../utils/firestore');
});

// Wire db.collection().doc() -> a ref, and runTransaction -> a txn whose get() returns existing.
const wireGuarded = (existing) => {
    const setSpy = vi.fn();
    const ref = { __ref: 'doc-1' };
    mocks.firestore.collection.mockReturnValue({
        doc: vi.fn().mockReturnValue(ref),
        add: vi.fn().mockResolvedValue(undefined),
    });
    mocks.firestore.runTransaction.mockImplementation(async (cb) => cb({
        get: vi.fn().mockResolvedValue(
            existing ? { exists: true, data: () => existing } : { exists: false, data: () => null },
        ),
        set: setSpy,
    }));
    return { setSpy, ref };
};

describe('writeSelfReportCancerDxDoc: submitted-guard (save/submit race backstop)', () => {
    it('guardSubmitted skips the write when the target doc is already submitted (finalized)', async () => {
        const { setSpy } = wireGuarded({ [DX_NUMBER_KEY]: '1' });
        await firestore.writeSelfReportCancerDxDoc('doc-1', { D_181737942: '295976386' }, { guardSubmitted: true });
        expect(setSpy).not.toHaveBeenCalled();
    });

    it('guardSubmitted writes when the target doc is still in-progress', async () => {
        const { setSpy, ref } = wireGuarded({ STARTED_TS: '2026-06-01T00:00:00.000Z' });
        const data = { D_181737942: '295976386' };
        await firestore.writeSelfReportCancerDxDoc('doc-1', data, { guardSubmitted: true });
        expect(setSpy).toHaveBeenCalledWith(ref, data);
    });

    it('default path (unguarded) replaces with a plain set: no submitted-check, no transaction', async () => {
        const setSpy = vi.fn().mockResolvedValue(undefined);
        mocks.firestore.collection.mockReturnValue({ doc: vi.fn().mockReturnValue({ set: setSpy }) });
        await firestore.writeSelfReportCancerDxDoc('doc-1', { [DX_NUMBER_KEY]: '1' }, {});
        expect(setSpy).toHaveBeenCalled(); // ref.set ran directly (the transaction path was not taken)
    });

    it('creates (add) a new doc when docId is null, regardless of guardSubmitted', async () => {
        const addSpy = vi.fn().mockResolvedValue(undefined);
        mocks.firestore.collection.mockReturnValue({ doc: vi.fn(), add: addSpy });
        await firestore.writeSelfReportCancerDxDoc(null, { D_181737942: '295976386' }, { guardSubmitted: true });
        expect(addSpy).toHaveBeenCalled();
    });
});

describe('submitSelfReportCancerDxTransaction: atomic DxNumber', () => {
    // Wire collection().where() -> query, runTransaction -> a txn whose get() returns the participant's
    // docs, and collection().doc(id) -> a ref (id present => reuse that doc, omitted => a fresh ref).
    const wireSubmit = (diagnosisDocs) => {
        const setSpy = vi.fn();
        const newRef = { __ref: 'new-doc' };
        mocks.firestore.collection.mockReturnValue({
            where: vi.fn().mockReturnValue({ __query: true }),
            doc: vi.fn((id) => (id ? { __ref: id } : newRef)),
        });
        mocks.firestore.runTransaction.mockImplementation(async (cb) => cb({
            get: vi.fn().mockResolvedValue({ docs: diagnosisDocs.map((doc) => ({ id: doc.docId, data: () => doc.data })) }),
            set: setSpy,
        }));
        return { setSpy, newRef };
    };

    it('builds the finalized doc from docs read INSIDE the txn and reuses the in-progress doc', async () => {
        const { setSpy } = wireSubmit([
            { docId: 'ip', data: { STARTED_TS: 't0' } },
            { docId: 's1', data: { [DX_NUMBER_KEY]: '1', D_181737942: '295976386', [PROSTATE_DXDT_KEY]: '2026-06-01T00:00:00.000Z' } },
        ]);
        let seen;
        await firestore.submitSelfReportCancerDxTransaction('uid-1', (ctx) => { seen = ctx; return { ok: 1 }; });
        expect(seen.inProgressDoc.docId).toBe('ip');
        expect(seen.submittedDiagnoses).toEqual([{ [DX_NUMBER_KEY]: '1', D_181737942: '295976386', [PROSTATE_DXDT_KEY]: '2026-06-01T00:00:00.000Z' }]); // submitted diagnoses, read in-txn
        expect(setSpy).toHaveBeenCalledWith({ __ref: 'ip' }, { ok: 1 });                  // reused the in-progress doc
    });

    it('creates a new doc when there is no in-progress doc', async () => {
        const { setSpy, newRef } = wireSubmit([]);
        let seen;
        await firestore.submitSelfReportCancerDxTransaction('uid-1', (ctx) => { seen = ctx; return { ok: 2 }; });
        expect(seen.inProgressDoc).toBeNull();
        expect(seen.submittedDiagnoses).toEqual([]);
        expect(setSpy).toHaveBeenCalledWith(newRef, { ok: 2 });
    });
});
