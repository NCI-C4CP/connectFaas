const { expect } = require('chai');
const sinon = require('sinon');
const { SmsBatchSender } = require('../../utils/notifications');

let recordIdCounter = 0;
const makeSmsRecord = (specId, language = 'english', token = 'token-1') => ({
  id: `record-${++recordIdCounter}`,
  notificationSpecificationsID: specId,
  language,
  token,
  phone: '+15551234567',
  notification: {title: "Message Title", body: 'This is a test message.', time: new Date().toISOString()},
});

const successResult = (smsRecord) => ({
  smsRecord: { ...smsRecord, messageSid: 'SM_fake_sid' },
  isSuccess: true,
  isRateLimit: false,
});

const failResult = (smsRecord) => ({
  smsRecord,
  isSuccess: false,
  isRateLimit: false,
});

const rateLimitResult = (smsRecord) => ({
  smsRecord,
  isSuccess: false,
  isRateLimit: true,
});

describe('SmsBatchSender', () => {
  let sendFn;
  let saveFn;
  let delayFn;

  const createSender = (opts = {}) =>
    new SmsBatchSender({
      batchSize: opts.batchSize ?? 10,
      maxRetries: opts.maxRetries,
      sendFn,
      saveFn,
      delayFn,
    });

  beforeEach(() => {
    sendFn = sinon.stub();
    saveFn = sinon.stub().resolves();
    delayFn = sinon.stub().resolves();
  });

  afterEach(() => {
    sinon.restore();
  });

  describe('getSentCounts / getFailedCounts', () => {
    it('should return zero counts for unknown specId', () => {
      const sender = createSender();
      expect(sender.getSentCounts('unknown')).to.deep.equal({ english: 0, spanish: 0 });
      expect(sender.getFailedCounts('unknown')).to.deep.equal({ english: 0, spanish: 0 });
    });

    it('should return a copy so callers cannot mutate internal state', () => {
      const sender = createSender();
      const counts = sender.getSentCounts('spec1');
      counts.english = 999;
      expect(sender.getSentCounts('spec1')).to.deep.equal({ english: 0, spanish: 0 });
    });
  });

  describe('isSpecFinished', () => {
    it('should return false for unknown specId', () => {
      const sender = createSender();
      expect(sender.isSpecFinished('spec1')).to.be.false;
    });
  });

  describe('basic send flow', () => {
    it('should send all records and track sent counts', async () => {
      sendFn.callsFake((record) => Promise.resolve(successResult(record)));

      const sender = createSender();
      const records = [
        makeSmsRecord('spec1', 'english'),
        makeSmsRecord('spec1', 'spanish'),
        makeSmsRecord('spec1', 'english'),
      ];
      sender.addToQueue(records);
      sender.markSpecEnd('spec1');

      const result = await sender.waitForSpec('spec1');
      expect(result.sentCounts).to.deep.equal({ english: 2, spanish: 1 });
      expect(result.failedCounts).to.deep.equal({ english: 0, spanish: 0 });
      expect(sendFn.callCount).to.equal(3);
      expect(saveFn.callCount).to.equal(1);
      expect(saveFn.firstCall.args[0]).to.have.lengthOf(3);
    });

    it('should handle an empty queue with only an end marker', async () => {
      const sender = createSender();
      sender.markSpecEnd('spec1');

      const result = await sender.waitForSpec('spec1');
      expect(result.sentCounts).to.deep.equal({ english: 0, spanish: 0 });
      expect(result.failedCounts).to.deep.equal({ english: 0, spanish: 0 });
      expect(sendFn.callCount).to.equal(0);
      expect(saveFn.callCount).to.equal(0);
    });
  });

  describe('failure handling', () => {
    it('should track failed counts for non-rate-limit errors', async () => {
      sendFn.callsFake((record) => Promise.resolve(failResult(record)));

      const sender = createSender();
      sender.addToQueue([
        makeSmsRecord('spec1', 'english'),
        makeSmsRecord('spec1', 'spanish'),
      ]);
      sender.markSpecEnd('spec1');

      const result = await sender.waitForSpec('spec1');
      expect(result.sentCounts).to.deep.equal({ english: 0, spanish: 0 });
      expect(result.failedCounts).to.deep.equal({ english: 1, spanish: 1 });
      expect(saveFn.callCount).to.equal(0);
    });

    it('should not increment sent counts when saveNotificationBatch throws', async () => {
      sendFn.callsFake((record) => Promise.resolve(successResult(record)));
      saveFn.rejects(new Error('Firestore write failed'));

      const consoleStub = sinon.stub(console, 'error');
      const sender = createSender();
      sender.addToQueue([makeSmsRecord('spec1', 'english')]);
      sender.markSpecEnd('spec1');

      const result = await sender.waitForSpec('spec1');
      expect(result.sentCounts).to.deep.equal({ english: 0, spanish: 0 });
      expect(result.failedCounts).to.deep.equal({ english: 0, spanish: 0 });
      expect(consoleStub.calledOnce).to.be.true;
      consoleStub.restore();
    });
  });

  describe('rate limit retry', () => {
    it('should re-queue rate-limited records and retry them', async () => {
      const record = makeSmsRecord('spec1', 'english');
      let callCount = 0;
      sendFn.callsFake((r) => {
        callCount++;
        if (callCount === 1) return Promise.resolve(rateLimitResult(r));
        return Promise.resolve(successResult(r));
      });

      const sender = createSender();
      sender.addToQueue([record]);
      sender.markSpecEnd('spec1');

      const result = await sender.waitForSpec('spec1');
      expect(result.sentCounts).to.deep.equal({ english: 1, spanish: 0 });
      expect(sendFn.callCount).to.equal(2);
    });

    it('should count as failed after exceeding maxRetries', async () => {
      sendFn.callsFake((r) => Promise.resolve(rateLimitResult(r)));
      const consoleStub = sinon.stub(console, 'error');

      const sender = createSender({ batchSize: 10, maxRetries: 2 });
      sender.addToQueue([makeSmsRecord('spec1', 'english')]);
      sender.markSpecEnd('spec1');

      const result = await sender.waitForSpec('spec1');
      expect(result.sentCounts).to.deep.equal({ english: 0, spanish: 0 });
      expect(result.failedCounts).to.deep.equal({ english: 1, spanish: 0 });
      // 1 initial + 2 retries = 3 total send attempts
      expect(sendFn.callCount).to.equal(3);
      consoleStub.restore();
    });

    it('should succeed on retry within maxRetries limit', async () => {
      let callCount = 0;
      sendFn.callsFake((r) => {
        callCount++;
        if (callCount <= 2) return Promise.resolve(rateLimitResult(r));
        return Promise.resolve(successResult(r));
      });

      const sender = createSender({ batchSize: 10, maxRetries: 3 });
      sender.addToQueue([makeSmsRecord('spec1', 'english')]);
      sender.markSpecEnd('spec1');

      const result = await sender.waitForSpec('spec1');
      expect(result.sentCounts).to.deep.equal({ english: 1, spanish: 0 });
      expect(result.failedCounts).to.deep.equal({ english: 0, spanish: 0 });
    });

    it('should preserve end marker when rate-limited records are re-queued', async () => {
      let callCount = 0;
      sendFn.callsFake((r) => {
        callCount++;
        if (callCount <= 2) return Promise.resolve(rateLimitResult(r));
        return Promise.resolve(successResult(r));
      });

      const sender = createSender();
      sender.addToQueue([
        makeSmsRecord('spec1', 'english'),
        makeSmsRecord('spec1', 'spanish'),
      ]);
      sender.markSpecEnd('spec1');

      const result = await sender.waitForSpec('spec1');
      expect(result.sentCounts).to.deep.equal({ english: 1, spanish: 1 });
      expect(sender.isSpecFinished('spec1')).to.be.true;
    });
  });

  describe('mixed results', () => {
    it('should handle a mix of success, failure, and rate-limit in one batch', async () => {
      const records = [
        makeSmsRecord('spec1', 'english', 'token-success'),
        makeSmsRecord('spec1', 'spanish', 'token-fail'),
        makeSmsRecord('spec1', 'english', 'token-ratelimit'),
      ];

      let rateLimitRetried = false;
      sendFn.callsFake((r) => {
        if (r.token === 'token-success') return Promise.resolve(successResult(r));
        if (r.token === 'token-fail') return Promise.resolve(failResult(r));
        if (r.token === 'token-ratelimit') {
          if (!rateLimitRetried) {
            rateLimitRetried = true;
            return Promise.resolve(rateLimitResult(r));
          }
          return Promise.resolve(successResult(r));
        }
      });

      const sender = createSender();
      sender.addToQueue(records);
      sender.markSpecEnd('spec1');

      const result = await sender.waitForSpec('spec1');
      expect(result.sentCounts).to.deep.equal({ english: 2, spanish: 0 });
      expect(result.failedCounts).to.deep.equal({ english: 0, spanish: 1 });
    });
  });

  describe('batching', () => {
    it('should respect batchSize and process in multiple batches', async () => {
      sendFn.callsFake((r) => Promise.resolve(successResult(r)));

      const sender = createSender({ batchSize: 2 });
      const records = [
        makeSmsRecord('spec1', 'english', 'a'),
        makeSmsRecord('spec1', 'english', 'b'),
        makeSmsRecord('spec1', 'english', 'c'),
      ];
      sender.addToQueue(records);
      sender.markSpecEnd('spec1');

      const result = await sender.waitForSpec('spec1');
      expect(result.sentCounts).to.deep.equal({ english: 3, spanish: 0 });
      // saveFn called once per batch that has successful records
      expect(saveFn.callCount).to.equal(2);
    });
  });

  describe('multiple specs', () => {
    it('should track counts independently per specId', async () => {
      sendFn.callsFake((r) => {
        if (r.notificationSpecificationsID === 'spec2') return Promise.resolve(failResult(r));
        return Promise.resolve(successResult(r));
      });

      const sender = createSender();
      sender.addToQueue([
        makeSmsRecord('spec1', 'english'),
        makeSmsRecord('spec2', 'english'),
        makeSmsRecord('spec1', 'spanish'),
      ]);
      sender.markSpecEnd('spec1');
      sender.markSpecEnd('spec2');

      const [result1, result2] = await Promise.all([
        sender.waitForSpec('spec1'),
        sender.waitForSpec('spec2'),
      ]);

      expect(result1.sentCounts).to.deep.equal({ english: 1, spanish: 1 });
      expect(result1.failedCounts).to.deep.equal({ english: 0, spanish: 0 });
      expect(result2.sentCounts).to.deep.equal({ english: 0, spanish: 0 });
      expect(result2.failedCounts).to.deep.equal({ english: 1, spanish: 0 });
    });

    it('should finish specs independently', async () => {
      sendFn.callsFake((r) => Promise.resolve(successResult(r)));

      const sender = createSender();
      sender.addToQueue([makeSmsRecord('spec1', 'english')]);
      sender.markSpecEnd('spec1');

      await sender.waitForSpec('spec1');
      expect(sender.isSpecFinished('spec1')).to.be.true;
      expect(sender.isSpecFinished('spec2')).to.be.false;
    });
  });

  describe('progress logging', () => {
    let clock;
    let consoleLogStub;

    beforeEach(() => {
      clock = sinon.useFakeTimers({ now: Date.now() });
      consoleLogStub = sinon.stub(console, 'log');
    });

    afterEach(() => {
      clock.restore();
      consoleLogStub.restore();
    });

    it('should log progress for in-flight specs after 30 seconds', async () => {
      sendFn.callsFake((r) => Promise.resolve(successResult(r)));

      const sender = createSender({ batchSize: 1 });
      sender.addToQueue([
        makeSmsRecord('spec1', 'english', 'a'),
        makeSmsRecord('spec1', 'english', 'b'),
      ]);
      sender.markSpecEnd('spec1');

      clock.tick(31000);
      await sender.waitForSpec('spec1');

      const progressLogs = consoleLogStub.args.filter((args) => args[0]?.includes?.('SMS in progress'));
      expect(progressLogs.length).to.be.greaterThan(0);
      expect(progressLogs[0][0]).to.include('spec1');
      expect(progressLogs[0][0]).to.include('sent');
      expect(progressLogs[0][0]).to.include('failed');
    });

    it('should skip finished specs in progress log', async () => {
      sendFn.callsFake((r) => Promise.resolve(successResult(r)));

      const sender = createSender();
      sender.addToQueue([makeSmsRecord('spec1', 'english')]);
      sender.markSpecEnd('spec1');

      // spec1 finishes, then start spec2 after 30s
      await sender.waitForSpec('spec1');
      clock.tick(31000);

      sender.addToQueue([makeSmsRecord('spec2', 'english')]);
      sender.markSpecEnd('spec2');
      await sender.waitForSpec('spec2');

      const progressLogs = consoleLogStub.args.filter((args) => args[0]?.includes?.('SMS in progress'));
      const spec1Logs = progressLogs.filter((args) => args[0].includes('spec1'));
      expect(spec1Logs.length).to.equal(0);
    });
  });

  describe('delay behavior', () => {
    it('should call delayFn for rate limiting between batches', async () => {
      sendFn.callsFake((r) => Promise.resolve(successResult(r)));

      const sender = createSender({ batchSize: 1 });
      sender.addToQueue([
        makeSmsRecord('spec1', 'english', 'a'),
        makeSmsRecord('spec1', 'english', 'b'),
      ]);
      sender.markSpecEnd('spec1');

      await sender.waitForSpec('spec1');
      expect(delayFn.called).to.be.true;
    });
  });
});
