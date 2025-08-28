import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { createConsumer } from '../src/consumer';
import { createProducer } from '../src/producer';
import { Redis } from 'ioredis';
import { sleep } from '../src/utils';

describe('consumer', () => {
  let redis: Redis;
  let producer: Awaited<ReturnType<typeof createProducer>>;
  const redisUrl = 'redis://localhost:6379/1';

  beforeEach(async () => {
    redis = new Redis(redisUrl);
    producer = await createProducer({ redisUrl, commandTimeout: 100 });
  });

  afterEach(async () => {
    await producer.stop();
    await redis.flushdb();
    await redis.quit();
  });

  describe('job processing', () => {
    it('processes a single job successfully', async () => {
      const queueName = 'process-queue';
      const jobData = { task: 'process-task', value: 42 };

      await producer.enqueue(queueName, jobData);

      const consumer = createConsumer({ redisUrl, queueName, waitTime: 1 });
      const processedJobs: any[] = [];

      const jobProcessor = async (job: any) => {
        processedJobs.push(job);
      };

      await consumer.runOnce(jobProcessor);
      await consumer.stop();

      expect(processedJobs).toHaveLength(1);
      expect(processedJobs[0]).toEqual(jobData);

      const remainingJobs = await redis.zcard(`xque:queue:${queueName}`);
      expect(remainingJobs).toBe(0);
    });

    it('processes multiple jobs in order', async () => {
      const queueName = 'multi-process-queue';

      await producer.enqueue(queueName, 'job-1', { priority: 1 });
      await producer.enqueue(queueName, 'job-2', { priority: 5 });
      await producer.enqueue(queueName, 'job-3', { priority: 3 });

      const consumer = createConsumer({ redisUrl, queueName, waitTime: 1 });
      const processedJobs: any[] = [];

      const jobProcessor = async (job: any) => {
        processedJobs.push(job);
      };

      await consumer.runOnce(jobProcessor);
      await consumer.runOnce(jobProcessor);
      await consumer.runOnce(jobProcessor);
      await consumer.stop();

      expect(processedJobs).toHaveLength(3);
      expect(processedJobs[0]).toBe('job-2');
      expect(processedJobs[1]).toBe('job-3');
      expect(processedJobs[2]).toBe('job-1');
    });

    it('waits for jobs when queue is empty', async () => {
      const consumer = createConsumer({ redisUrl, queueName: 'empty-wait-queue', waitTime: 1 });
      const processedJobs: any[] = [];

      const jobProcessor = async (job: any) => {
        processedJobs.push(job);
      };

      await consumer.runOnce(jobProcessor);
      await consumer.stop();

      expect(processedJobs).toHaveLength(0);
    });
  });

  describe('retry logic', () => {
    it('retries failed jobs with backoff', async () => {
      const queueName = 'retry-queue';
      const jobData = { task: 'failing-task' };

      await producer.enqueue(queueName, jobData);

      const consumer = createConsumer({
        redisUrl,
        queueName,
        retries: 2,
        backoffs: [50, 100],
        waitTime: 1,
        logger: { error: vi.fn() },
      });

      let attemptCount = 0;
      const processedJobs: any[] = [];

      const jobProcessor = async (job: any) => {
        attemptCount++;
        processedJobs.push(job);

        if (attemptCount <= 2) {
          throw new Error('Simulated failure');
        }
      };

      await consumer.runOnce(jobProcessor);
      await sleep(60);
      await consumer.runOnce(jobProcessor);
      await sleep(110);
      await consumer.runOnce(jobProcessor);
      await consumer.stop();

      expect(attemptCount).toBe(3);
      expect(processedJobs).toHaveLength(3);

      const remainingJobs = await redis.zcard(`xque:queue:${queueName}`);
      expect(remainingJobs).toBe(0);
    });

    it('deletes job after exceeding retry limit', async () => {
      const queueName = 'max-retry-queue';
      const jobData = { task: 'always-failing-task' };

      await producer.enqueue(queueName, jobData);

      const mockLogger = {
        error: vi.fn()
      };

      const consumer = createConsumer({
        redisUrl,
        queueName,
        retries: 2,
        backoffs: [50, 100],
        logger: mockLogger,
        waitTime: 1,
      });

      let attemptCount = 0;

      const jobProcessor = async (job: any) => {
        attemptCount++;
        throw new Error('Always fails');
      };

      await consumer.runOnce(jobProcessor);
      await sleep(60);
      await consumer.runOnce(jobProcessor);
      await sleep(110);
      await consumer.runOnce(jobProcessor);
      await consumer.stop();

      expect(attemptCount).toBe(3);
      expect(mockLogger.error).toHaveBeenCalledTimes(3);

      const remainingJobs = await redis.zcard(`xque:queue:${queueName}`);
      const pendingJobs = await redis.zcard(`xque:pending:${queueName}`);
      const storedJobs = await redis.hlen('xque:jobs');

      expect(remainingJobs).toBe(0);
      expect(pendingJobs).toBe(0);
      expect(storedJobs).toBe(0);
    });
  });

  describe('pending job handling', () => {
    it('processes expired pending jobs', async () => {
      const queueName = 'expired-pending-queue';
      const jobData = { task: 'expired-job' };
      const jobId = await producer.enqueue(queueName, jobData);

      const pastTime = Math.floor(Date.now() / 1_000) - 100;
      await redis.zrem(`xque:queue:${queueName}`, jobId);
      await redis.zadd(`xque:pending:${queueName}`, pastTime, jobId);

      const consumer = createConsumer({ redisUrl, queueName, retries: 3 });
      const processedJobs: any[] = [];

      const jobProcessor = async (job: any) => {
        processedJobs.push(job);
      };

      await consumer.runOnce(jobProcessor);
      await consumer.stop();

      expect(processedJobs).toHaveLength(1);
      expect(processedJobs[0]).toEqual(jobData);
    });

    it('does not process non-expired pending jobs', async () => {
      const queueName = 'future-pending-queue';
      const jobData = { task: 'future-job' };
      const jobId = await producer.enqueue(queueName, jobData);

      const futureTime = Math.floor(Date.now() / 1_000) + 3_600;
      await redis.zrem(`xque:queue:${queueName}`, jobId);
      await redis.zadd(`xque:pending:${queueName}`, futureTime, jobId);

      const consumer = createConsumer({ redisUrl, queueName, waitTime: 1 });
      const processedJobs: any[] = [];

      await consumer.runOnce(async (job: any) => {
        processedJobs.push(job);
      });

      await consumer.stop();

      expect(processedJobs).toHaveLength(0);
    });
  });

  describe('error handling', () => {
    it('continues processing after job processing errors', async () => {
      const queueName = 'continue-after-error-queue';

      await producer.enqueue(queueName, { task: 'job-1' });
      await producer.enqueue(queueName, { task: 'job-2' });

      const logger = {
        error: vi.fn()
      };

      const consumer = createConsumer({ redisUrl, queueName, retries: 0, logger });
      let jobCount = 0;
      const processedJobs: any[] = [];

      const jobProcessor = async (job: any) => {
        jobCount++;
        processedJobs.push(job);

        if (jobCount === 1) {
          throw new Error('First job fails');
        }
      };

      await consumer.runOnce(jobProcessor);
      await consumer.runOnce(jobProcessor);
      await consumer.stop();

      expect(processedJobs).toHaveLength(2);
      expect(logger.error).toHaveBeenCalledTimes(1);
    });
  });

  describe('stop functionality', () => {
    it('stops gracefully when called', async () => {
      const consumer = createConsumer({ redisUrl, queueName: 'stop-queue', retries: 3 });

      const runPromise = consumer.run(async (job: any) => {
        await sleep(100);
      });

      await sleep(50);

      const stopStart = Date.now();
      await consumer.stop();
      await runPromise;
      const stopDuration = Date.now() - stopStart;

      expect(stopDuration).toBeLessThan(200);
    });

    it('stops while waiting for jobs', async () => {
      const consumer = createConsumer({ redisUrl, queueName: 'stop-while-waiting-queue', retries: 3 });

      const runPromise = consumer.run(async (job: any) => {
        // Won't be called
      });

      await sleep(50);

      const stopStart = Date.now();
      await consumer.stop();
      await runPromise;
      const stopDuration = Date.now() - stopStart;

      expect(stopDuration).toBeLessThan(100);
    });
  });
});