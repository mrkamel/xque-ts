import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createProducer } from '../src/producer';
import { Redis } from 'ioredis';

describe('producer', () => {
  let producer: Awaited<ReturnType<typeof createProducer>>;
  let redis: Redis;

  beforeEach(async () => {
    const redisUrl = 'redis://localhost:6379/1';

    producer = await createProducer({ redisUrl });
    redis = new Redis(redisUrl);
  });

  afterEach(async () => {
    await producer.stop();
    await redis.flushdb();
    await redis.quit();
  });

  describe('enqueue', () => {
    it('enqueues a job with default options', async () => {
      const queueName = 'test-queue';
      const jobData = { task: 'test-task', value: 42 };
      const jobId = await producer.enqueue(queueName, jobData);

      expect(jobId).toBeDefined();
      expect(typeof jobId).toBe('string');

      const storedJob = await redis.hget('xque:jobs', jobId);
      expect(storedJob).toBeDefined();

      const parsedJob = JSON.parse(storedJob!);
      expect(parsedJob.jid).toBe(jobId);
      expect(parsedJob.data).toEqual(jobData);
      expect(parsedJob.expiry).toBe(3_600_000);

      const queueSize = await redis.zcard(`xque:queue:${queueName}`);
      expect(queueSize).toBe(1);
    });

    it('enqueues a job with custom expiry and priority', async () => {
      const queueName = 'test-queue-custom';
      const jobData = { task: 'high-priority-task' };
      const jobId = await producer.enqueue(queueName, jobData, { expiry: 7200, priority: 10 });

      const storedJob = await redis.hget('xque:jobs', jobId);
      const parsedJob = JSON.parse(storedJob!);

      expect(parsedJob.expiry).toBe(7200);
      expect(parsedJob.data).toEqual(jobData);
    });

    it('maintains priority order in queue', async () => {
      const queueName = 'priority-queue';
      const lowPriorityJobId = await producer.enqueue(queueName, { task: 'low' }, { priority: 1 });
      const highPriorityJobId = await producer.enqueue(queueName, { task: 'high' }, { priority: 10 });
      const mediumPriorityJobId = await producer.enqueue(queueName, { task: 'medium' }, { priority: 5 });

      const queueItems = await redis.zrange(`xque:queue:${queueName}`, 0, -1);

      expect(queueItems[0]).toBe(highPriorityJobId);
      expect(queueItems[1]).toBe(mediumPriorityJobId);
      expect(queueItems[2]).toBe(lowPriorityJobId);
    });

    it('maintains FIFO order for jobs with same priority', async () => {
      const queueName = 'fifo-queue';
      const firstJobId = await producer.enqueue(queueName, { task: 'first' }, { priority: 5 });
      const secondJobId = await producer.enqueue(queueName, { task: 'second' }, { priority: 5 });
      const thirdJobId = await producer.enqueue(queueName, { task: 'third' }, { priority: 5 });

      const queueItems = await redis.zrange(`xque:queue:${queueName}`, 0, -1);

      expect(queueItems[0]).toBe(firstJobId);
      expect(queueItems[1]).toBe(secondJobId);
      expect(queueItems[2]).toBe(thirdJobId);
    });
  });

  describe('queue size operations', () => {
    it('returns correct queue size', async () => {
      const queueName = 'size-queue';

      expect(await producer.queueSize(queueName)).toBe(0);

      await producer.enqueue(queueName, { task: 'task1' });
      expect(await producer.queueSize(queueName)).toBe(1);

      await producer.enqueue(queueName, { task: 'task2' });
      expect(await producer.queueSize(queueName)).toBe(2);
    });

    it('returns correct pending size', async () => {
      const queueName = 'pending-queue';

      expect(await producer.pendingSize(queueName)).toBe(0);

      await redis.zadd(`xque:pending:${queueName}`, Date.now() + 1000, 'test-job-id');
      expect(await producer.pendingSize(queueName)).toBe(1);
    });

    it('returns correct total size', async () => {
      const queueName = 'total-size-queue';

      await producer.enqueue(queueName, { task: 'queued' });
      await redis.zadd(`xque:pending:${queueName}`, Date.now() + 1000, 'pending-job-id');

      expect(await producer.size(queueName)).toBe(2);
    });
  });

  describe('job operations', () => {
    it('finds an existing job', async () => {
      const queueName = 'find-queue';
      const jobData = { task: 'findable-task', id: 123 };
      const jobId = await producer.enqueue(queueName, jobData);

      const foundJob = await producer.findJob(queueName, jobId);
      expect(foundJob).toBeDefined();
      expect(foundJob!.jid).toBe(jobId);
      expect(foundJob!.data).toEqual(jobData);
    });

    it('returns null for non-existent job', async () => {
      const queueName = 'empty-queue';
      const foundJob = await producer.findJob(queueName, 'non-existent-id');
      expect(foundJob).toBeNull();
    });

    it('calculates pending time correctly', async () => {
      const queueName = 'pending-time-queue';
      const currentTime = Math.floor(Date.now() / 1000);
      const futureTime = currentTime + 300;

      await redis.zadd(`xque:pending:${queueName}`, futureTime, 'test-job-id');

      const pendingTime = await producer.pendingTime(queueName, 'test-job-id');
      expect(pendingTime).toBeGreaterThan(250);
      expect(pendingTime).toBeLessThan(350);
    });

    it('returns null for pending time of non-pending job', async () => {
      const queueName = 'no-pending-queue';
      const pendingTime = await producer.pendingTime(queueName, 'non-existent-job');
      expect(pendingTime).toBeNull();
    });
  });

  describe('scanning operations', () => {
    it('scans all jobs in queue', async () => {
      const queueName = 'scan-queue';
      const jobData1 = { task: 'scan-task-1' };
      const jobData2 = { task: 'scan-task-2' };

      await producer.enqueue(queueName, jobData1);
      await producer.enqueue(queueName, jobData2);

      const scannedJobs: any[] = [];

      await producer.scanQueue(queueName, async (job) => {
        scannedJobs.push(job);
      });

      expect(scannedJobs).toHaveLength(2);
      expect(scannedJobs.map(j => j.data)).toContainEqual(jobData1);
      expect(scannedJobs.map(j => j.data)).toContainEqual(jobData2);
    });

    it('scans pending jobs', async () => {
      const queueName = 'scan-pending-queue';
      const jobData = { task: 'pending-scan-task' };
      const jobId = await producer.enqueue(queueName, jobData);

      await redis.zrem(`xque:queue:${queueName}`, jobId);
      await redis.zadd(`xque:pending:${queueName}`, Date.now() + 1000, jobId);

      const scannedJobs: any[] = [];

      await producer.scanPending(queueName, async (job) => {
        scannedJobs.push(job);
      });

      expect(scannedJobs).toHaveLength(1);
      expect(scannedJobs[0].data).toEqual(jobData);
    });

    it('scans all jobs (queue + pending)', async () => {
      const queueName = 'scan-all-queue';
      const queuedJobData = { task: 'queued-job' };
      const pendingJobData = { task: 'pending-job' };

      await producer.enqueue(queueName, queuedJobData);
      const pendingJobId = await producer.enqueue(queueName, pendingJobData);

      await redis.zrem(`xque:queue:${queueName}`, pendingJobId);
      await redis.zadd(`xque:pending:${queueName}`, Date.now() + 1000, pendingJobId);

      const scannedJobs: any[] = [];

      await producer.scan(queueName, async (job) => {
        scannedJobs.push(job);
      });

      expect(scannedJobs).toHaveLength(2);
      const taskNames = scannedJobs.map(j => j.data.task);
      expect(taskNames).toContain('queued-job');
      expect(taskNames).toContain('pending-job');
    });
  });

  describe('error handling', () => {
    it('handles invalid queue names', async () => {
      const result = await producer.enqueue('', { task: 'test' });
      expect(result).toBeDefined();
    });
  });
});