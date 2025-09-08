import { Redis, RedisOptions } from 'ioredis';
import { v7 as timeUuid } from 'uuid';
import { Job } from './types';

export async function createProducer({ redisConfig, commandTimeout = 1_000 }: { redisConfig: RedisOptions, commandTimeout?: number }) {
  const redis = new Redis({ commandTimeout, ...redisConfig });

  async function enqueue(queueName: string, data: any, { expiry = 3_600_000, priority = 0 }: { expiry?: number, priority?: number } = {}) {
    const job: Job = { jid: timeUuid(), expiry, data };

    const enqueueScript = `
      local queue_name, jid, job, priority = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[4])

      redis.call('hset', 'xque:jobs', jid, job)

      local sequence_number = redis.call('incr', 'xque:seq:' .. queue_name)
      local score = -priority * (2^50) + sequence_number

      redis.call('zadd', 'xque:queue:' .. queue_name, score, jid)
      redis.call('lpush', 'xque:notifications:' .. queue_name, 1)
    `;

    await redis.eval(enqueueScript, 0, [queueName, job.jid, JSON.stringify(job), priority.toString()]);

    return job.jid;
  }

  async function queueSize(queueName: string) {
    return await redis.zcard(`xque:queue:${queueName}`);
  }

  async function pendingSize(queueName: string) {
    return await redis.zcard(`xque:pending:${queueName}`);
  }

  async function size(queueName: string) {
    return (await queueSize(queueName)) + (await pendingSize(queueName));
  }

  async function findJob(jid: string) {
    const job = await redis.hget('xque:jobs', jid);

    return job ? JSON.parse(job) : null;
  }

  async function pendingTime(queueName: string, jid: string) {
    const pendingTimeScript = `
      local queue_name, jid = ARGV[1], ARGV[2]

      return { redis.call('time')[1], redis.call('zscore', 'xque:pending:' .. queue_name, jid) }
    `;

    const [time, score] = await redis.eval(pendingTimeScript, 0, [queueName, jid]) as [number, string | null];

    if (time === null || score === null) return null;

    return parseInt(score, 10) - time;
  }

  async function zscanKey(key: string, fn: (job: Job) => Promise<void>) {
    let cursor = '0';

    do {
      const [newCursor, items] = await redis.zscan(key, cursor, 'COUNT', 100);
      cursor = newCursor;

      for (const item of items) {
        const job = await redis.hget('xque:jobs', item);
        if (!job) continue;

        await fn(JSON.parse(job) as Job);
      }
    } while (cursor !== '0');
  }

  async function scanQueue(queueName: string, fn: (job: Job) => Promise<void>) {
    await zscanKey(`xque:queue:${queueName}`, fn);
  }

  async function scanPending(queueName: string, fn: (job: Job) => Promise<void>) {
    await zscanKey(`xque:pending:${queueName}`, fn);
  }

  async function scan(queueName: string, fn: (job: Job) => Promise<void>) {
    await scanPending(queueName, fn);
    await scanQueue(queueName, fn);
  }

  async function stop() {
    await redis.quit();
  }

  return { enqueue, size, queueSize, pendingSize, findJob, pendingTime, scan, scanQueue, scanPending, stop };
}