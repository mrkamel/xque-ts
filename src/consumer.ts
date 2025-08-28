import { Redis } from 'ioredis';
import { Job, Logger } from './types';
import { sleep } from './utils';

const SLEEP_INTERVAL = 5_000;
const DEFAULT_BACKOFF = 60_000;
const COMMAND_TIMEOUT = 30_000;

export function createConsumer(
  { redisUrl, queueName, waitTime = 5_000, retries = 3, backoffs = [30_000, 90_000, 270_000], logger = console }:
  { redisUrl: string; queueName: string, waitTime?: number, retries?: number, backoffs?: number[], logger?: Logger }
) {
  const redis = new Redis(redisUrl, { commandTimeout: COMMAND_TIMEOUT });
  let resolveStopPromise: (value: boolean | PromiseLike<boolean>) => void;
  let stopped = false;

  const stopPromise = new Promise((resolve) => {
    resolveStopPromise = resolve;
  });

  async function brpopNotification() {
    const blockingRedis = new Redis(redisUrl, { commandTimeout: COMMAND_TIMEOUT });
    await blockingRedis.brpop(`xque:notifications:${queueName}`, waitTime / 1_000);
    blockingRedis.disconnect();
  }

  async function waitForNotification() {
    await Promise.any([stopPromise, brpopNotification()]);
  }

  async function dequeue(): Promise<string | null> {
    const dequeueScript = `
      local queue_name = ARGV[1]

      local zitem = redis.call('zrange', 'xque:pending:' .. queue_name, 0, 0, 'WITHSCORES')
      local job_id = zitem[1]

      local time = redis.call('time')
      local time_float = tonumber(time[1]) + (tonumber(time[2]) / 1e6)

      if not zitem[2] or tonumber(zitem[2]) > time_float then
        job_id = redis.call('zpopmin', 'xque:queue:' .. queue_name)[1]
      end

      if not job_id then return nil end

      local job = redis.call('hget', 'xque:jobs', job_id)

      if not job then return nil end

      local object = cjson.decode(job)

      time = redis.call('time')
      time_float = tonumber(time[1]) + (tonumber(time[2]) / 1e6)

      redis.call('zadd', 'xque:pending:' .. queue_name, time_float + (object['expiry'] / 1e3), job_id)

      return job
    `;

    return await redis.eval(dequeueScript, 0, [queueName]) as string | null;
  }

  async function deleteJob(parsedJob: Job) {
    const deleteScript = `
      local queue_name, job_id = ARGV[1], ARGV[2]

      redis.call('hdel', 'xque:jobs', job_id)
      redis.call('zrem', 'xque:pending:' .. queue_name, job_id)
    `;

    return await redis.eval(deleteScript, 0, [queueName, parsedJob.jid]); 
  }

  async function backoffJob(parsedJob: Job) {
    const errors = (parsedJob.errors || 0) + 1;

    if (errors > retries) {
      await deleteJob(parsedJob);

      return;
    }

    const backoff = backoffs[errors - 1] || backoffs[backoffs.length - 1] || DEFAULT_BACKOFF;
    const updatedJob = JSON.stringify({ ...parsedJob, errors });

    const backoffScript = `
      local queue_name, job_id, job, backoff = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[4])

      local time = redis.call('time')
      local time_float = tonumber(time[1]) + (tonumber(time[2]) / 1e6)

      redis.call('hset', 'xque:jobs', job_id, job)
      redis.call('zrem', 'xque:pending:' .. queue_name, job_id)
      redis.call('zadd', 'xque:pending:' .. queue_name, time_float + (backoff / 1e3), job_id)
    `;

    return await redis.eval(backoffScript, 0, [queueName, parsedJob.jid, updatedJob, backoff.toString()]);
  }

  async function executeJob(job: string, fn: (job: Job) => Promise<void>) {
    const parsedJob = JSON.parse(job);

    try {
      await fn(parsedJob.data);
      await deleteJob(parsedJob);
    } catch (error) {
      logger.error('Job execution error', error);

      await backoffJob(parsedJob);
    }
  }

  async function runOnce(fn: (job: Job) => Promise<void>) {
    try {
      const job = await dequeue();

      if (!job) {
        await waitForNotification();
        return;
      }

      await executeJob(job, fn);
    } catch (error) {
      logger.error('Queue processing error', error);

      await Promise.any([stopPromise, sleep(SLEEP_INTERVAL)]);
    }
  }

  async function run(fn: (job: Job) => Promise<void>) {
    while (!stopped) {
      await runOnce(fn);
    }
  }

  async function stop() {
    stopped = true;
    resolveStopPromise(true);

    await stopPromise;
    await redis.quit();
  }

  return { run, runOnce, stop };
}