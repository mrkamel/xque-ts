# xque

A redis-based job queue library written in typescript that supports
retries, job priorities and expiration.

## Usage

Create a producer and enqueue jobs:

```js
import { createProducer } from 'xque';

const producer = await createProducer({ redisUrl: 'redis://localhost:6379' });

const jobId = await producer.enqueue('myQueue', { message: 'hello' }, { expiry: 3600, priority: 1 });
const jobId2 = await producer.enqueue('myQueue', { message: 'world' }, { expiry: 7200, priority: 2 });

await producer.stop();
```

Create a consumer and consume jobs from a queue:

```js
import { createConsumer } from 'xque';

const consumer = await createConsumer({ redisUrl: 'redis://localhost:6379', queueName: 'myQueue' });

process.on('SIGTERM', () => {
  consumer.stop();
});

await consumer.run(async (job) => {
  console.log('Processing job', job);
});
```

## Semantic Versioning

Topscript is using Semantic Versioning: [SemVer](http://semver.org/)

## Contributing

Bug reports and pull requests are welcome on GitHub at
https://github.com/mrkamel/xque-ts

## License

The library is available as open source under the terms of the 
[MIT License](https://opensource.org/licenses/MIT).
