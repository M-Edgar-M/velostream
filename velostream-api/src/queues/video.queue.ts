import { Queue } from 'bullmq';
import IORedis from 'ioredis';

export const redisConnection = new IORedis({
	host: 'localhost',
	port: 6380,
	maxRetriesPerRequest: null,
});

export const probeQueue = new Queue('video-probe', { connection: redisConnection });
