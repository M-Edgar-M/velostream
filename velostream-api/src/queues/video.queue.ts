import { Queue } from 'bullmq';
import IORedis from 'ioredis';

export const redisConnection = new IORedis(process.env.REDIS_URL || 'redis://localhost:6379', {
	maxRetriesPerRequest: null,
});

export const probeQueue = new Queue('video-probe', { connection: redisConnection });
