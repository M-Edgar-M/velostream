import { Queue } from 'bullmq';
import IORedis from 'ioredis';

export const redisConnection = new IORedis(process.env.REDIS_URL || 'redis://localhost:6379', {
	maxRetriesPerRequest: null,
});

redisConnection.on('connect', () => console.log('[Redis] Connected successfully'));
redisConnection.on('error', (err) => console.error('[Redis] Connection error:', err));
redisConnection.on('close', () => console.warn('[Redis] Connection closed'));

export const probeQueue = new Queue('video-probe', { connection: redisConnection });
export const transcodeQueue = new Queue('video-transcode', { connection: redisConnection });
