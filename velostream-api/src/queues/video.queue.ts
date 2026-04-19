import { Queue } from 'bullmq';
import IORedis from 'ioredis';

const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';

export const redisConnection = new IORedis(redisUrl, {
	maxRetriesPerRequest: null,
});

redisConnection.on('connect', () => console.log('[Redis] Connected successfully'));
redisConnection.on('error', (err) => console.error('[Redis] Connection error:', err));
redisConnection.on('close', () => console.warn('[Redis] Connection closed'));

export const probeQueue = new Queue('video-probe', { connection: redisConnection });
export const transcodeQueue = new Queue('video-transcode', { connection: redisConnection });

let transcodeQueueListenersAttached = false;

function attachTranscodeQueueListeners(queue: Queue): void {
	if (transcodeQueueListenersAttached) {
		return;
	}
	transcodeQueueListenersAttached = true;

	queue.on('error', (err) => {
		console.error('[video-transcode queue] BullMQ error:', err);
	});

	queue.on('waiting', (job) => {
		console.log('[video-transcode queue] waiting:', { jobId: job.id, name: job.name });
	});

	queue.on('removed', (jobId) => {
		console.warn('[video-transcode queue] job removed:', jobId);
	});
}

attachTranscodeQueueListeners(transcodeQueue);

/**
 * Call once at API startup after module load. Uses the same IORedis instance as BullMQ.
 */
export async function verifyRedisConnection(): Promise<boolean> {
	try {
		const pong = await redisConnection.ping();
		if (pong !== 'PONG') {
			console.error('REDIS CONNECTION FAILED: unexpected PING response', pong);
			return false;
		}
		console.log('[Redis] PING ok', { redisUrl });
		return true;
	} catch (err) {
		console.error('REDIS CONNECTION FAILED', err);
		return false;
	}
}
