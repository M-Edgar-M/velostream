import { FastifyInstance } from 'fastify';
import { S3Client, PutObjectCommand, HeadObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import type { PrismaClient } from '@prisma/client';
import { transcodeQueue } from '../queues/video.queue';

// Internal client — container-to-container, used to build URLs the Rust worker can reach
const s3Client = new S3Client({
  endpoint: process.env.MINIO_ENDPOINT || "http://localhost:9000",
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || 'minioadmin',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || 'minioadmin'
  },
  region: process.env.AWS_REGION || 'us-east-1',
  forcePathStyle: true,
});

// Public client — browser-facing presigned URLs use the host-visible address
const presignClient = new S3Client({
  endpoint: process.env.MINIO_PUBLIC_URL || process.env.MINIO_ENDPOINT || "http://localhost:9000",
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || 'minioadmin',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || 'minioadmin'
  },
  region: process.env.AWS_REGION || 'us-east-1',
  forcePathStyle: true,
});

export async function uploadRoutes(fastify: FastifyInstance) {
  const prisma = (fastify as unknown as { prisma: PrismaClient }).prisma;

  /**
   * 1. Initiate Upload
   * Validates user quota and returns a Presigned URL for direct-to-S3 upload.
   */
  fastify.post('/uploads/initiate', async (request, reply) => {
    const { fileName, fileSize, contentType, userId } = request.body as {
      fileName: string;
      fileSize: number;
      contentType?: string;
      userId: string;
    };

    request.log.info({ userId, fileName, fileSize }, 'initiate upload');

    // 1. Quota Check (Multi-tenancy extension)
    const user = await prisma.user.findUnique({ where: { id: userId } });
    if (!user) return reply.status(404).send({ error: "User not found" });

    // Use BigInt for large file calculations (JS number limit is 2GB for some operations)
    const currentUsage = user.currentUsageBytes || 0n;
    const quotaBytes = user.storageQuotaBytes || 0n;

    if (currentUsage + BigInt(fileSize) > quotaBytes) {
      return reply.status(403).send({ error: "Quota exceeded. Please clear space or upgrade." });
    }

    const video = await prisma.video.create({
      data: {
        userId,
        originalFileName: fileName,
        status: 'PENDING',
        storageKey: `uploads/${userId}/${Date.now()}-${fileName}`,
      }
    });

    // Do not enqueue transcode here: the object does not exist until the client finishes the PUT.
    // Call POST /uploads/:videoId/ack after upload (or rely on the MinIO webhook).

    // 2. Generate Presigned URL
    const command = new PutObjectCommand({
      Bucket: 'velostream-uploads',
      Key: video.storageKey,
      ContentType: contentType || 'video/mp4', // Required to match the exact client upload
    });

    const uploadUrl = await getSignedUrl(presignClient, command, { expiresIn: 3600 });

    return { uploadUrl, videoId: video.id };
  });

  /**
   * After a successful direct-to-S3 PUT, the client calls this to start the transcode pipeline.
   * Confirms the object exists, then enqueues directly to the transcode queue so the
   * Rust worker can pick it up immediately — no intermediate probe step required.
   */
  fastify.post('/uploads/:videoId/ack', async (request, reply) => {
    const { videoId } = request.params as { videoId: string };
    const { userId } = (request.body as { userId?: string }) || {};

    if (!userId) {
      return reply.status(400).send({ error: 'userId is required' });
    }

    const video = await prisma.video.findUnique({ where: { id: videoId } });
    if (!video || video.userId !== userId) {
      return reply.status(404).send({ error: 'Video not found' });
    }

    if (video.status !== 'PENDING') {
      return { ok: true, alreadyStarted: true, status: video.status };
    }

    // Confirm the object is in storage before touching the queue
    try {
      await s3Client.send(
        new HeadObjectCommand({ Bucket: 'velostream-uploads', Key: video.storageKey })
      );
    } catch {
      return reply
        .status(400)
        .send({ error: 'Object not in storage yet; complete the PUT to MinIO first' });
    }

    // Build a long-lived signed URL using the INTERNAL endpoint so the Rust
    // worker container can reach MinIO directly (minio:9000, not localhost).
    const internalUrl = await getSignedUrl(
      s3Client,
      new GetObjectCommand({ Bucket: 'velostream-uploads', Key: video.storageKey }),
      { expiresIn: 86400 }
    );

    await prisma.video.update({ where: { id: video.id }, data: { status: 'PROCESSING' } });

    try {
      const job = await transcodeQueue.add(
        'transcode-video',
        { videoId: video.id, url: internalUrl },
        { jobId: video.id }
      );
      request.log.info({ videoId: video.id, jobId: job.id }, 'transcode job enqueued');
    } catch (queueError) {
      await prisma.video
        .update({ where: { id: video.id }, data: { status: 'PENDING' } })
        .catch(() => undefined);
      request.log.error({ videoId: video.id, err: queueError }, 'transcodeQueue.add failed');
      return reply.status(503).send({ error: 'Queue unavailable; retry shortly' });
    }

    return { ok: true, videoId: video.id };
  });

  fastify.post("/webhooks/minio", async (request, reply) => {
    const event = request.body as any;

    // Extract the storage key from the MinIO event payload
    const rawKey = event.Records?.[0]?.s3?.object?.key;

    if (!rawKey) {
      return reply.status(400).send({ error: "Invalid event data from storage provider" });
    }

    const storageKey = decodeURIComponent(rawKey);

    request.log.info({ rawKey, storageKey }, 'minio webhook received');

    // First, see if we have ANY record for this key (status may have changed)
    const videoAnyStatus = await prisma.video.findFirst({
      where: { storageKey },
      orderBy: { createdAt: 'desc' },
    });

    if (!videoAnyStatus) {
      request.log.warn({ storageKey }, 'no video found for key');
      // Return 200 so MinIO doesn't keep retrying the webhook for a non-existent record
      return reply.status(200).send({ message: "No video found for this key" });
    }

    if (videoAnyStatus.status !== 'PENDING') {
      request.log.info(
        { videoId: videoAnyStatus.id, status: videoAnyStatus.status, storageKey },
        'video not pending; ignoring webhook'
      );
      return reply.status(200).send({ message: "Video already handled", status: videoAnyStatus.status });
    }

    // Update status to PROCESSING to signal the start of the pipeline
    await prisma.video.update({
      where: { id: videoAnyStatus.id },
      data: { status: 'PROCESSING' }
    });

    // Build a long-lived signed URL the Rust container can reach
    const internalUrl = await getSignedUrl(
      s3Client,
      new GetObjectCommand({ Bucket: 'velostream-uploads', Key: videoAnyStatus.storageKey }),
      { expiresIn: 86400 }
    );

    await transcodeQueue.add(
      'transcode-video',
      { videoId: videoAnyStatus.id, url: internalUrl },
      { jobId: videoAnyStatus.id }
    );

    request.log.info({ videoId: videoAnyStatus.id }, "transcode job enqueued (webhook)");

    return reply.status(200).send({ success: true });
  });
}
