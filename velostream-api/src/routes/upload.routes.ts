import { FastifyInstance } from 'fastify';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import type { PrismaClient } from '@prisma/client';
import { probeQueue, transcodeQueue } from '../queues/video.queue';

const s3Client = new S3Client({
  endpoint: process.env.MINIO_ENDPOINT || "http://localhost:9000",
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || 'minioadmin',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || 'minioadmin'
  },
  region: process.env.AWS_REGION || 'us-east-1',
  forcePathStyle: true,
});

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

    // 2. Enqueue transcode job — must happen before the response is sent
    const minioEndpoint = process.env.MINIO_ENDPOINT || 'http://minio:9000';
    console.log("DEBUG: Attempting to add job to BullMQ", video.id);
    try {
      const job = await transcodeQueue.add('process-video', {
        videoId: video.id,
        url: `${minioEndpoint}/velostream-uploads/${video.storageKey}`,
        inputPath: `velostream-uploads/${video.storageKey}`
      }, { jobId: video.id });
      console.log("DEBUG: Successfully added job", job.id);
      request.log.info({ videoId: video.id, jobId: job.id }, "transcode-video job added to queue");
    } catch (queueError) {
      request.log.error({ videoId: video.id, err: queueError }, "FAILED to add transcode job to BullMQ queue");
      console.error("DEBUG: transcodeQueue.add() threw an error:", queueError);
      // Do not abort the upload — the video record exists and can be requeued.
      // The error is logged so the operator can diagnose the Redis issue.
    }

    // 3. Generate Presigned URL
    const command = new PutObjectCommand({
      Bucket: 'velostream-uploads',
      Key: video.storageKey,
      ContentType: contentType || 'video/mp4', // Required to match the exact client upload
    });

    const uploadUrl = await getSignedUrl(presignClient, command, { expiresIn: 3600 });

    return { uploadUrl, videoId: video.id };
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

    await probeQueue.add('probe-metadata', {
      videoId: videoAnyStatus.id,
      storageKey: videoAnyStatus.storageKey
    });

    request.log.info({ videoId: videoAnyStatus.id }, "probe-metadata job added");

    // NEXT STEP: Emit job to BullMQ (Phase 2)
    return reply.status(200).send({ success: true });
  });
}
