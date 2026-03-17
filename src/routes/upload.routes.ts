// src/routes/uploads.ts
import { FastifyInstance } from 'fastify';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { prisma } from '../plugins/prisma';
import { probeQueue } from '../queues/video.queue';

const s3Client = new S3Client({
  endpoint: "http://localhost:9000", // MinIO address
  credentials: { accessKeyId: 'minioadmin', secretAccessKey: 'minioadmin' },
  region: 'us-east-1', // Default for MinIO
  forcePathStyle: true,
});

export async function uploadRoutes(fastify: FastifyInstance) {
  /**
   * 1. Initiate Upload
   * Validates user quota and returns a Presigned URL for direct-to-S3 upload.
   */
  fastify.post('/uploads/initiate', async (request, reply) => {
    const { fileName, fileSize, userId } = request.body as {
      fileName: string;
      fileSize: number;
      userId: string;
    };

    // 1. Quota Check (Multi-tenancy extension)
    const user = await prisma.user.findUnique({ where: { id: userId } });
    if (!user) return reply.status(404).send({ error: "User not found" });

    // Use BigInt for large file calculations (JS number limit is 2GB for some operations)
    const currentUsage = user.currentUsageBytes || 0n;
    const quotaBytes = BigInt(user.storageQuotaGb) * 1024n * 1024n * 1024n;

    if (currentUsage + BigInt(fileSize) > quotaBytes) {
      return reply.status(403).send({ error: "Quota exceeded. Please clear space or upgrade." });
    }

    // 2. Create PENDING Database Record
    const video = await prisma.video.create({
      data: {
        userId,
        originalFileName: fileName,
        status: 'PENDING',
        // Prefix with userId and timestamp to prevent naming collisions
        storageKey: `uploads/${userId}/${Date.now()}-${fileName}`,
      }
    });

    // 3. Generate Presigned URL
    const command = new PutObjectCommand({
      Bucket: 'velostream-uploads',
      Key: video.storageKey,
      ContentType: 'video/mp4', // Optional: enforce type
    });

    const uploadUrl = await getSignedUrl(s3Client, command, { expiresIn: 3600 });

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

    const video = await prisma.video.findFirst({
      where: {
        storageKey: storageKey,
        status: 'PENDING'
      }
    });

    if (!video) {
      // Return 200 so MinIO doesn't keep retrying the webhook for a non-existent record
      return reply.status(200).send({ message: "No pending video found for this key" });
    }

    // Update status to PROCESSING to signal the start of the pipeline
    await prisma.video.update({
      where: { id: video.id },
      data: { status: 'PROCESSING' }
    });

	await probeQueue.add('probe-metadata', { 
  videoId: video.id, 
  storageKey: video.storageKey 
});

console.log(`🛰️ Job 'probe-metadata' added to Redis for video ${video.id}`); 

    // NEXT STEP: Emit job to BullMQ (Phase 2)
    return reply.status(200).send({ success: true });
  });
}
