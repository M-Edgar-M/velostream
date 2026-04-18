import { FastifyInstance } from 'fastify';
import { PrismaClient } from '@prisma/client';
import { transcodeQueue } from '../queues/video.queue';

export async function internalRoutes(fastify: FastifyInstance) {
  const prisma = (fastify as any).prisma as PrismaClient;

  fastify.patch('/video/:id/complete', async (request, reply) => {
    const { id } = request.params as { id: string };
    const { status, hls_path } = request.body as { status: string; hls_path: string };

    try {
      const video = await prisma.video.update({
        where: { id },
        data: {
          status: status,    // Should be "COMPLETED"
          hlsUrl: hls_path,  // Mapping the Rust path to your DB column
        },
      });

      request.log.info({ videoId: id }, "✅ Video marked as COMPLETED in DB");
  return { success: true, videoId: video.id, status: video.status };
    } catch (error) {
      request.log.error(error, "❌ Failed to update video status");
      return reply.status(500).send({ error: "Internal Server Error" });
    }
  });

  fastify.get('/test-user', async (request, reply) => {
    const user = await prisma.user.findFirst();
    if (!user) return reply.status(404).send({ error: "No user found. Did you run the seed script?" });
    return { id: user.id, email: user.email };
  });
  fastify.get('/system-health', async (request, reply) => {
    const queueStats = await transcodeQueue.getJobCounts();
    const recentVideos = await prisma.video.findMany({
      take: 5,
      orderBy: { createdAt: 'desc' },
      select: { id: true, status: true, createdAt: true, originalFileName: true }
    });

    let storageHealthy = false;
    try {
      const minioUrl = process.env.MINIO_ENDPOINT || 'http://minio:9000';
      const res = await fetch(`${minioUrl}/minio/health/live`);
      storageHealthy = res.ok;
    } catch (e) {
      storageHealthy = false;
    }

    return { queueStats, recentVideos, storageHealthy };
  });

  fastify.post('/clear-queue', async (request, reply) => {
    try {
      await transcodeQueue.obliterate({ force: true });
      return { success: true };
    } catch (e) {
      // If obliterate fails (e.g. redis busy), fallback to drain and clean
      await transcodeQueue.drain(true);
      return { success: true, message: "Drained queue" };
    }
  });
}
