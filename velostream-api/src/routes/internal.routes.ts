import { FastifyInstance } from 'fastify';
import { PrismaClient } from '@prisma/client';
import { probeQueue, transcodeQueue } from '../queues/video.queue';

export async function internalRoutes(fastify: FastifyInstance) {
  const prisma = (fastify as any).prisma as PrismaClient;

  fastify.patch('/video/:id/complete', async (request, reply) => {
    const { id } = request.params as { id: string };
    const { status, hls_path } = request.body as { status: string; hls_path: string };

    try {
      const video = await prisma.video.update({
        where: { id },
        data: {
          status: status,
          hlsUrl: hls_path,
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
    // Count pipeline stages from DB — always accurate, no Redis dependency
    const [pending, processing, completed, failed] = await Promise.all([
      prisma.video.count({ where: { status: 'PENDING' } }),
      prisma.video.count({ where: { status: 'PROCESSING' } }),
      prisma.video.count({ where: { status: 'COMPLETED' } }),
      prisma.video.count({ where: { status: 'FAILED' } }),
    ]);

    const pipelineStats = { waiting: pending, active: processing, completed, failed };

    const recentVideos = await prisma.video.findMany({
      take: 5,
      orderBy: { createdAt: 'desc' },
      select: { id: true, status: true, createdAt: true, originalFileName: true },
    });

    let storageHealthy = false;
    try {
      const minioUrl = process.env.MINIO_ENDPOINT || 'http://minio:9000';
      const res = await fetch(`${minioUrl}/minio/health/live`);
      storageHealthy = res.ok;
    } catch {
      storageHealthy = false;
    }

    return { pipelineStats, recentVideos, storageHealthy };
  });

  /**
   * Wipes the Video table and flushes both queues. Dev/test only.
   */
  fastify.post('/reset', async (request, reply) => {
    await prisma.video.deleteMany();

    const errors: string[] = [];

    for (const queue of [probeQueue, transcodeQueue]) {
      try {
        await queue.obliterate({ force: true });
      } catch (err) {
        try {
          await queue.drain(true);
        } catch (e) {
          errors.push(`${queue.name}: ${e instanceof Error ? e.message : String(e)}`);
        }
      }
    }

    return { ok: true, ...(errors.length ? { warnings: errors } : {}) };
  });
}

