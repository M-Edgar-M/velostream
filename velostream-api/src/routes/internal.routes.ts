import { FastifyInstance } from 'fastify';
import { PrismaClient } from '@prisma/client';

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
}
