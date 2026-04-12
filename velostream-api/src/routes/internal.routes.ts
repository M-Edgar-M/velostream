export async function internalRoutes(fastify: any) {
  fastify.patch('/internal/video/:id/complete', async (request: any, reply: any) => {
    const { id } = request.params;
    const { status, hls_path } = request.body;

    try {
      const updatedVideo = await prisma.video.update({
        where: { id },
        data: { 
          status: status, // "READY"
          // Ensure your Prisma schema has an 'hlsUrl' field!
          hlsUrl: hls_path 
        },
      });

      request.log.info({ videoId: id }, 'Video marked as READY in database');
      return { success: true, video: updatedVideo };
    } catch (error) {
      request.log.error(error, '❌ Failed to update video status');
      return reply.status(500).send({ error: 'Internal Server Error' });
    }
  });
}
