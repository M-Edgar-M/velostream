import fp from 'fastify-plugin';
import { PrismaClient } from '@prisma/client';

export default fp(async (server) => {
  const prisma = new PrismaClient();
  await prisma.$connect();

  server.decorate('prisma', prisma);
  server.addHook('onClose', async (server) => {
    await server.prisma.$disconnect();
  });
});

// This helps TypeScript know about the 'prisma' property on the Fastify instance
declare module 'fastify' {
  interface FastifyInstance {
    prisma: PrismaClient;
  }
}
