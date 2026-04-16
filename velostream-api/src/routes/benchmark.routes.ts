import { FastifyInstance } from 'fastify';
import type { PrismaClient } from '@prisma/client';

/**
 * Benchmark routes — lightweight endpoints used by benchmark.ts to:
 *   GET /benchmark/ping  — performs a real Prisma DB query so we can measure
 *                          API latency *under load* (i.e. does the Node worker
 *                          block the event loop?).
 */
export async function benchmarkRoutes(fastify: FastifyInstance) {
  const prisma = (fastify as unknown as { prisma: PrismaClient }).prisma;

  /**
   * GET /benchmark/ping
   * Runs a lightweight `SELECT COUNT(*)` via Prisma so the latency measurement
   * reflects genuine DB round-trips, not just in-memory echo latency.
   */
  fastify.get('/benchmark/ping', async (_request, reply) => {
    const start = Date.now();

    // Real DB query — forces the event loop to yield and wait for I/O.
    // Using aggregateRaw for a raw COUNT avoids fetching unnecessary rows.
    const count = await prisma.video.count();

    return reply.send({
      ok: true,
      videoCount: count,
      dbQueryMs: Date.now() - start,
      ts: Date.now(),
    });
  });
}
