import Fastify from "fastify";
import prismaPlugin from "./plugins/prisma";
import { uploadRoutes } from "./routes/upload.routes";
import { internalRoutes } from "./routes/internal.routes";
import { benchmarkRoutes } from "./routes/benchmark.routes";
import * as dotenv from "dotenv";

dotenv.config();

// ── Workers ──────────────────────────────────────────────────────────────────
// Import workers AFTER dotenv so env vars are available for S3/Redis clients.
// Named imports ensure tsx cannot tree-shake these away.
import { probeWorker } from "./workers/probe.worker.ts";
import { legacyTranscodeWorker } from "./workers/transcode.worker.ts";

const server = Fastify({ logger: true });

async function start() {
  // Register plugins and routes
  await server.register(prismaPlugin);
  await server.register(uploadRoutes);
  await server.register(internalRoutes, { prefix: '/internal' });
  await server.register(benchmarkRoutes);

  try {
    await server.listen({ port: 3000, host: "0.0.0.0" });
    console.log("VeloStream API is running at http://localhost:3000");
    console.log(`✅ Workers active: probe=${probeWorker.name}, transcode=${legacyTranscodeWorker.name}`);
  } catch (err) {
    server.log.error(err);
    process.exit(1);
  }
}

(BigInt.prototype as any).toJSON = function () {
  return this.toString();
};

start();

