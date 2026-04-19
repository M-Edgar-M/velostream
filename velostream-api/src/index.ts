import Fastify from "fastify";
import prismaPlugin from "./plugins/prisma";
import { uploadRoutes } from "./routes/upload.routes";
import { internalRoutes} from "./routes/internal.routes";
import { verifyRedisConnection } from "./queues/video.queue";
import * as dotenv from "dotenv";

dotenv.config();

const server = Fastify({ logger: true });

async function start() {
  await server.register(prismaPlugin);
  const redisOk = await verifyRedisConnection();
  if (!redisOk) {
    server.log.warn("Redis PING failed at startup; BullMQ enqueue and queue stats may not work until Redis is reachable.");
  }
  await server.register(uploadRoutes);
  await server.register(require('@fastify/static'), {
    root: require('path').join(__dirname, '..', 'public'),
    prefix: '/',
  });
  await server.register(internalRoutes, { prefix: '/internal' });

  try {
    await server.listen({ port: 3000, host: "0.0.0.0" });
    console.log("VeloStream API is running at http://localhost:3000");
  } catch (err) {
    server.log.error(err);
    process.exit(1);
  }
}

(BigInt.prototype as any).toJSON = function () {
  return this.toString();
};

start();
