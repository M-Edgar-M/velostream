import Fastify from "fastify";
import prismaPlugin from "./plugins/prisma";
import { uploadRoutes } from "./routes/upload.routes";
import { internalRoutes} from "./routes/internal.routes";
import * as dotenv from "dotenv";

dotenv.config();

const server = Fastify({ logger: true });

async function start() {
  await server.register(prismaPlugin);
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
