import 'dotenv/config'; // Absolute first line to fix environment hoisting
import { Worker, Queue } from "bullmq";
import { prisma } from "../plugins/prisma.ts"; // Now importing the named constant
import { exec } from "child_process";
import { promisify } from "util";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { GetObjectCommand } from "@aws-sdk/client-s3";
import { s3Client } from "../services/storage.service.ts";
import { redisConnection } from "../queues/video.queue.ts";

const execPromise = promisify(exec);

    const transcodeQueue = new Queue('video-transcode', {connection: redisConnection });

export const probeWorker = new Worker('video-probe', async (job) => {
  const { videoId, storageKey } = job.data;

  try {
    const command = new GetObjectCommand({
      Bucket: 'velostream-uploads',
      Key: storageKey,
    });

    const signedUrl = await getSignedUrl(s3Client, command, { expiresIn: 300 });
    console.log(`🔎 Probing video ${videoId}...`);

    // Run ffprobe via shell
    const { stdout } = await execPromise(
      `ffprobe -v error -show_format -show_streams -of json "${signedUrl}"`
    );

    const metadata = JSON.parse(stdout);

    const duration = parseFloat(metadata.format.duration) ||
                     parseFloat(metadata.streams[0]?.duration) || 0;

    const sizeBytes = BigInt(metadata.format.size);

    // Update the record
    await prisma.video.update({
      where: { id: videoId },
      data: {
        status: 'PROBED' as any,
        duration,
        sizeBytes,
      },
    });

    console.log(`✅ Video ${videoId} successfully probed. Duration: ${duration}s`);
   await transcodeQueue.add('start-transcode', {
	   videoId: videoId,
	   storageKey: storageKey,
	   url: signedUrl,

   }); 
   console.log(`PUSHED ${videoId} to Transcode Queue`);


  } catch (error) {
    console.error(`❌ Probe failed for ${videoId}:`, error);

    // We try to mark it as FAILED if possible
    await prisma.video.update({
      where: { id: videoId },
      data: { status: 'FAILED' as any }
    }).catch(() => console.error("Could not update status to FAILED"));

    throw error;
  }
}, {
  connection: redisConnection,
  concurrency: 2
});
