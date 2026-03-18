import { Worker } from "bullmq";
import { prisma } from "../plugins/prisma.ts";
import { exec } from "child_process";
import { promisify } from "util";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { GetObjectCommand } from "@aws-sdk/client-s3";
import { s3Client } from "../services/storage.service.ts";
import { redisConnection } from "../queues/video.queue.ts";

const execPromise = promisify(exec);
export const probeWorker = new Worker('video-probe', async (job) => {
  const { videoId, storageKey } = job.data;
  
  try {
    const command = new GetObjectCommand({
      Bucket: 'velostream-uploads',
      Key: storageKey,
    });
    console.log("THIS IS THE JOB: ", job);
    console.log('🚀 ~ command:', command);
    
    // Valid for 5 minutes - plenty of time for ffprobe to read the header
    const signedUrl = await getSignedUrl(s3Client, command, { expiresIn: 300 });

    console.log(`🔎 Probing video ${videoId}...`);

    // Use quotes and disable shell parsing if possible, 
    // but the double quotes here are the minimum requirement.
    const { stdout } = await execPromise(
      `ffprobe -v error -show_format -show_streams -of json "${signedUrl}"`
    );

    const metadata = JSON.parse(stdout);
    
    // Fallback: If format duration is missing, check the first stream
    const duration = parseFloat(metadata.format.duration) || 
                     parseFloat(metadata.streams[0]?.duration) || 0;
    
    // Ensure we use BigInt for Prisma compatibility
    const sizeBytes = BigInt(metadata.format.size);

    await prisma.video.update({
      where: { id: videoId },
      data: {
        status: 'PROBED' as any, // Cast to any if JobStatus enum is strict
        duration,
        sizeBytes,
      },
    });

    console.log(`✅ Video ${videoId} successfully probed. Duration: ${duration}s`);
  } catch (error) {
    console.error(`❌ Probe failed for ${videoId}:`, error);
    
    // Update DB to FAILED so the UI knows something went wrong
    await prisma.video.update({
      where: { id: videoId },
      data: { status: 'FAILED' as any }
    }).catch(() => {}); // Ignore error if update fails

    throw error; 
  }
}, { 
  connection: redisConnection,
  concurrency: 2 // Allow processing 2 videos at once on this machine
});
