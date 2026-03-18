import { Worker } from "bullmq";
import { prisma } from "../plugins/prisma";
import { exec } from "child_process";
import { promisify } from "util";

const execPromise = promisify(exec);

export const probeWorker = new Worker('video-probe', async (job) => {
	
	const { videoId, storageKey, signedUrl } = job.data;

	try {
		const { stdout } = await execPromise(`ffprobe -v error -show_format -show_streams -of json "${signedUrl}"`);
		const metadata = JSON.parse(stdout);
		const sizeBytes = BigInt(metadat.format.size);
		const duration = parseFloat(metadata.format.duration);

		await prisma.$transaction([
      prisma.video.update({
        where: { id: videoId },
        data: {
          status: 'PROBED',
          duration,
          sizeBytes
        }
      }),
      prisma.user.update({
        where: { id: job.data.userId },
        data: { currentUsageBytes: { increment: sizeBytes } }
      })
    ]);
    console.log(`✅ Video ${videoId} probed. Size: ${sizeBytes} bytes.`);
	} catch(error) {
		console.error(`❌ Probe failed for ${videoId}:`, error);
    throw error; // BullMQ will mark this as "Failed"
	}

}, { connection: { host: 'localhost', port: 6379 }
});
