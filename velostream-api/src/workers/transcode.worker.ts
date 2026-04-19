import 'dotenv/config';
import { Worker } from "bullmq";
import { exec } from "child_process";
import { promisify } from "util";
import { redisConnection } from "../queues/video.queue";

const execPromise = promisify(exec);

export const legacyTranscodeWorker = new Worker('video-transcode', async (job) => {
  const { videoId, storageKey, url } = job.data;
  
  console.time(`⏱️ Node-Transcode-${videoId}`);
  
  try {
    // We'll simulate a 720p HLS transcode
    // Note: We are using a basic shell exec which is standard in Node "legacy" ways
    const cmd = `ffmpeg -i "${url}" -profile:v baseline -level 3.0 -s 1280x720 -start_number 0 -hls_time 10 -hls_list_size 0 -f hls output.m3u8`;
    
    await execPromise(cmd);
    
    console.timeEnd(`⏱️ Node-Transcode-${videoId}`);
  } catch (err) {
    console.error("Legacy transcode failed", err);
  }
}, { connection: redisConnection });
