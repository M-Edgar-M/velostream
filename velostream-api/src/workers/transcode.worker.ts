import 'dotenv/config';
import { Queue, Worker } from 'bullmq';
import { exec } from 'child_process';
import { promisify } from 'util';
import fs from 'fs';
import path from 'path';
import http from 'http';
import { PutObjectCommand } from '@aws-sdk/client-s3';
import { s3Client } from '../services/storage.service.ts';
import { redisConnection } from '../queues/video.queue.ts';

const execPromise = promisify(exec);

const MINIO_BUCKET = 'velostream-public';

// ────────────────────────────────────────────────────────────────────────────
// Step 2: Upload all HLS segments to MinIO (mirrors s3upload.rs)
// ────────────────────────────────────────────────────────────────────────────
async function uploadHlsToMinio(videoId: string, outputDir: string): Promise<void> {
  const files = fs.readdirSync(outputDir);

  for (const fileName of files) {
    const filePath = path.join(outputDir, fileName);
    if (!fs.statSync(filePath).isFile()) continue;

    // Match the Rust engine's key structure: transcoded/{videoId}/{filename}
    const key = `transcoded/${videoId}/${fileName}`;

    // Match the Rust engine's content-type logic
    let contentType = 'application/octet-stream';
    if (fileName.endsWith('.m3u8')) contentType = 'application/vnd.apple.mpegurl';
    else if (fileName.endsWith('.ts')) contentType = 'video/MP2T';

    console.log(`📤 Uploading: ${fileName}`);

    const body = fs.readFileSync(filePath);

    await s3Client.send(
      new PutObjectCommand({
        Bucket: MINIO_BUCKET,
        Key: key,
        Body: body,
        ContentType: contentType,
      }),
    );
  }

  console.log(`✅ All HLS segments uploaded for ${videoId}`);
}

// ────────────────────────────────────────────────────────────────────────────
// Step 3: Notify the API that transcoding is complete (mirrors notify_api_completion)
// ────────────────────────────────────────────────────────────────────────────
function notifyApiCompletion(videoId: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify({
      status: 'COMPLETED',
      hls_path: `transcoded/${videoId}/playlist.m3u8`,
    });

    const req = http.request(
      {
        hostname: '127.0.0.1',
        port: 3000,
        path: `/internal/video/${videoId}/complete`,
        method: 'PATCH',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(payload),
        },
      },
      (res) => {
        res.resume(); // drain response
        if (res.statusCode && res.statusCode >= 200 && res.statusCode < 300) {
          console.log(`✅ Successfully notified API for video ${videoId}`);
          resolve();
        } else {
          console.error(`❌ Failed to notify API. Status: ${res.statusCode}`);
          resolve(); // don't fail the job for a notification error
        }
      },
    );

    req.on('error', (err) => {
      console.error(`❌ Failed to reach API:`, err.message);
      resolve(); // don't fail the job for a notification error
    });

    req.write(payload);
    req.end();
  });
}

// ────────────────────────────────────────────────────────────────────────────
// Full lifecycle: Transcode → Upload → Notify → Cleanup
// Mirrors the exact sequence in transcode-engine/src/main.rs
// ────────────────────────────────────────────────────────────────────────────

// Create the worker initially PAUSED so it does not pick up stale Redis jobs
// (from previous benchmark runs or crashed sessions) before we drain them.
export const legacyTranscodeWorker = new Worker(
  'video-transcode-legacy',
  async (job) => {
    const { videoId, url } = job.data;

    const outputDir = `outputs/${videoId}`;
    const playlist = `${outputDir}/playlist.m3u8`;

    console.time(`⏱️ Node-Transcode-${videoId}`);

    try {
      // ── Step 1: FFmpeg Transcode ──────────────────────────────────────
      // Exact same flags as the Rust engine (including -map 0)
      fs.mkdirSync(outputDir, { recursive: true });

      const cmd = `ffmpeg -y -i "${url}" -codec:v libx264 -codec:a aac -map 0 -f hls -hls_time 10 -hls_playlist_type event "${playlist}"`;
      console.log(`🎬 Starting FFmpeg for ${videoId} | input: ${url}`);
      await execPromise(cmd, { maxBuffer: 50 * 1024 * 1024 });
      console.log(`✅ Transcode complete for ${videoId}`);

      // ── Step 2: S3 Upload ─────────────────────────────────────────────
      console.log(`Starting upload to MinIO...`);
      await uploadHlsToMinio(videoId, outputDir);

      // ── Step 3: Notify API ────────────────────────────────────────────
      await notifyApiCompletion(videoId);

      // ── Step 4: Cleanup ───────────────────────────────────────────────
      fs.rmSync(outputDir, { recursive: true, force: true });
      console.log(`🧹 Local workspace cleaned for ${videoId}`);

      console.timeEnd(`⏱️ Node-Transcode-${videoId}`);
    } catch (err) {
      console.timeEnd(`⏱️ Node-Transcode-${videoId}`);
      console.error(`❌ Legacy transcode failed for ${videoId}:`, err);
      throw err;
    }
  },
  { connection: redisConnection, autorun: false },
);

// ────────────────────────────────────────────────────────────────────────────
// Drain stale jobs then start the worker. This ensures no leftover benchmark
// or crashed-session jobs get auto-processed when the API server boots.
// ────────────────────────────────────────────────────────────────────────────
(async () => {
  const q = new Queue('video-transcode-legacy', { connection: { host: 'localhost', port: 6380, maxRetriesPerRequest: null } });
  try {
    const waiting = await q.getWaiting();
    const delayed = await q.getDelayed();
    const staleCount = waiting.length + delayed.length;

    if (staleCount > 0) {
      console.log(`🧹 Draining ${staleCount} stale job(s) from video-transcode-legacy queue...`);
      await q.drain();
      console.log(`✅ Stale jobs drained.`);
    }
  } catch (err) {
    console.error('⚠️ Failed to drain stale transcode jobs:', err);
  } finally {
    await q.close();
  }

  // Now it's safe to start processing
  legacyTranscodeWorker.run();
  console.log('✅ Legacy transcode worker is now accepting jobs.');
})();

