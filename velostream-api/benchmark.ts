/**
 * VeloStream Engine Benchmark
 * ────────────────────────────────────────────────────────────────────
 * Pushes 5 concurrent jobs to each transcode engine and measures:
 *   • Total round-trip duration  (job enqueued → DB status = COMPLETED)
 *   • API latency (P50 / P95)    via polling GET /benchmark/ping (real DB query)
 *   • Peak RSS memory            via `ps -o rss=` on the worker PID
 *   • Throughput                 MB/s = fileSizeMB / (avgDurationMs / 1000)
 *
 * Assumptions (per user spec):
 *   • Fastify API is already running on http://localhost:3000
 *   • Rust engine (transcode-engine) is already running
 *   • A real 10-second MP4 test file exists at TEST_VIDEO_PATH
 *   • Redis is on localhost:6380
 *   • Postgres is reachable via DATABASE_URL in .env
 *
 * Usage:
 *   cd velostream-api
 *   TEST_VIDEO_PATH=/path/to/test.mp4 tsx benchmark.ts
 *
 * Output:
 *   benchmark-results.json
 *   benchmark-results.csv
 */

import 'dotenv/config';
import { Queue } from 'bullmq';
import { PrismaClient } from '@prisma/client';
import { PrismaPg } from '@prisma/adapter-pg';
import pg from 'pg';
import { exec } from 'child_process';
import { promisify } from 'util';
import fs from 'fs';
import path from 'path';
import http from 'http';
import { fileURLToPath } from 'url';

const execAsync = promisify(exec);

// ─── Configuration ────────────────────────────────────────────────────────────

const CONCURRENCY = 5;
const API_BASE = 'http://127.0.0.1:3000';
const API_PROBE_INTERVAL = 200;   // ms between latency pings
const RSS_POLL_INTERVAL = 500;   // ms between ps samples
const COMPLETION_TIMEOUT = 10 * 60 * 1000; // 10 min max wait per engine
const DB_POLL_INTERVAL = 2_000; // ms between DB completion polls (Rust path)

/**
 * Path to the test MP4 file.
 * Override at runtime:  TEST_VIDEO_PATH=/abs/path/to/test.mp4 tsx benchmark.ts
 *
 * We use Big Buck Bunny's first 10 seconds as the canonical test clip.
 * Place it at velostream-api/test-assets/test-10s.mp4 or override via env.
 */
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const TEST_VIDEO_PATH = process.env.TEST_VIDEO_PATH
  ?? path.resolve(__dirname, 'test-assets', 'test-10s.mp4');

const REDIS_OPTS = { host: 'localhost', port: 6380, maxRetriesPerRequest: null } as const;

// ─── Prisma (standalone — no Fastify) ─────────────────────────────────────────

const connectionString = process.env.DATABASE_URL!;
if (!connectionString) {
  console.error('❌  DATABASE_URL is not set. Check your .env file.');
  process.exit(1);
}
const pool = new pg.Pool({ connectionString });
const adapter = new PrismaPg(pool);
const prisma = new PrismaClient({ adapter });

// ─── Queues ───────────────────────────────────────────────────────────────────

const legacyQueue = new Queue('video-transcode-legacy', { connection: REDIS_OPTS });
const rustQueue = new Queue('video-transcode', { connection: REDIS_OPTS });

// ─── Types ────────────────────────────────────────────────────────────────────

interface JobMetric {
  jobId: string;
  videoId: string;
  durationMs: number;
  memoryRssKb: number;  // peak RSS sampled during this job's lifetime
}

interface EngineResult {
  engine: 'node-legacy' | 'rust';
  jobs: JobMetric[];
  avgDurationMs: number;
  peakMemoryRssKb: number;
  apiLatencyP50Ms: number;
  apiLatencyP95Ms: number;
  throughputMBps: number;  // fileSizeMB / avgDurationSeconds
}

interface BenchmarkReport {
  timestamp: string;
  concurrency: number;
  testVideoPath: string;
  fileSizeMB: number;
  node: EngineResult;
  rust: EngineResult;
}

// ─── Utilities ────────────────────────────────────────────────────────────────

/** Returns the RSS (resident set size) in kB for a given PID.
 *  Recursively finds all child processes and sums their RSS, so FFmpeg's
 *  memory is accurately counted in the total.
 */
async function getTotalRss(pid: number): Promise<number> {
  const allPids = new Set<number>([pid]);
  let currentPids = [pid];

  while (currentPids.length > 0) {
    try {
      const { stdout } = await execAsync(`pgrep -P ${currentPids.join(',')}`);
      const kids = stdout
        .trim()
        .split('\n')
        .map(Number)
        .filter((n) => !isNaN(n) && !allPids.has(n));
      for (const kid of kids) allPids.add(kid);
      currentPids = kids;
    } catch {
      break; // pgrep returns error if no children found
    }
  }

  try {
    const pidsStr = Array.from(allPids).join(',');
    const { stdout } = await execAsync(`ps -o rss= -p ${pidsStr}`);
    return stdout
      .trim()
      .split('\n')
      .reduce((sum, line) => {
        const val = parseInt(line.trim(), 10);
        return sum + (isNaN(val) ? 0 : val);
      }, 0);
  } catch {
    return 0; // process may have exited
  }
}

/** Returns the PID of the first process whose command matches the pattern. */
async function findPid(pattern: string): Promise<number | null> {
  try {
    // Modify the pattern to avoid matching the pgrep command itself if it's a simple string
    // e.g. "transcode-engine" -> "[t]ranscode-engine"
    const safePattern = pattern.length > 1 && !pattern.includes('[') 
      ? `[${pattern[0]}]${pattern.slice(1)}` 
      : pattern;
      
    const { stdout } = await execAsync(`pgrep -f "${safePattern}" | head -1`);
    const pid = parseInt(stdout.trim(), 10);
    return isNaN(pid) ? null : pid;
  } catch {
    return null;
  }
}

/** Percentile helper (input array need not be sorted). */
function percentile(values: number[], p: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

/**
 * Make a GET request using Node's built-in http module.
 * Returns parsed JSON body + statusCode.
 * We avoid the global `fetch` because it's experimental on Node 18 and unreliable.
 */
function httpGet(url: string): Promise<{ statusCode: number; body: Record<string, unknown> }> {
  return new Promise((resolve, reject) => {
    http.get(url, (res) => {
      let data = '';
      res.on('data', (chunk: Buffer) => { data += chunk; });
      res.on('end', () => {
        try {
          resolve({ statusCode: res.statusCode ?? 0, body: JSON.parse(data) });
        } catch {
          resolve({ statusCode: res.statusCode ?? 0, body: {} });
        }
      });
    }).on('error', reject);
  });
}

/** Probe the /benchmark/ping endpoint and return response time in ms. */
async function probeApiLatency(): Promise<number> {
  const start = Date.now();
  try {
    await httpGet(`${API_BASE}/benchmark/ping`);
    return Date.now() - start;
  } catch {
    return -1; // API down / unreachable
  }
}

/**
 * Spin up a background interval that:
 *   1. Probes API latency every API_PROBE_INTERVAL ms → fills `latencies[]`
 *   2. Samples RSS for `pid` every RSS_POLL_INTERVAL ms → updates `peakRss`
 *
 * Call the returned `stop()` function to halt collection and retrieve data.
 */
function startTelemetryCollection(pid: number): {
  stop: () => { latencies: number[]; peakRssKb: number };
} {
  const latencies: number[] = [];
  let peakRssKb = 0;

  const apiInterval = setInterval(async () => {
    const ms = await probeApiLatency();
    if (ms >= 0) latencies.push(ms);
  }, API_PROBE_INTERVAL);

  const rssInterval = setInterval(async () => {
    const rss = await getTotalRss(pid);
    if (rss > peakRssKb) peakRssKb = rss;
  }, RSS_POLL_INTERVAL);

  return {
    stop: () => {
      clearInterval(apiInterval);
      clearInterval(rssInterval);
      return { latencies, peakRssKb };
    },
  };
}

/**
 * Create a synthetic Video DB record so we have a real `videoId` for the job.
 * Uses the seeded test user (test2@example.com). Creates it fresh each run.
 */
async function createSyntheticVideoRecord(index: number): Promise<string> {
  const user = await prisma.user.findFirstOrThrow({
    where: { email: 'test2@example.com' },
  });

  const video = await prisma.video.create({
    data: {
      userId: user.id,
      originalFileName: `benchmark-${index}-${Date.now()}.mp4`,
      storageKey: `benchmark/${user.id}/${Date.now()}-${index}.mp4`,
      status: 'PROCESSING',
    },
  });

  return video.id;
}

/**
 * Poll the DB until the video `status` is COMPLETED (or FAILED), or until
 * `timeoutMs` elapses. Returns the final status string.
 */
async function waitForCompletion(
  videoId: string,
  timeoutMs = COMPLETION_TIMEOUT,
): Promise<'COMPLETED' | 'FAILED' | 'TIMEOUT'> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const video = await prisma.video.findUnique({ where: { id: videoId } });
    if (video?.status === 'COMPLETED') return 'COMPLETED';
    if (video?.status === 'FAILED') return 'FAILED';
    await new Promise((r) => setTimeout(r, DB_POLL_INTERVAL));
  }
  return 'TIMEOUT';
}

// ─── Phase 1: Node.js Legacy Worker Benchmark ─────────────────────────────────

async function runNodeLegacyBenchmark(fileSizeMB: number): Promise<EngineResult> {
  console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('🟡  Phase 1 — Node.js Legacy Worker (child_process.exec)');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');

  // Find the Node.js API process PID (which also runs the transcode worker).
  // We match on 'tsx' + 'src/index' to avoid matching the benchmark process itself.
  const nodePid = await findPid('tsx.*watch.*src/index');
  if (!nodePid) {
    console.warn('⚠️  Could not find the Fastify API process. Trying alternative pattern...');
    // Fallback: try matching with just tsx src/index
    const fallbackPid = await findPid('tsx.*src/index');
    if (!fallbackPid) {
      console.warn('⚠️  Still no match. RSS will read as 0.\n   Make sure the API is running with: pnpm dev:api');
    } else {
      console.log(`📍  Node.js API PID (fallback): ${fallbackPid}`);
    }
    var effectiveNodePid = fallbackPid;
  } else {
    console.log(`📍  Node.js API PID: ${nodePid}`);
    var effectiveNodePid = nodePid;
  }

  const telemetry = startTelemetryCollection(effectiveNodePid ?? process.pid);

  const jobPromises: Promise<JobMetric>[] = [];

  for (let i = 0; i < CONCURRENCY; i++) {
    const videoId = await createSyntheticVideoRecord(i);

    jobPromises.push(
      (async (): Promise<JobMetric> => {
        const enqueueTime = Date.now();

        const bullJob = await legacyQueue.add('benchmark-transcode', {
          videoId,
          storageKey: `benchmark/${videoId}.mp4`,
          url: TEST_VIDEO_PATH,  // absolute file path — ffmpeg handles it natively
        });

        console.log(`   ↳ Job [${i + 1}/${CONCURRENCY}] enqueued  jobId=${bullJob.id}  videoId=${videoId}`);

        // Poll DB for completion — identical to the Rust path.
        // The Node worker calls PATCH /internal/video/:id/complete which sets
        // the DB status to COMPLETED, just like the Rust engine does.
        const finalStatus = await waitForCompletion(videoId);
        const durationMs = Date.now() - enqueueTime;

        if (finalStatus !== 'COMPLETED') {
          console.warn(`   ⚠️  Job ${bullJob.id} finished with status: ${finalStatus} (${durationMs} ms)`);
        }

        return {
          jobId: bullJob.id!,
          videoId,
          durationMs,
          memoryRssKb: 0, // filled in from aggregate peak below
        };
      })(),
    );
  }

  console.log(`\n⏳  Waiting for all ${CONCURRENCY} Node.js jobs to complete (polling DB every ${DB_POLL_INTERVAL}ms)...\n`);
  const results = await Promise.all(jobPromises);
  const { latencies, peakRssKb } = telemetry.stop();

  // Distribute the aggregate peak RSS across jobs (best we can without per-job isolation)
  const metrics: JobMetric[] = results.map((r) => ({ ...r, memoryRssKb: peakRssKb }));

  const avgDurationMs = metrics.reduce((s, j) => s + j.durationMs, 0) / metrics.length;
  const throughputMBps = fileSizeMB / (avgDurationMs / 1000);

  console.log(`\n✅  Node.js benchmark complete:`);
  console.log(`    Avg duration : ${avgDurationMs.toFixed(0)} ms`);
  console.log(`    Peak RSS     : ${(peakRssKb / 1024).toFixed(1)} MB`);
  console.log(`    API P50      : ${percentile(latencies, 50)} ms`);
  console.log(`    API P95      : ${percentile(latencies, 95)} ms`);
  console.log(`    Throughput   : ${throughputMBps.toFixed(3)} MB/s`);

  return {
    engine: 'node-legacy',
    jobs: metrics,
    avgDurationMs,
    peakMemoryRssKb: peakRssKb,
    apiLatencyP50Ms: percentile(latencies, 50),
    apiLatencyP95Ms: percentile(latencies, 95),
    throughputMBps,
  };
}

// ─── Phase 2: Rust Engine Benchmark ───────────────────────────────────────────

async function runRustBenchmark(fileSizeMB: number): Promise<EngineResult> {
  console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('🦀  Phase 2 — Rust Engine (tokio::process::Command)');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');

  // Locate the Rust binary process.
  const rustPid = await findPid('transcode-engine');
  if (!rustPid) {
    console.warn('⚠️  Could not find the Rust transcode-engine process.\n   Make sure it is running: cd transcode-engine && cargo run --release');
  } else {
    console.log(`📍  Rust engine PID: ${rustPid}`);
  }

  const telemetry = startTelemetryCollection(rustPid ?? process.pid);

  // For each job we:
  //   1. Create a synthetic DB record
  //   2. Push to BullMQ video-transcode queue (which the Rust engine polls)
  //   3. Poll the DB until status = COMPLETED

  const jobPromises: Promise<JobMetric>[] = [];

  for (let i = 0; i < CONCURRENCY; i++) {
    const videoId = await createSyntheticVideoRecord(i);

    jobPromises.push(
      (async (): Promise<JobMetric> => {
        const enqueueTime = Date.now();

        const bullJob = await rustQueue.add('benchmark-transcode', {
          videoId,
          storageKey: `benchmark/${videoId}.mp4`,
          // The Rust engine reads `url` from job data and passes to ffmpeg -i
          url: TEST_VIDEO_PATH,
        });

        console.log(`   ↳ Job [${i + 1}/${CONCURRENCY}] enqueued  jobId=${bullJob.id}  videoId=${videoId}`);

        // Poll Prisma until the Rust notify_api_completion call lands (or we get COMPLETED in DB)
        const finalStatus = await waitForCompletion(videoId);
        const durationMs = Date.now() - enqueueTime;

        if (finalStatus !== 'COMPLETED') {
          console.warn(`   ⚠️  Job ${bullJob.id} finished with status: ${finalStatus} (${durationMs} ms)`);
        }

        return {
          jobId: bullJob.id!,
          videoId,
          durationMs,
          memoryRssKb: 0, // filled from aggregate peak below
        };
      })(),
    );
  }

  console.log(`\n⏳  Waiting for all ${CONCURRENCY} Rust jobs to complete (polling DB every ${DB_POLL_INTERVAL}ms)...\n`);
  const results = await Promise.all(jobPromises);
  const { latencies, peakRssKb } = telemetry.stop();

  const metrics: JobMetric[] = results.map((r) => ({ ...r, memoryRssKb: peakRssKb }));

  const avgDurationMs = metrics.reduce((s, j) => s + j.durationMs, 0) / metrics.length;
  const throughputMBps = fileSizeMB / (avgDurationMs / 1000);

  console.log(`\n✅  Rust benchmark complete:`);
  console.log(`    Avg duration : ${avgDurationMs.toFixed(0)} ms`);
  console.log(`    Peak RSS     : ${(peakRssKb / 1024).toFixed(1)} MB`);
  console.log(`    API P50      : ${percentile(latencies, 50)} ms`);
  console.log(`    API P95      : ${percentile(latencies, 95)} ms`);
  console.log(`    Throughput   : ${throughputMBps.toFixed(3)} MB/s`);

  return {
    engine: 'rust',
    jobs: metrics,
    avgDurationMs,
    peakMemoryRssKb: peakRssKb,
    apiLatencyP50Ms: percentile(latencies, 50),
    apiLatencyP95Ms: percentile(latencies, 95),
    throughputMBps,
  };
}

// ─── Output Writers ───────────────────────────────────────────────────────────

function writeJson(report: BenchmarkReport): string {
  const filePath = path.resolve(__dirname, 'benchmark-results.json');
  fs.writeFileSync(filePath, JSON.stringify(report, null, 2), 'utf-8');
  return filePath;
}

function writeCsv(report: BenchmarkReport): string {
  const filePath = path.resolve(__dirname, 'benchmark-results.csv');

  const header = [
    'engine',
    'jobId',
    'videoId',
    'durationMs',
    'memoryRssKb',
    'apiLatencyP50Ms',
    'apiLatencyP95Ms',
    'avgDurationMs',
    'throughputMBps',
  ].join(',');

  const rows: string[] = [header];

  for (const engine of [report.node, report.rust] as EngineResult[]) {
    for (const job of engine.jobs) {
      rows.push(
        [
          engine.engine,
          job.jobId,
          job.videoId,
          job.durationMs,
          job.memoryRssKb,
          engine.apiLatencyP50Ms,
          engine.apiLatencyP95Ms,
          engine.avgDurationMs.toFixed(2),
          engine.throughputMBps.toFixed(4),
        ].join(','),
      );
    }
  }

  fs.writeFileSync(filePath, rows.join('\n'), 'utf-8');
  return filePath;
}

// ─── Preflight Checks ─────────────────────────────────────────────────────────

async function preflightChecks(): Promise<number> {
  console.log('\n🔍  Running preflight checks...\n');

  // 1. Test video file
  if (!fs.existsSync(TEST_VIDEO_PATH)) {
    console.error(`❌  Test video not found: ${TEST_VIDEO_PATH}`);
    console.error(`   Set TEST_VIDEO_PATH env var or place a file at:\n   ${TEST_VIDEO_PATH}`);
    console.error(`\n   Quick download (10s Big Buck Bunny clip):`);
    console.error(`   mkdir -p test-assets && curl -Lo test-assets/test-10s.mp4 \\`);
    console.error(`     "https://download.blender.org/peach/bigbuckbunny_movies/BigBuckBunny_320x180.mp4"`);
    process.exit(1);
  }

  const statBytes = fs.statSync(TEST_VIDEO_PATH).size;
  const fileSizeMB = statBytes / (1024 * 1024);
  console.log(`   ✅  Test video   : ${TEST_VIDEO_PATH}`);
  console.log(`   📦  File size    : ${fileSizeMB.toFixed(2)} MB`);

  // 2. API reachability
  try {
    const { statusCode, body } = await httpGet(`${API_BASE}/benchmark/ping`);
    console.log(`   ✅  API          : ${API_BASE}/benchmark/ping → ${statusCode} (dbQueryMs: ${body.dbQueryMs}ms)`);
  } catch (err) {
    console.error(`   ❌  API unreachable at ${API_BASE}`);
    console.error(`       Start it with: pnpm dev:api`);
    process.exit(1);
  }

  // 3. DB connectivity
  try {
    await prisma.$connect();
    const count = await prisma.video.count();
    console.log(`   ✅  Database     : connected (${count} existing videos)`);
  } catch (err) {
    console.error(`   ❌  Database connection failed:`, err);
    process.exit(1);
  }

  // 4. Redis connectivity (just try adding to legacy queue)
  try {
    await legacyQueue.getJobCounts();
    await rustQueue.getJobCounts();
    console.log(`   ✅  Redis        : connected to queues`);
  } catch (err) {
    console.error(`   ❌  Redis connection failed (localhost:6380):`, err);
    process.exit(1);
  }

  // 5. Seed user check
  const user = await prisma.user.findFirst({ where: { email: 'test2@example.com' } });
  if (!user) {
    console.error(`   ❌  Seed user test2@example.com not found.`);
    console.error(`       Run: tsx prisma/seed.ts`);
    process.exit(1);
  }
  console.log(`   ✅  Seed user    : ${user.email}`);

  console.log('\n✅  All preflight checks passed.\n');
  return fileSizeMB;
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log('');
  console.log('╔══════════════════════════════════════════════════╗');
  console.log('║   VeloStream Transcode Engine Benchmark          ║');
  console.log(`║   Concurrency: ${String(CONCURRENCY).padEnd(34)}║`);
  console.log('╚══════════════════════════════════════════════════╝');

  const fileSizeMB = await preflightChecks();

  // ── Phase 1: Node.js Legacy ──────────────────────────────────────────────
  const nodeResult = await runNodeLegacyBenchmark(fileSizeMB);

  // Brief cooldown between phases so RSS/latency samples don't bleed across
  console.log('\n⏸   Cooling down 5 seconds between phases...');
  await new Promise((r) => setTimeout(r, 5_000));

  // ── Phase 2: Rust Engine ─────────────────────────────────────────────────
  const rustResult = await runRustBenchmark(fileSizeMB);

  // ── Build Report ──────────────────────────────────────────────────────────
  const report: BenchmarkReport = {
    timestamp: new Date().toISOString(),
    concurrency: CONCURRENCY,
    testVideoPath: TEST_VIDEO_PATH,
    fileSizeMB,
    node: nodeResult,
    rust: rustResult,
  };

  // ── Write Output ──────────────────────────────────────────────────────────
  const jsonPath = writeJson(report);
  const csvPath = writeCsv(report);

  // ── Print Summary Table ───────────────────────────────────────────────────
  console.log('\n\n╔══════════════════════════════════════════════════════════════════╗');
  console.log('║                        BENCHMARK SUMMARY                        ║');
  console.log('╠══════════════════════════════════════════════════╦═══════════════╣');
  console.log('║ Metric                                           ║ Node  │ Rust  ║');
  console.log('╠══════════════════════════════════════════════════╬═══════════════╣');

  const fmt = (n: number, unit = '') => `${n.toFixed(0)}${unit}`.padStart(6);
  const fmtF = (n: number, unit = '') => `${n.toFixed(2)}${unit}`.padStart(7);

  const rows = [
    [`Avg Total Duration`, fmt(nodeResult.avgDurationMs, 'ms'), fmt(rustResult.avgDurationMs, 'ms')],
    [`Peak RSS Memory`, fmt(nodeResult.peakMemoryRssKb / 1024, 'MB'), fmt(rustResult.peakMemoryRssKb / 1024, 'MB')],
    [`API Latency P50`, fmt(nodeResult.apiLatencyP50Ms, 'ms'), fmt(rustResult.apiLatencyP50Ms, 'ms')],
    [`API Latency P95`, fmt(nodeResult.apiLatencyP95Ms, 'ms'), fmt(rustResult.apiLatencyP95Ms, 'ms')],
    [`Throughput`, fmtF(nodeResult.throughputMBps, 'MB/s'), fmtF(rustResult.throughputMBps, 'MB/s')],
    [`Jobs Completed`, String(nodeResult.jobs.length).padStart(6), String(rustResult.jobs.length).padStart(6)],
  ];

  for (const [label, nodeVal, rustVal] of rows) {
    console.log(`║ ${label.padEnd(48)} ║ ${nodeVal} │ ${rustVal} ║`);
  }
  console.log('╚══════════════════════════════════════════════════╩═══════════════╝');

  console.log(`\n📄  Results written to:`);
  console.log(`    JSON : ${jsonPath}`);
  console.log(`    CSV  : ${csvPath}`);
  console.log('\n   Load the CSV in a spreadsheet or use the charting tool of your choice.\n');

  await prisma.$disconnect();
  await legacyQueue.close();
  await rustQueue.close();
}

main().catch((err) => {
  console.error('❌  Benchmark failed:', err);
  process.exit(1);
});
