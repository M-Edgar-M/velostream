mod s3upload;

use dotenv::dotenv;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::Semaphore;
use tokio::time::{Duration, sleep};

/// Max number of ffmpeg processes running at the same time.
/// Each transcode is CPU/IO-heavy, so 3 is a safe default for a dev machine.
const MAX_CONCURRENT: usize = 3;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct JobData {
    #[serde(rename = "videoId")]
    video_id: String,
    #[serde(rename = "url")]
    url: String,
}

#[derive(Serialize)]
struct StatusUpdate {
    status: String,
    hls_path: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6380".to_string());
    let api_url = Arc::new(
        env::var("API_URL").unwrap_or_else(|_| "http://localhost:3000".to_string()),
    );

    println!("Rust worker | Redis: {} | API: {}", redis_url, api_url);

    let client = redis::Client::open(redis_url)?;

    // Verify Redis is reachable before entering the loop
    {
        let mut probe = client.get_async_connection().await?;
        let pong: String = redis::cmd("PING").query_async(&mut probe).await?;
        println!("Redis PING → {}", pong);
    }

    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT));
    // Dedicated connection for the polling loop
    let mut poll_con = client.get_async_connection().await?;

    println!("Listening for transcode jobs (max {} concurrent)...", MAX_CONCURRENT);

    loop {
        // RPOP from BullMQ's wait list
        let pop_result: redis::RedisResult<Option<String>> =
            poll_con.rpop("bull:video-transcode:wait", None).await;

        let job_id = match pop_result {
            Ok(opt) => opt,
            Err(e) => {
                eprintln!("Redis poll error (will retry): {}", e);
                // Reconnect on next iteration
                match client.get_async_connection().await {
                    Ok(new_con) => poll_con = new_con,
                    Err(e2) => eprintln!("Reconnect failed: {}", e2),
                }
                sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        if let Some(id) = job_id {
            let job_key = format!("bull:video-transcode:{}", id);
            let job_hash: std::collections::HashMap<String, String> =
                match poll_con.hgetall(&job_key).await {
                    Ok(h) => h,
                    Err(e) => {
                        eprintln!("Could not read job hash {}: {}", job_key, e);
                        continue;
                    }
                };

            let data_str = match job_hash.get("data") {
                Some(s) => s.clone(),
                None => {
                    eprintln!("Job {} has no 'data' field in hash, skipping", id);
                    continue;
                }
            };

            let data: JobData = match serde_json::from_str(&data_str) {
                Ok(d) => d,
                Err(e) => {
                    eprintln!("Failed to parse job data for {}: {}", id, e);
                    continue;
                }
            };

            println!(
                "[{}] Picked up job [{}] — acquiring slot ({}/{} busy)",
                data.video_id,
                id,
                MAX_CONCURRENT - semaphore.available_permits(),
                MAX_CONCURRENT
            );

            // Acquire a concurrency slot — if all slots are taken this blocks the
            // poll loop, which is intentional: we won't RPOP more work than we
            // can handle.
            let permit = semaphore.clone().acquire_owned().await?;
            let api_url_clone = api_url.clone();

            tokio::spawn(async move {
                let _permit = permit; // released automatically when task ends

                let video_id = &data.video_id;
                println!("[{}] Starting ffmpeg transcode...", video_id);

                if process_video(&data).await {
                    println!("[{}] Transcode complete. Uploading HLS to MinIO...", video_id);

                    // Convert the Box<dyn Error> (!Send) to a String before any .await
                    let upload_result = s3upload::upload_hls_to_minio(video_id)
                        .await
                        .map_err(|e| e.to_string());

                    match upload_result {
                        Ok(_) => {
                            println!("[{}] Upload done. Notifying API...", video_id);
                            if let Err(e) = notify_api(&api_url_clone, video_id, true).await {
                                eprintln!("[{}] Failed to notify API (COMPLETED): {}", video_id, e);
                            }
                        }
                        Err(err_msg) => {
                            eprintln!("[{}] ❌ MinIO upload failed: {}", video_id, err_msg);
                            let _ = notify_api(&api_url_clone, video_id, false).await;
                        }
                    }
                } else {
                    eprintln!("[{}] ❌ ffmpeg failed.", video_id);
                    let _ = notify_api(&api_url_clone, video_id, false).await;
                }

                // Best-effort cleanup of local segment files
                let output_dir = format!("outputs/{}", video_id);
                if let Err(e) = fs::remove_dir_all(&output_dir) {
                    eprintln!("[{}] ⚠️  Cleanup failed: {}", video_id, e);
                }
            });
        } else {
            // Queue is empty — sleep before polling again
            sleep(Duration::from_millis(500)).await;
        }
    }
}

async fn process_video(data: &JobData) -> bool {
    let output_dir = format!("outputs/{}", data.video_id);

    if let Err(e) = fs::create_dir_all(&output_dir) {
        eprintln!("[{}] Could not create output dir: {}", data.video_id, e);
        return false;
    }

    let status = Command::new("ffmpeg")
        .arg("-i")
        .arg(&data.url)
        .arg("-codec:v").arg("libx264")
        .arg("-codec:a").arg("aac")
        .arg("-map").arg("0")
        .arg("-f").arg("hls")
        .arg("-hls_time").arg("6")
        .arg("-hls_playlist_type").arg("vod")   // finalised m3u8, seekable immediately
        .arg("-hls_segment_filename")
        .arg(format!("{}/segment%04d.ts", output_dir))
        .arg(format!("{}/playlist.m3u8", output_dir))
        .status()
        .await;

    match status {
        Ok(s) if s.success() => {
            println!("[{}] ffmpeg exited OK", data.video_id);
            true
        }
        Ok(s) => {
            eprintln!("[{}] ffmpeg exited with code {:?}", data.video_id, s.code());
            false
        }
        Err(e) => {
            eprintln!("[{}] ffmpeg spawn error: {}", data.video_id, e);
            false
        }
    }
}

async fn notify_api(
    api_url: &str,
    video_id: &str,
    success: bool,
) -> Result<(), String> {
    let client = reqwest::Client::new();
    let url = format!("{}/internal/video/{}/complete", api_url, video_id);

    let payload = StatusUpdate {
        status: if success { "COMPLETED".to_string() } else { "FAILED".to_string() },
        hls_path: if success {
            format!("transcoded/{}/playlist.m3u8", video_id)
        } else {
            String::new()
        },
    };

    let res = client
        .patch(&url)
        .json(&payload)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if res.status().is_success() {
        println!("[{}] API notified → {}", video_id, payload.status);
    } else {
        eprintln!(
            "[{}] API notification failed: HTTP {} ({})",
            video_id,
            res.status(),
            url
        );
    }
    Ok(())
}
