use dotenv::dotenv;
use std::env;
use redis::AsyncCommands;
use serde::{Serialize, Deserialize};
use std::process::Command;
use std::fs;
use tokio::time::{sleep, Duration};

#[derive(Debug, Serialize, Deserialize)]
struct JobData {
    #[serde(rename = "videoId")]
    video_id: String,
    #[serde(rename = "url")]
    url: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct BullJob {
    id: String,
    name: String,
    data: String,
}

#[derive(Serialize)]
struct CompletionRequest {
    status: String,
    hls_path: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6380".to_string());
    println!("Rust Engine connecting to Redis at {}", redis_url);

    let client = redis::Client::open(redis_url)?;
    let mut con = client.get_async_connection().await?;

    println!("Listening for transcodeing jobs...");

    loop {
        let job_id: Option<String> = con.rpop("bull:video-transcode:wait", None).await?;
        if let Some(id) = job_id {
            let job_key = format!("bull:video-transcode:{}", id);

            let job_hash: std::collections::HashMap<String, String> = con.hgetall(&job_key).await?;
            if let Some(data_str) = job_hash.get("data") {
                let data: JobData = serde_json::from_str(data_str)?;
                println!("Received job [{}]: Processing Video {}",id, data.video_id);
                process_video(data).await;
            }
        }
            sleep(Duration::from_millis(500)).await;
    }
}
async fn process_video(data: JobData) {

    let output_dir = format!("outputs/{}", data.video_id);


    if let Err(e) = fs::create_dir_all(&output_dir) {

        eprint!("Failed to create directory: {}", e);
        
        return;
    }

    println!("Starting HLS Transcode for {}...", data.video_id);


    let status = Command::new("ffmpeg")
        .arg("-i").arg(&data.url)
        .arg("-codec:v").arg("libx264")
        .arg("-codec:a").arg("aac")
        .arg("-map").arg("0")
        .arg("-f").arg("hls")
        .arg("-hls_time").arg("10")
        .arg("-hls_playlist_type").arg("event")
        .arg(format!("{}/playlist.m3u8", output_dir))
        .status();
        
    match status {
          Ok(s) if s.success() => {
            println!("Transcoding complete for {}", data.video_id);

            // handle async error manually
            if let Err(e) = notify_api_completion(&data.video_id).await {
                eprintln!("Failed to notify API: {}", e);
            }
        }
        Ok(_) => {
            eprintln!("FFmpeg exited with non-zero status for {}", data.video_id);
        }
        Err(e) => {
            eprintln!("Failed to start FFmpeg: {}", e);
        }
}
}

async fn notify_api_completion(video_id: &str) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();


    let hls_path = format!("transcoded/{}/playlist.m3u8", video_id);

    let payload = CompletionRequest {
        status: "COMPLETED".to_string(),
        hls_path,
    };


    let url = format!("http://localhost:3000/internal/video/{}/complete", video_id);


    let res = client.patch(url)
        .json(&payload)
        .send()
        .await?;

    if res.status().is_success() {
       println!("Successfully notified Node.js API for video {}", video_id); 

    } else {
        eprintln!("Failed to notify API. Status: {}", res.status());
    }
    Ok(())
}
