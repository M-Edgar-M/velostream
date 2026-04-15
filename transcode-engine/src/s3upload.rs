use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{Region, Credentials};
use aws_sdk_s3::primitives::ByteStream;
use std::fs;
use std::path::Path;

pub async fn upload_hls_to_minio(video_id: &str) -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("MINIO_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".to_string());
    let credentials = Credentials::new(
        "minioadmin",
        "minioadmin",
        None,
        None,
        "static",
    );
    
    let config = aws_config::from_env()
        .endpoint_url(&endpoint)
        .region(Region::new("us-east-1"))
        .credentials_provider(credentials)
        .load()
        .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&config)
        .force_path_style(true)
        .build();

    let client = S3Client::from_conf(s3_config);
    let bucket = "velostream-public"; 

    let output_dir = format!("outputs/{}", video_id);
    let dir_path = Path::new(&output_dir);

    if !dir_path.exists() {
        return Err(format!("Output directory {} does not exist", output_dir).into());
    }

    // 2. Iterate and Upload
    for entry in fs::read_dir(dir_path)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            let file_name = path.file_name().unwrap().to_str().unwrap();
            let key = format!("transcoded/{}/{}", video_id, file_name);

            // Set correct Content-Type so browsers can stream it
            let content_type = if file_name.ends_with(".m3u8") {
                "application/vnd.apple.mpegurl"
            } else if file_name.ends_with(".ts") {
                "video/MP2T"
            } else {
                "application/octet-stream"
            };

            println!("📤 Uploading: {}", file_name);

            let body = ByteStream::from_path(&path).await?;
            
            client.put_object()
                .bucket(bucket)
                .key(key)
                .body(body)
                .content_type(content_type)
                .send()
                .await?;
        }
    }

    println!("✅ All HLS segments uploaded for {}", video_id);
    Ok(())
}
