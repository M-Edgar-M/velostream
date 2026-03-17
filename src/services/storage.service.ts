import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { v4 as uuidv4 } from "uuid";

// Initialize S3 Client for MinIO
const s3Client = new S3Client({
  endpoint: process.env.MINIO_ENDPOINT, // http://localhost:9000
  credentials: {
    accessKeyId: process.env.MINIO_ACCESS_KEY!,
    secretAccessKey: process.env.MINIO_SECRET_KEY!,
  },
  forcePathStyle: true, // Required for MinIO
  region: "us-east-1",
});

export class StorageService {
  private bucket = process.env.MINIO_BUCKET_NAME!;

  async generateUploadUrl(userId: string, fileName: string) {
    const videoId = uuidv4();
    const extension = fileName.split('.').pop();
    
    // Nested structure: uploads/{userId}/{videoId}/original.{ext}
    const storageKey = `uploads/${userId}/${videoId}/original.${extension}`;

    const command = new PutObjectCommand({
      Bucket: this.bucket,
      Key: storageKey,
      ContentType: "video/mp4", // TODO: make this dynamic later
    });

    // URL valid for 15 minutes (900 seconds)
    const presignedUrl = await getSignedUrl(s3Client, command, { expiresIn: 900 });

    return {
      videoId,
      storageKey,
      presignedUrl,
    };
  }
}
