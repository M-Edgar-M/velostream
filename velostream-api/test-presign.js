const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');

const presignClient = new S3Client({
  endpoint: "http://localhost:9000",
  credentials: { accessKeyId: 'minioadmin', secretAccessKey: 'minioadmin' },
  region: 'us-east-1',
  forcePathStyle: true,
});

async function main() {
  const command = new PutObjectCommand({
    Bucket: 'velostream-uploads',
    Key: 'test-key',
    ContentType: 'video/mp4',
  });
  const url = await getSignedUrl(presignClient, command, { expiresIn: 3600 });
  console.log(url);
}
main();
