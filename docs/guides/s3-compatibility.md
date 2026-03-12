# S3 Compatibility

Shoebox implements a subset of the S3 API — enough for standard file operations with AWS CLI, rclone, and S3 SDKs. No custom clients needed.

## Supported Operations

### Object Operations

| Operation | Method | Path | Notes |
|-----------|--------|------|-------|
| GetObject | `GET` | `/{bucket}/{key}` | Range requests, conditional headers supported |
| PutObject | `PUT` | `/{bucket}/{key}` | Up to 5GB per request |
| DeleteObject | `DELETE` | `/{bucket}/{key}` | |
| DeleteObjects | `POST` | `/{bucket}?delete` | Batch delete, up to 1000 keys |
| HeadObject | `HEAD` | `/{bucket}/{key}` | Metadata without body |
| CopyObject | `PUT` | `/{bucket}/{key}` | Uses `x-amz-copy-source` header |
| ListObjectsV2 | `GET` | `/{bucket}?list-type=2` | Prefix, delimiter, pagination |

### Bucket Operations

| Operation | Method | Path |
|-----------|--------|------|
| ListBuckets | `GET` | `/` |
| HeadBucket | `HEAD` | `/{bucket}` |
| GetBucketLocation | `GET` | `/{bucket}?location` |
| GetBucketVersioning | `GET` | `/{bucket}?versioning` |

### Multipart Upload

| Operation | Method | Path |
|-----------|--------|------|
| InitiateMultipartUpload | `POST` | `/{bucket}/{key}?uploads` |
| UploadPart | `PUT` | `/{bucket}/{key}?partNumber=N&uploadId=ID` |
| CompleteMultipartUpload | `POST` | `/{bucket}/{key}?uploadId=ID` |
| AbortMultipartUpload | `DELETE` | `/{bucket}/{key}?uploadId=ID` |
| ListParts | `GET` | `/{bucket}/{key}?uploadId=ID` |
| ListMultipartUploads | `GET` | `/{bucket}?uploads` |

### Object Tagging

| Operation | Method | Path |
|-----------|--------|------|
| GetObjectTagging | `GET` | `/{bucket}/{key}?tagging` |
| PutObjectTagging | `PUT` | `/{bucket}/{key}?tagging` |
| DeleteObjectTagging | `DELETE` | `/{bucket}/{key}?tagging` |

### Bucket Configuration

| Operation | Method | Path |
|-----------|--------|------|
| GetBucketCors | `GET` | `/{bucket}?cors` |
| PutBucketCors | `PUT` | `/{bucket}?cors` |
| DeleteBucketCors | `DELETE` | `/{bucket}?cors` |
| GetBucketNotification | `GET` | `/{bucket}?notification` |
| PutBucketNotification | `PUT` | `/{bucket}?notification` |

### Shoebox Extensions

| Operation | Method | Path | Description |
|-----------|--------|------|-------------|
| Sync | `POST` | `/{bucket}?sync` | Trigger filesystem rescan |
| Duplicates | `GET` | `/{bucket}?duplicates` | Find duplicate files |
| Cross-bucket Duplicates | `GET` | `/?duplicates` | Find duplicates across all buckets |
| Duplicate Directories | `GET` | `/{bucket}?duplicate-dirs` | Find duplicate directories |
| Compare Directories | `GET` | `/?compare-dirs` | Compare two directories |
| Merge | `POST` | `/{bucket}?merge` | Merge duplicate files |
| Integrity Check | `GET` | `/{bucket}?integrity-check` | Verify file integrity |
| Integrity Status | `GET` | `/{bucket}?integrity-status` | Check async integrity status |
| Scan Status | `GET` | `/_shoebox/scan/status` | Background scanner status |
| Reload Config | `POST` | `/_shoebox/reload` | Reload credentials from disk |

## Not Supported

Shoebox intentionally omits enterprise S3 features that don't fit local-first storage:

- Object versioning
- Object lock / retention
- Lifecycle policies
- Server-side encryption
- ACLs / bucket policies
- S3 Select
- SNS/SQS notifications (webhooks instead)
- Replication

See [When Not to Use Shoebox](../when-not-to-use-shoebox.md) for details.

## Routing Styles

Shoebox supports both S3 routing conventions:

**Path-style** (default):
```
http://localhost:9000/photos/vacation/sunset.jpg
```

**Virtual-hosted style**:
```
http://photos.localhost:9000/vacation/sunset.jpg
```

Virtual-hosted style requires DNS or `/etc/hosts` entries pointing `*.localhost` to `127.0.0.1`. Most modern browsers resolve `*.localhost` automatically.

## Using with AWS CLI

### Configure a Profile

```bash
aws configure --profile shoebox
```

Enter the access key and secret from Shoebox's startup output:
```
AWS Access Key ID: AKIAFQA4RDZ3OQYV5VZF
AWS Secret Access Key: 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
Default region name: us-east-1
Default output format: json
```

### Common Operations

```bash
# List buckets
aws --profile shoebox --endpoint-url http://localhost:9000 s3 ls

# List objects
aws --profile shoebox --endpoint-url http://localhost:9000 s3 ls s3://photos/ --recursive

# Upload a file
aws --profile shoebox --endpoint-url http://localhost:9000 s3 cp photo.jpg s3://photos/

# Download a file
aws --profile shoebox --endpoint-url http://localhost:9000 s3 cp s3://photos/photo.jpg ./

# Sync a directory
aws --profile shoebox --endpoint-url http://localhost:9000 s3 sync ./local-folder s3://photos/

# Delete a file
aws --profile shoebox --endpoint-url http://localhost:9000 s3 rm s3://photos/old-photo.jpg
```

### Shell Alias

To avoid repeating `--profile` and `--endpoint-url`:

```bash
alias sb='aws --profile shoebox --endpoint-url http://localhost:9000'

sb s3 ls
sb s3 cp photo.jpg s3://photos/
sb s3 ls s3://photos/ --recursive
```

## Using with rclone

### Configure a Remote

```bash
rclone config create shoebox s3 \
  provider=Other \
  endpoint=http://localhost:9000 \
  access_key_id=AKIAFQA4RDZ3OQYV5VZF \
  secret_access_key=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
```

### Common Operations

```bash
# List buckets
rclone lsd shoebox:

# List files
rclone ls shoebox:photos

# Sync local to remote
rclone sync ./local-folder shoebox:photos

# Copy a single file
rclone copy photo.jpg shoebox:photos/

# Mount as filesystem (FUSE)
rclone mount shoebox:photos /mnt/photos
```

## Using with Python (boto3)

```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="AKIAFQA4RDZ3OQYV5VZF",
    aws_secret_access_key="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",
    region_name="us-east-1",
)

# List objects
response = s3.list_objects_v2(Bucket="photos")
for obj in response.get("Contents", []):
    print(obj["Key"], obj["Size"])

# Upload
s3.upload_file("photo.jpg", "photos", "vacation/photo.jpg")

# Download
s3.download_file("photos", "vacation/photo.jpg", "downloaded.jpg")
```

## Using with JavaScript (@aws-sdk/client-s3)

```javascript
import { S3Client, ListObjectsV2Command } from "@aws-sdk/client-s3";

const client = new S3Client({
  endpoint: "http://localhost:9000",
  region: "us-east-1",
  credentials: {
    accessKeyId: "AKIAFQA4RDZ3OQYV5VZF",
    secretAccessKey: "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",
  },
  forcePathStyle: true,
});

const response = await client.send(
  new ListObjectsV2Command({ Bucket: "photos" })
);

for (const obj of response.Contents ?? []) {
  console.log(obj.Key, obj.Size);
}
```

Note `forcePathStyle: true` — this tells the SDK to use path-style URLs (`localhost:9000/photos/key`) instead of virtual-hosted style (`photos.localhost:9000/key`).

## Region

Shoebox always reports `us-east-1` as the bucket region. Set your S3 client's region to `us-east-1` (or any value — Shoebox doesn't enforce region matching for signatures).

## See Also

- [Credentials](credentials.md) — Managing access keys and permissions
- [CORS](cors.md) — Enabling browser access
- [Pre-signed URLs](presigned-urls.md) — Temporary access links
