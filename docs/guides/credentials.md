# Credentials & Authentication

Shoebox uses AWS Signature V4 (SigV4) authentication — the same protocol used by AWS S3. Any S3 client (AWS CLI, rclone, SDKs) that supports SigV4 works with Shoebox without modification.

## How It Works

1. You start Shoebox. It generates an access key and secret for each bucket.
2. You configure your S3 client with those credentials.
3. Every request is signed with the secret key. Shoebox verifies the signature.

No accounts, no IAM policies, no tokens to rotate. Credentials live in `.shoebox/config.toml` alongside your files.

## Auto-Generated Credentials

On first run, Shoebox creates an admin credential for each bucket:

```
  photos -> /home/user/Photos
    (new) Credentials generated:
      [1] AKIAFQA4RDZ3OQYV5VZF (Full access (admin))
          Secret: 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
```

This credential has full access. The secret is shown once on first run, then hidden on subsequent starts. Use `--show-secrets` to display it again:

```bash
shoebox --show-secrets ~/Photos
```

## Permission Model

Shoebox has four permission levels:

| Permission | What It Allows |
|-----------|----------------|
| `admin` | Everything — read, write, sync, credential management, CORS, webhooks |
| `read` | List buckets, list objects, download objects, head objects, list multipart uploads/parts |
| `write` | Upload objects, delete objects, multipart upload operations |
| `sync` | Trigger filesystem rescan (`POST /{bucket}?sync`) |

Permissions are additive. A credential with `read,write` can do both. A credential with no permissions listed (legacy format) is treated as admin.

### What Each Permission Covers

**read** operations:
- `ListBuckets`, `HeadBucket`, `GetBucketLocation`, `GetBucketVersioning`
- `ListObjectsV2`, `GetObject`, `HeadObject`
- `ListParts`, `ListMultipartUploads`

**write** operations:
- `PutObject`, `DeleteObject`, `DeleteObjects`
- `InitiateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`

**sync** operations:
- `SyncBucket` (the `POST /{bucket}?sync` endpoint)

**admin** includes all of the above plus:
- Credential CRUD endpoints
- CORS configuration
- Webhook configuration
- Duplicate detection and integrity checks

## Managing Credentials

### Add a Credential

```bash
# Read-only access for a gallery app
shoebox add-credential ~/Photos --permissions read --description "Gallery viewer"

# Read + write for an upload service
shoebox add-credential ~/Photos --permissions read,write --description "Upload service"

# Sync-only for a cron job
shoebox add-credential ~/Photos --permissions sync --description "Nightly rescan"
```

### List Credentials

```bash
shoebox list-credentials ~/Photos
```

```
Credentials for /home/user/Photos:
  [1] AKIAFQA4RDZ3OQYV5VZF (Full access (admin)) [admin]
  [2] AKIAW7NEXAMPLE12345 (Gallery viewer) [read]
  [3] AKIAX9MEXAMPLE67890 (Upload service) [read,write]
```

### Remove a Credential

```bash
shoebox remove-credential ~/Photos AKIAW7NEXAMPLE12345
```

If a server is running, you'll need to reload credentials for the change to take effect:

```bash
curl -X POST http://localhost:9000/_shoebox/reload
```

## Global vs Per-Bucket Credentials

**Per-bucket credentials** live in each bucket's `.shoebox/config.toml` and only work for that bucket. This is the default.

**Global credentials** are defined in a global config file and work across all buckets:

```toml
# /etc/shoebox.toml
[[credentials]]
access_key_id = "AKIAGLOBAL1234567890"
secret_access_key = "globalSecretKey1234567890123456789012345"
description = "Cross-bucket admin"
```

```bash
shoebox --config /etc/shoebox.toml ~/Photos ~/Documents
```

Global credentials are useful when you need one set of keys to access multiple buckets — for example, a backup script that reads from all buckets.

## Credential Format

Credentials follow the AWS S3 format:

- **Access Key ID**: 20 characters, starts with `AKIA` (e.g., `AKIAFQA4RDZ3OQYV5VZF`)
- **Secret Access Key**: 40 characters, base64-like charset (e.g., `RKib+TOJvhxuTTwkv+ZWszjncIBTqKcRTVR8a+Ya`)

Shoebox generates these automatically. You don't need to create them by hand.

## Credential Storage

Credentials are stored in plain text in `.shoebox/config.toml`. On Unix systems, the file is created with `0600` permissions (owner read/write only).

```toml
[[credentials]]
access_key_id = "AKIAFQA4RDZ3OQYV5VZF"
secret_access_key = "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"
description = "Full access (admin)"

[[credentials]]
access_key_id = "AKIAW7NEXAMPLE12345"
secret_access_key = "anotherSecretKey1234567890123456789012"
description = "Gallery viewer"
permissions = ["read"]
```

## Reloading Credentials

When you add or remove credentials while the server is running, the server doesn't pick up the changes automatically. Two options:

1. **Restart the server** — simplest option.
2. **Hot reload** — call the reload endpoint:

```bash
curl -X POST http://localhost:9000/_shoebox/reload
```

This reloads all bucket configs and global credentials from disk without downtime.

## See Also

- [Configuration](configuration.md) — Global config, per-bucket config, environment variables
- [S3 Compatibility](s3-compatibility.md) — Setting up AWS CLI, rclone, and SDKs with credentials
- [Pre-signed URLs](presigned-urls.md) — Temporary access without sharing credentials
