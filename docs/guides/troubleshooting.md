# Troubleshooting

## Connection Issues

### "Connection refused" when using AWS CLI or curl

The server isn't running, or it's on a different port.

```bash
# Check if Shoebox is running
curl http://localhost:9000/

# If using a custom port
curl http://localhost:8080/
```

**Fixes:**
- Start the server: `shoebox ~/Photos`
- Check the port: look at the startup output for "Serving N bucket(s) on http://..."
- If another process is using port 9000, use `--port <PORT>` to pick a different one

### "Port 9000 is already in use"

Another Shoebox instance (or another service) is using that port.

```bash
# Find what's using port 9000
lsof -i :9000

# Use a different port
shoebox --port 8080 ~/Photos
```

## Credential Errors

### "InvalidAccessKeyId"

The access key ID in your S3 client doesn't match any configured credential.

**Fixes:**
- List credentials to find the right key: `shoebox list-credentials ~/Photos`
- Re-check your AWS CLI profile: `aws configure --profile shoebox`
- If using a global config, make sure `--config` is passed when starting the server

### "SignatureDoesNotMatch"

The secret access key in your S3 client doesn't match what Shoebox expects.

**Fixes:**
- Show the secret: `shoebox --show-secrets ~/Photos` (start the server briefly to see credentials)
- Re-configure your client with the correct secret
- Check for trailing whitespace or newlines in your AWS credentials file

### "AccessDenied"

The credential is valid but doesn't have permission for this operation.

**Fixes:**
- Check the credential's permissions: `shoebox list-credentials ~/Photos`
- A `read`-only credential can't upload or delete. See [Credentials](credentials.md) for the permission model.
- Use an `admin` credential for full access

## Files Not Appearing

### "I added files to the directory but they don't show in S3 listings"

Shoebox discovers files through its background scanner. After adding files directly to the filesystem:

1. **Wait a moment** — the filesystem watcher detects changes and triggers a scan. Files typically appear within seconds.
2. **Trigger a manual sync**:
   ```bash
   curl -X POST http://localhost:9000/photos?sync \
     --aws-sigv4 "aws:amz:us-east-1:s3" \
     --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY"
   ```
3. **Check scan status**:
   ```bash
   curl http://localhost:9000/_shoebox/scan/status
   ```

### "Duplicate detection shows no results"

Duplicate detection requires L3 (content hash) scanning to complete. This happens in the background and can take time for large buckets.

- Use `--allow-partial` to see results from files already hashed
- Check scan progress: `curl http://localhost:9000/_shoebox/scan/status`
- Content hashing speed depends on disk I/O — an HDD will be slower than an SSD

## CORS Errors

### "Access-Control-Allow-Origin header is missing" in the browser

CORS rules haven't been configured for this bucket.

**Fix:** Configure CORS rules. See [CORS](cors.md) for details.

```bash
aws --profile shoebox --endpoint-url http://localhost:9000 \
  s3api put-bucket-cors --bucket photos --cors-configuration '{
    "CORSRules": [{
      "AllowedOrigins": ["*"],
      "AllowedMethods": ["GET", "PUT", "POST", "DELETE", "HEAD"],
      "AllowedHeaders": ["*"],
      "ExposeHeaders": ["ETag", "x-amz-request-id"],
      "MaxAgeSeconds": 3600
    }]
  }'
```

### "The 'Access-Control-Allow-Origin' header has a value that is not equal to the supplied origin"

The origin in your browser doesn't match `AllowedOrigins` in the CORS rule. Check:
- Your app's origin includes the protocol and port: `http://localhost:3000`, not just `localhost:3000`
- The CORS rule uses `*` or includes the exact origin

## Configuration Issues

### "permission denied; use --data-dir to store state elsewhere"

The bucket directory is read-only (e.g., a mounted NAS share). Shoebox needs to write `.shoebox/config.toml` and `metadata.db`.

**Fix:** Use `--data-dir` to store state in a writable location:

```bash
shoebox --data-dir /var/lib/shoebox /mnt/nas/photos
```

### Validating Configuration

Run the `validate` command to check for common issues:

```bash
shoebox validate ~/Photos
```

This checks:
- Path exists and is a valid directory
- Bucket name is valid
- Credentials are properly formatted (no duplicates, correct key format)
- CORS rules use recognized methods
- Webhook URLs are valid and preferably HTTPS

## Debugging

### Enable Debug Logging

```bash
SHOEBOX_LOG=debug shoebox ~/Photos
```

For even more detail:

```bash
SHOEBOX_LOG=trace shoebox ~/Photos
```

To focus on specific modules:

```bash
# Only debug Shoebox's scanner
SHOEBOX_LOG=shoebox::scanner=debug,info shoebox ~/Photos

# Debug auth issues
SHOEBOX_LOG=shoebox::auth=debug,info shoebox ~/Photos
```

### Inspecting the Database

Each bucket's metadata is stored in SQLite. You can query it directly:

```bash
sqlite3 ~/Photos/.shoebox/metadata.db

# List all objects
SELECT key, size, scan_level, checksum_sha256 FROM objects LIMIT 20;

# Check scan progress
SELECT scan_level, COUNT(*) FROM objects GROUP BY scan_level;

# Find objects with missing hashes
SELECT key FROM objects WHERE scan_level < 3;
```

## FAQ

### How long until new files appear in listings?

Files added directly to the filesystem are detected by the filesystem watcher and typically appear within seconds. Files added via the S3 API (PUT) appear immediately.

### Can I edit files directly on disk?

Yes. Edit them with any tool. Shoebox detects changes via the filesystem watcher and updates metadata. If you want to force an immediate rescan, use `POST /{bucket}?sync`.

### Does it work on NFS/SMB network shares?

Yes, but filesystem watching may be slower or unavailable depending on the network filesystem. On NFS, `inotify` events may not be generated — Shoebox falls back to periodic scanning. Use `POST /{bucket}?sync` to trigger manual rescans.

### What's the maximum file size?

- Single PUT: 5GB (S3 protocol limit)
- Multipart upload: no practical limit — files are uploaded in parts

### Can I run multiple Shoebox instances?

Yes, as long as they use different ports and don't share the same `.shoebox/` directory. Use `--port` to assign different ports.

### How do I reset a bucket's metadata?

Delete the `.shoebox/metadata.db` file and restart Shoebox. It will re-scan and rebuild the metadata. Your credentials in `config.toml` are preserved.

```bash
rm ~/Photos/.shoebox/metadata.db
shoebox ~/Photos
```

## See Also

- [Configuration](configuration.md) — All configuration options
- [Credentials](credentials.md) — Managing access keys
- [CLI Reference](cli-reference.md) — All commands and flags
