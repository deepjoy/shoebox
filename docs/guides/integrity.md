# Integrity Checking

Hard drives fail. Bits rot. Files get silently corrupted during transfers. Shoebox detects these problems by rehashing files on disk and comparing against stored checksums.

## How It Works

During the L3 background scan, Shoebox computes SHA-256 (and other) hashes for every file and stores them in its metadata database. An integrity check re-reads the file, recomputes the hash, and compares:

- **Match**: The file is intact.
- **Mismatch**: The file content has changed since it was last hashed — either modified intentionally or corrupted.
- **Missing**: The metadata has a record but the file no longer exists on disk.

## Running a Check

### CLI

```bash
shoebox integrity-check ~/Photos
```

```
Integrity Check: a1b2c3d4-e5f6-7890-abcd-ef1234567890
Status: completed
Files checked: 1247 (8589934592 bytes)
Files OK: 1245
Discrepancies (2):
  vacation/corrupt.jpg — hash_mismatch (550e8400-e29b-41d4-a716-446655440000)
  old/missing.txt — file_not_found (6ba7b810-9dad-11d1-80b4-00c04fd430c8)
```

### Scoped Check

Check only files under a specific prefix:

```bash
shoebox integrity-check ~/Photos --scope vacation/
```

### JSON Output

```bash
shoebox integrity-check ~/Photos --format json
```

Returns structured data with `files_checked`, `bytes_checked`, `files_ok`, and a `discrepancies` array.

### API (Synchronous)

```bash
curl http://localhost:9000/photos?integrity-check \
  --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY"
```

This blocks until the check completes. For large buckets, use the async endpoint instead.

### API (Asynchronous)

Start a check in the background:

```bash
# Start async check
curl -X POST http://localhost:9000/photos?integrity-check \
  --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY"

# Check status
curl http://localhost:9000/photos?integrity-status \
  --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY"
```

## Understanding Results

| Reason | What It Means | Action |
|--------|--------------|--------|
| `hash_mismatch` | File content has changed since last L3 scan | Investigate: was the file edited intentionally? If not, restore from backup. |
| `file_not_found` | Metadata exists but file is missing from disk | File was deleted outside the S3 API. Run `POST /{bucket}?sync` to update metadata. |

A `hash_mismatch` on a file you didn't modify is a strong signal of disk corruption or bit rot. Consider:

1. Replacing the file from a known-good backup.
2. Checking disk health with `smartctl` or similar tools.
3. Running checks more frequently on aging hardware.

## Scheduled Checks

Shoebox runs automatic integrity checks every 24 hours while the server is running. No configuration needed — this happens in the background without affecting normal operations.

## Prerequisites

Integrity checking requires L3 (content hash) scanning to have completed for the files being checked. If L3 scanning is still in progress, only files that have been hashed so far will be checked.

## See Also

- [Duplicate Detection](duplicates.md) — Finding duplicate files using the same content hashes
- [CLI Reference](cli-reference.md) — Full `integrity-check` command options
- [Troubleshooting](troubleshooting.md) — What to do when integrity checks find problems
