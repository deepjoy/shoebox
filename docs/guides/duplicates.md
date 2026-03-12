# Duplicate Detection

When your object store knows the content hash of every file, finding duplicates is a query, not a project. Shoebox computes SHA-256 hashes for all files in the background and exposes duplicate detection through both the CLI and the S3 API.

## How It Works

Shoebox scans files in three levels:

| Level | What It Does | Speed |
|-------|-------------|-------|
| **L1 — Discovery** | Finds files on disk, records names and paths | Fast (filesystem walk) |
| **L2 — Metadata** | Reads mtime, ctime, inode, device ID | Fast (stat calls) |
| **L3 — Content** | Computes MD5, SHA-256, SHA-1, CRC32, CRC32C | Slow (reads every byte) |

L1 runs at startup so files are immediately listable. L2 and L3 run in the background. Duplicate detection requires L3 — it compares SHA-256 hashes, so two files are duplicates only if their content is byte-for-byte identical.

## Finding Duplicates

### CLI

```bash
shoebox duplicates ~/Photos
```

```
Found 3 duplicate group(s) in bucket 'photos':

  Group 1 — a1b2c3d4e5f6g7h8 (4194304 bytes each, 4194304 wasted):
    vacation/sunset.jpg (550e8400-e29b-41d4-a716-446655440000)
    backup/sunset_copy.jpg (6ba7b810-9dad-11d1-80b4-00c04fd430c8)

  Group 2 — f9e8d7c6b5a43210 (1048576 bytes each, 2097152 wasted):
    photos/portrait.jpg (12345678-1234-1234-1234-123456789abc)
    imports/portrait.jpg (87654321-4321-4321-4321-cba987654321)
    old/portrait_v1.jpg (abcdef01-2345-6789-abcd-ef0123456789)
```

Each group shows files that share the same SHA-256 hash. "Wasted" is the space used by extra copies beyond the first.

### JSON Output

```bash
shoebox duplicates ~/Photos --format json
```

Returns a structured report with `duplicates` array, `scan_complete` flag, and per-group details.

### API

```bash
# Per-bucket duplicates
curl http://localhost:9000/photos?duplicates \
  --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY"

# Cross-bucket duplicates (all buckets)
curl http://localhost:9000/?duplicates \
  --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY"
```

## Scan Completion

Duplicate detection only works after L3 scanning completes. If the scan is still running:

```
Warning: Scan incomplete — results may be partial.
```

Options:
- **Wait** for the scan to finish (happens automatically in the background).
- **Use `--allow-partial`** to see results from files that have been hashed so far.
- **Check scan progress** via the API: `GET /_shoebox/scan/status`.

For a fresh bucket with thousands of files, L3 scanning may take minutes to hours depending on total file size and disk speed.

## Comparing Directories

Compare two directories to see which files are identical, different, or only exist in one side:

```bash
shoebox compare-dirs ~/Photos/vacation ~/Backups/vacation
```

```
Comparing photos/vacation/ vs backups/vacation/
Identical: false
  Files identical: 42
  Only in left: 3
  Only in right: 1
  Different content: 2

Differences:
  edited.jpg — different_content
  new-photo.jpg — only_in_left
```

This works across different buckets — the directories don't need to be under the same parent.

### API

```bash
curl "http://localhost:9000/?compare-dirs&left-bucket=photos&left-prefix=vacation/&right-bucket=backups&right-prefix=vacation/" \
  --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY"
```

## Duplicate Directories

Find entire directories that are duplicates of each other:

```bash
curl http://localhost:9000/photos?duplicate-dirs \
  --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY"
```

This compares the combined content hash of all files within each directory to find directories with identical contents.

## Merging Duplicates

Once you've identified duplicates, you can merge them — keeping one file and deleting the rest:

```bash
curl -X POST "http://localhost:9000/photos?merge" \
  --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "winner": "vacation/sunset.jpg",
    "losers": ["backup/sunset_copy.jpg"]
  }'
```

The "winner" file is kept. The "loser" files are deleted from both disk and metadata. This is irreversible — make sure you want to delete the losers before merging.

## See Also

- [Integrity Checking](integrity.md) — Verifying files haven't been corrupted
- [CLI Reference](cli-reference.md) — Full `duplicates` and `compare-dirs` command options
