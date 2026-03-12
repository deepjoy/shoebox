# CLI Reference

Shoebox runs in two modes: **serve mode** (default) starts the S3-compatible server, and **subcommands** perform offline operations on buckets.

## Serve Mode

```bash
shoebox [OPTIONS] <PATH>...
```

Point Shoebox at one or more directories. Each directory becomes an S3 bucket.

```bash
# Single bucket
shoebox ~/Photos

# Multiple buckets
shoebox ~/Photos ~/Documents ~/Backups

# Custom host and port
shoebox --host 127.0.0.1 --port 8080 ~/Photos

# Show credentials on startup (including secrets)
shoebox --show-secrets ~/Photos

# Store state outside the bucket directory (for read-only mounts)
shoebox --data-dir /var/lib/shoebox ~/Photos

# Use a global config file
shoebox --config /etc/shoebox.toml
```

### Serve Options

| Flag | Env Variable | Default | Description |
|------|-------------|---------|-------------|
| `--host <HOST>` | `SHOEBOX_HOST` | `0.0.0.0` | Listen address |
| `--port <PORT>` | `SHOEBOX_PORT` | `9000` | Listen port |
| `--show-secrets` | — | `false` | Print secret access keys on startup |
| `--data-dir <DIR>` | `SHOEBOX_DATA_DIR` | — | Store per-bucket state in `<DIR>/<bucket>/` instead of `<bucket>/.shoebox/` |
| `--config <PATH>` | `SHOEBOX_CONFIG` | — | Path to global config file |

### Startup Output

```
  photos -> /home/user/Photos
    (new) Credentials generated:
      [1] AKIAFQA4RDZ3OQYV5VZF (Full access (admin))
          Secret: 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

Serving 1 bucket on http://0.0.0.0:9000
Credentials saved to .shoebox/config.toml
Use --show-secrets to display secret access keys
```

On first run, Shoebox generates an admin credential and saves it to `.shoebox/config.toml`. On subsequent runs, secrets are hidden unless `--show-secrets` is passed.

---

## Credential Management

### add-credential

Add a new credential to a bucket.

```bash
shoebox add-credential <BUCKET_PATH> [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--permissions <PERMS>` | `admin` | Comma-separated: `read`, `write`, `sync`, `admin` |
| `--description <DESC>` | — | Human-readable label |
| `--port <PORT>` | `9000` | Port to check for a running server |

```bash
# Add a read-only credential
shoebox add-credential ~/Photos --permissions read --description "Gallery viewer"

# Add a credential that can read and write
shoebox add-credential ~/Photos --permissions read,write --description "App uploads"
```

Output:

```
Credential added:
  Access Key ID: AKIAW7NEXAMPLE12345
  Secret Access Key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
  Permissions: read
  Description: Gallery viewer
```

If a Shoebox server is running on the specified port, you'll see a reminder to reload credentials:

```
Warning: A Shoebox server appears to be running on port 9000.
  Changes will take effect on next restart, or call:
  curl -X POST http://localhost:9000/_shoebox/reload
```

### list-credentials

List all credentials for a bucket.

```bash
shoebox list-credentials <BUCKET_PATH>
```

Output:

```
Credentials for /home/user/Photos:
  [1] AKIAFQA4RDZ3OQYV5VZF (Full access (admin)) [admin]
  [2] AKIAW7NEXAMPLE12345 (Gallery viewer) [read]
```

### remove-credential

Remove a credential by its access key ID.

```bash
shoebox remove-credential <BUCKET_PATH> <ACCESS_KEY_ID>
```

```bash
shoebox remove-credential ~/Photos AKIAW7NEXAMPLE12345
# Credential AKIAW7NEXAMPLE12345 removed
```

---

## Object Management

### rename (alias: mv)

Rename or move an object within a bucket. The operation is atomic — the file is renamed on disk and the metadata is updated in a single transaction.

```bash
shoebox rename <BUCKET_PATH> <SOURCE_KEY> <DEST_KEY> [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--overwrite` | `false` | Overwrite the destination if it exists |

```bash
# Rename a file
shoebox rename ~/Photos vacation/IMG_001.jpg vacation/sunset.jpg

# Move to a different directory
shoebox rename ~/Photos old/file.txt archive/file.txt

# Overwrite an existing file
shoebox rename ~/Photos draft.txt final.txt --overwrite
```

---

## Pre-signed URLs

### presign get

Generate a temporary download URL that works without credentials.

```bash
shoebox presign get <BUCKET> <KEY> [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--expires <DURATION>` | `1h` | Expiration: `30m`, `1h`, `7d`, etc. |
| `--endpoint <URL>` | `http://localhost:9000` | Server endpoint for the URL |
| `--bucket-path <PATH>` | — | Path to bucket directory (required) |

```bash
shoebox presign get photos vacation/sunset.jpg --bucket-path ~/Photos --expires 7d
# https://localhost:9000/photos/vacation/sunset.jpg?X-Amz-Algorithm=...
```

### presign put

Generate a temporary upload URL.

```bash
shoebox presign put <BUCKET> <KEY> [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--expires <DURATION>` | `1h` | Expiration: `30m`, `1h`, `7d`, etc. |
| `--endpoint <URL>` | `http://localhost:9000` | Server endpoint for the URL |
| `--bucket-path <PATH>` | — | Path to bucket directory (required) |
| `--content-type <TYPE>` | — | Required content type for the upload |

```bash
shoebox presign put photos uploads/new-file.jpg \
  --bucket-path ~/Photos \
  --expires 1h \
  --content-type image/jpeg
```

**Duration format:** A number followed by `m` (minutes), `h` (hours), or `d` (days).

---

## Duplicate Detection

### duplicates

Find duplicate files in a bucket based on content hashes.

```bash
shoebox duplicates <BUCKET_PATH> [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--max-results <N>` | `100` | Maximum duplicate groups to return |
| `--allow-partial` | `false` | Show results even if the content scan is incomplete |
| `--format <FORMAT>` | `table` | Output format: `table` or `json` |

```bash
shoebox duplicates ~/Photos
```

Output:

```
Found 3 duplicate group(s) in bucket 'photos':

  Group 1 — a1b2c3d4e5f6g7h8 (4194304 bytes each, 4194304 wasted):
    vacation/sunset.jpg (550e8400-e29b-41d4-a716-446655440000)
    backup/sunset_copy.jpg (6ba7b810-9dad-11d1-80b4-00c04fd430c8)
```

If the L3 (content hash) scan hasn't completed, you'll see:

```
Warning: Scan incomplete — results may be partial.
```

Use `--allow-partial` to see whatever results are available. Otherwise, wait for the scan to finish — Shoebox runs L3 content hashing in the background after startup.

### compare-dirs

Compare two directories across buckets. Each argument is a path where the existing directory portion is the bucket and the remainder is the prefix.

```bash
shoebox compare-dirs <LEFT> <RIGHT> [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--format <FORMAT>` | `table` | Output format: `table` or `json` |

```bash
shoebox compare-dirs ~/Photos/vacation ~/Backups/vacation
```

Output:

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

---

## Integrity Checking

### integrity-check

Verify that files on disk still match their stored content hashes. Detects bit rot, silent corruption, and files modified outside the S3 API.

```bash
shoebox integrity-check <BUCKET_PATH> [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--scope <PREFIX>` | — | Only check objects with this key prefix |
| `--format <FORMAT>` | `table` | Output format: `table` or `json` |

```bash
# Check all files
shoebox integrity-check ~/Photos

# Check only a subdirectory
shoebox integrity-check ~/Photos --scope vacation/
```

Output:

```
Integrity Check: a1b2c3d4-e5f6-7890-abcd-ef1234567890
Status: completed
Files checked: 1247 (8589934592 bytes)
Files OK: 1245
Discrepancies (2):
  vacation/corrupt.jpg — hash_mismatch (550e8400-e29b-41d4-a716-446655440000)
  old/missing.txt — file_not_found (6ba7b810-9dad-11d1-80b4-00c04fd430c8)
```

---

## Validation

### validate

Check a bucket's configuration for common issues: path validity, bucket name, credential format, CORS rules, and webhook configs.

```bash
shoebox validate <BUCKET_PATH> [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--format <FORMAT>` | `table` | Output format: `table` or `json` |

```bash
shoebox validate ~/Photos
```

Output:

```
Validating bucket: photos (/home/user/Photos)

  [PASS] Bucket name "photos" is valid
  [PASS] 2 credential(s) configured
  [WARN] No CORS rules configured
  [PASS] No webhooks configured

Result: 2 passed, 1 warnings, 0 errors
```

Exit code is `1` if any errors are found, `0` otherwise. Warnings don't affect the exit code.

---

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SHOEBOX_HOST` | Listen address | `0.0.0.0` |
| `SHOEBOX_PORT` | Listen port | `9000` |
| `SHOEBOX_DATA_DIR` | Directory for per-bucket state | _(bucket root)_ |
| `SHOEBOX_CONFIG` | Path to global config file | _(none)_ |
| `SHOEBOX_LOG` | Log level: `trace`, `debug`, `info`, `warn`, `error` | `info` |
| `RUST_LOG` | Fallback log level (used if `SHOEBOX_LOG` not set) | _(none)_ |

`SHOEBOX_LOG` takes precedence over `RUST_LOG`. Both accept [env_filter syntax](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html) for fine-grained control:

```bash
# Debug logging for Shoebox, info for everything else
SHOEBOX_LOG=shoebox=debug,info shoebox ~/Photos
```

---

## Global Help

```bash
shoebox --help          # Show all commands and options
shoebox --version       # Show version
shoebox <COMMAND> --help  # Show help for a specific command
```
