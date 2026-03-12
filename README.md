# Shoebox

I have 2TB of photos across 3 drives. Some are backups of backups. Some are originals I'm afraid to delete. Finding duplicates was always a weekend project that never happened.

Then I realized: if an object store knows the content hash of every file, duplicates are just a query.

I'm building a tool to do that. Once you have an S3 API for local files, everything else comes for free—rclone, AWS CLI, any SDK. I set out to find duplicate photos and accidentally designed a local S3 server.

![Shoebox webapp — browsing a bucket](docs/screenshots/bucket-sample.png)

## Webapp

A companion browser UI for Shoebox is available at **https://deepjoy.github.io/shoebox-webapp/**.

Browse buckets, view objects, and see duplicate groups visually—no CLI needed. The webapp talks directly to your local Shoebox server via the S3 API.

**CORS setup** (required for browser access):

```bash
# Start Shoebox
shoebox ~/Photos

# Enable CORS for the webapp origin
aws s3api put-bucket-cors --bucket photos --cors-configuration '{
  "CORSRules": [{
    "AllowedOrigins": ["https://deepjoy.github.io"],
    "AllowedMethods": ["GET", "PUT", "DELETE", "HEAD"],
    "AllowedHeaders": ["*"],
    "ExposeHeaders": ["ETag", "x-amz-request-id"],
    "MaxAgeSeconds": 3600
  }]
}'
```

## What This Is

```bash
shoebox ~/Photos
```

Your photos accessible via S3. Files stay where they are. No configuration. No cloud account. No data leaving your machine.

**The goal:**
- S3-compatible API backed by your local filesystem
- Zero-config startup—just point at directories
- Built-in duplicate detection via content hashing
- Integrity verification to detect bit rot
- Sync endpoint with move detection
- CORS support for browser-based clients
- Works with rclone, AWS CLI, and standard SDKs
- Single binary, ~10MB

[![asciicast](https://asciinema.org/a/pcK5rLzuAW2xlhp2.svg)](https://asciinema.org/a/pcK5rLzuAW2xlhp2)

## Duplicate Detection

Shoebox hashes every file (SHA-256) in the background. Finding duplicates is a query:

```bash
$ shoebox duplicates ~/Photos --format table

Duplicate groups (2 groups, 5 files, 3 duplicates):

  Hash (SHA-256)       Size   Files
  ─────────────────────────────────────────────
  a]3f…c8d1            32 B   3 copies
    originals/sunset.txt
    backup/sunset.txt        ← duplicate
    edited/sunset-copy.txt   ← duplicate

  7b2e…f104            26 B   2 copies
    originals/mountain.txt
    backup/mountain.txt      ← duplicate
```

Pick a winner, delete the rest:

```bash
$ shoebox duplicates ~/Photos --merge
# or via the S3 API:
# POST /photos?merge  {"winner_key": "originals/sunset.txt", "loser_keys": ["backup/sunset.txt", "edited/sunset-copy.txt"]}
```

## Current Status — v0.3.0

**Phases 1–9 complete.** 157 tests passing. Works with AWS CLI, rclone, any S3 SDK.

**What works today:**
- **Core operations** — ListBuckets, PutObject, GetObject, DeleteObject, HeadObject, ListObjectsV2, DeleteObjects
- **Authentication** — AWS Signature V4 (header and pre-signed URLs), per-bucket and global credentials, runtime credential CRUD via CLI and API
- **Virtual-hosted routing** — `bucket.localhost:9000/key` style requests alongside path-style
- **Copy & rename** — Same-bucket and cross-bucket copy, atomic rename
- **Range requests** — Partial content reads (206 responses)
- **Conditional requests** — If-Match, If-None-Match, If-Modified-Since, If-Unmodified-Since
- **Object tagging** — Get, put, delete tags with S3-compatible XML
- **Multipart uploads** — Initiate, upload parts, complete, abort, list uploads/parts
- **Filesystem scanner** — Multi-level scanning (L1 walk, L2 stat, L3 dual hashing), background workers, real-time filesystem watching, checkpoint and resume
- **Sync endpoint** — Trigger rescan via `POST /{bucket}?sync`, move detection preserves object identity across renames
- **Duplicate detection** — Per-bucket and cross-bucket duplicate files and directories, streaming merge algorithm, duplicate merge (keep winner, delete losers)
- **Integrity verification** — Sync and async integrity checks, scheduled checks (every 24h), bit rot detection, CLI subcommands
- **Directory comparison** — Compare two directories across buckets, showing identical/modified/unique files
- **CORS** — PutBucketCors, GetBucketCors, DeleteBucketCors, preflight OPTIONS handling, in-memory rule cache
- **Bucket notifications** — Webhook delivery with retry on object events (put, delete, copy, multipart complete)
- **Library API** — Rust-native `Shoebox` struct with methods that map 1:1 to S3 operations, usable without an HTTP server
- **CLI subcommands** — `duplicates`, `integrity-check`, `compare-dirs`, `presign`, `rename`, credential management
- **Graceful shutdown** — Clean SIGINT/SIGTERM handling with WAL flush

Files already on disk appear in S3 without uploading — the scanner picks them up automatically.

## The Problem

### Finding Duplicates is Surprisingly Hard

You have photos scattered across drives, backup folders, and downloads. Some are duplicates. Finding them is tedious:

- Filesystem tools compare by name, not content
- Cloud S3 has no duplicate detection
- Third-party tools require exporting data or running separate processes

When your object store knows the content hash of every file, finding duplicates is a query, not a project.

### Cloud S3 for Local Development is Wasteful

You're building an app that stores files in S3. To test it, you need an AWS account, managed credentials, network connectivity, patience for latency, and money for data transfer. For files that exist only to be deleted when you're done testing.

### Existing Solutions Solve Different Problems

MinIO, SeaweedFS, and Garage are built for distributed storage—erasure coding, multi-node replication, cluster management. They solve a real problem: storing more data than fits on one machine.

But most people don't have that problem. They have a NAS, a laptop, maybe an external drive. For single-machine storage, these tools bring complexity you don't need.

## Who It's For

- **Developers**: Test S3 integrations without cloud dependencies. Work offline.
- **Home users**: Expose NAS storage to S3-compatible backup tools. Find duplicates with a single query.
- **Archivists**: Verify file integrity with content hashes. Detect bit rot.
- **Privacy-conscious users**: Keep files local. No account required, no telemetry.

## Comparison

See [docs/why-shoebox.md](docs/why-shoebox.md) for the full story — problem, approach, and who it's for.

| Concern | Cloud S3 | MinIO | SeaweedFS | Garage | Shoebox |
|---------|----------|-------|-----------|--------|---------|
| Primary strength | Scalability, AWS ecosystem | High performance, enterprise | Small files, high throughput | Simplicity, geo-replication | Existing files, zero config |
| Best for | Production workloads | AI/ML, large data (TB/PB) | Data lakes, file storage | Edge/distributed, low ops | Local dev, NAS, home lab |
| Architecture | Managed service | Specialized nodes | Master/volume servers | Homogeneous nodes | Single process |
| Setup | Account + IAM | Docker + config | Docker + config | Docker + config | Single command |
| Data location | Cloud | MinIO data dir | SeaweedFS volumes | Garage data dir | Your existing files |
| File visibility | S3 only | S3 only | S3 only | S3 only | Filesystem + S3 |
| Offline use | No | Yes | Yes | Yes | Yes |
| Binary size | N/A | ~200MB | ~40MB | ~25MB | ~10MB |
| Duplicate detection | No | No | No | No | Built-in |
| Integrity checks | No | Yes (bitrot healing) | No | Yes (scrub) | Built-in (scheduled) |
| Max recommended scale | Unlimited | Petabytes | Petabytes | Petabytes | ~10TB |

## When Not to Use Shoebox

See [docs/when-not-to-use-shoebox.md](docs/when-not-to-use-shoebox.md) for an honest assessment of limitations, including:

- Strong consistency requirements
- Distributed / multi-node storage
- >10TB of data
- Enterprise S3 features (object lock, lifecycle policies, versioning)
- High-throughput ingestion (thousands of files/second)

## License

MIT

## Documentation

- [Quickstart](docs/quickstart.md) — Running in 5 minutes
- [Installation](docs/installation.md) — Docker, cargo install, from source
- [User Guides](docs/README.md) — Configuration, credentials, S3 compatibility, and more

## Following Along

This is a personal project built in public. v0.3.0 is the latest release — expect breaking changes before 1.0.

If you're curious about local-first S3 storage or have thoughts on the approach, I'd like to hear from you. Open an issue or start a discussion.
