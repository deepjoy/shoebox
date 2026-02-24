# Shoebox

I have 2TB of photos across 3 drives. Some are backups of backups. Some are originals I'm afraid to delete. Finding duplicates was always a weekend project that never happened.

Then I realized: if an object store knows the content hash of every file, duplicates are just a query.

I'm building a tool to do that. Once you have an S3 API for local files, everything else comes for free—rclone, AWS CLI, any SDK. I set out to find duplicate photos and accidentally designed a local S3 server.

## What This Will Be

```bash
shoebox ~/Photos
```

Your photos accessible via S3. Files stay where they are. No configuration. No cloud account. No data leaving your machine.

**The goal:**
- S3-compatible API backed by your local filesystem
- Zero-config startup—just point at directories
- Built-in duplicate detection via content hashing
- Works with rclone, AWS CLI, and standard SDKs
- Single binary, ~10MB

## Current Status — v0.1.0

**Phases 1–6 complete.** 127 tests passing. Works with AWS CLI, rclone, any S3 SDK.

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
- **Library API** — Rust-native `Shoebox` struct with methods that map 1:1 to S3 operations, usable without an HTTP server
- **Graceful shutdown** — Clean SIGINT/SIGTERM handling with WAL flush

Files already on disk appear in S3 without uploading — the scanner picks them up automatically.

Versioning, sync, and duplicate detection are next.

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

## When Not to Use Shoebox

Be honest about limitations:

- You need distributed storage across multiple machines
- You have more than 10TB of data
- You need strong consistency (file on disk must instantly appear in API)
- You need enterprise S3 features (object lock, lifecycle policies, event notifications)
- You're ingesting thousands of files per second

For these cases, look at MinIO, SeaweedFS, Garage, or actual S3.

## License

MIT

## Following Along

This is a personal project built in public. v0.1.0 is the first tagged release — early preview, expect breaking changes before 1.0.

If you're curious about local-first S3 storage or have thoughts on the approach, I'd like to hear from you. Open an issue or start a discussion.
