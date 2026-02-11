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

## Current Status

**This project is in early development. Nothing works yet.**

The implementation plan is complete. The code is not. If you're interested in where this is going, check back or watch the repo.

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

This is a personal project built in public. Implementation is just beginning.

If you're curious about local-first S3 storage or have thoughts on the approach, I'd like to hear from you. Open an issue or start a discussion.
