# When Not to Use Shoebox

Shoebox solves a specific problem well. Here's where it doesn't.

## The Dual-Write Reality

Shoebox serves files directly from your filesystem while maintaining metadata in SQLite. Both paths are writable—you can `PUT` via S3 API or drag files into Finder. This flexibility has tradeoffs.

When you drop a file into the directory, Shoebox discovers it through filesystem events, then schedules background scans to compute size, timestamps, and content hashes. That takes time. If your workflow needs the S3 API to immediately reflect filesystem changes, you'll hit a gap.

## Use Something Else When...

### You Need Strong Consistency

A file lands on disk. Your application queries the S3 API expecting to find it. It's not there yet—the scanner hasn't run.

For workflows where "file exists on disk" must instantly equal "file exists in API," Shoebox's eventual consistency model won't work. Quick scans catch new files within seconds. Content hash computation takes longer. Plan accordingly.

### Both Paths Write Simultaneously

Filesystem writes and API writes to the same file at the same time? You're racing. Shoebox doesn't lock files across the two paths. The last write wins, and metadata may reflect neither correctly until the next scan.

If you have scripts modifying files while users upload via S3, pick one path and stick to it.

### Your Files Live on a Network Share

inotify doesn't work on NFS, CIFS, or SMB mounts. Shoebox falls back to periodic scans, which means minutes between a file appearing and Shoebox knowing about it.

For network-mounted storage, you need something designed for that environment.

### You Need Distributed Storage

Shoebox is single-node. One machine, one filesystem, one SQLite database.

For multi-node clusters, replication, or geographic distribution: MinIO, SeaweedFS, or actual S3.

### You Need Runtime Bucket Creation

Buckets are directories you specify at startup:

```bash
shoebox /path/to/photos /path/to/documents
```

There's no `CreateBucket` API. No `DeleteBucket`. If your application provisions storage dynamically, Shoebox won't cooperate.

### ETags Must Be Immediate

Files uploaded via `PutObject` get ETags computed inline—no delay. Files added via filesystem need an async scan to hash the contents. That scan runs at background priority.

CDN invalidation workflows, cache-busting logic, or anything else that checks ETags immediately after filesystem writes will see stale or missing values.

### You're Ingesting Thousands of Files Per Second

The filesystem watcher debounces events (100ms by default). Under heavy load, the event queue can overflow, triggering a full rescan.

For high-throughput ingestion pipelines, use the S3 API directly or a tool built for that scale.

### You Have More Than 10TB

Shoebox uses SQLite for metadata. It handles millions of files well, but beyond roughly 10TB of storage, you'll start hitting practical limits—scan times grow, queries slow down, and the single-node architecture becomes a bottleneck.

For larger storage needs, distributed solutions like MinIO or SeaweedFS are better suited.

### You Need Enterprise S3 Features

Shoebox implements the core S3 API—enough for uploads, downloads, multipart transfers, and pre-signed URLs. It doesn't implement:

- **Object versioning**: See [Why Shoebox Does Not Implement Object Versioning](architecture/no-versioning.md)
- **Object Lock / WORM**: Compliance requirements for immutable storage
- **Lifecycle policies**: Automatic expiration, transition to cold storage
- **Server-side encryption**: Encryption at rest (use filesystem encryption instead)
- **SNS/SQS event notifications**: AWS-style triggers (Shoebox supports [webhook notifications](../README.md#current-status--v030) instead)
- **Bucket policies / ACLs**: Complex permission models (use credential-based access instead)
- **Replication rules**: Cross-region or cross-bucket replication

If your workflow depends on these, you need actual S3 or a more complete implementation.

### You're on Windows (For Now)

Shoebox uses inotify for filesystem watching on Linux and FSEvents on macOS. Windows support via ReadDirectoryChangesW is planned but not yet implemented.

On Windows, Shoebox falls back to periodic polling, which means slower detection of filesystem changes.

### You Depend on Link Semantics for Write Coordination

Shoebox detects both symlinks and hardlinks and tracks their relationships, but the S3 API doesn't expose link semantics. When you write to a hardlink via `PutObject`, all linked paths see the update—but the S3 API doesn't notify you which other keys changed.

If your workflow coordinates writes through link structures (e.g., "update this hardlink and expect watchers on the other path to notice"), you'll need filesystem-level tooling instead.

**Note:** Symlinks and hardlinks are detected and represented explicitly. See [Symlink Handling](plans/shoebox.md#symlink-handling) and [Hardlink Handling](plans/shoebox.md#hardlink-handling) in the implementation plan.

## The Right Fit

Shoebox works when:

- You have files on local storage that you also want accessible via S3 API
- Eventual consistency within seconds is acceptable
- You're the primary user, maybe sharing with a small team
- Files arrive at human pace, not machine pace

The NAS in your closet. The photo library on an external drive. The documents folder you want to sync with rclone.

For everything else, there are better tools. See [Why Shoebox?](why-shoebox.md) for the comparison table.
