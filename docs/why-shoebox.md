# Why Shoebox?

I have 2TB of photos across 3 drives. Some are backups of backups. Some are originals I'm afraid to delete. Finding duplicates was always a weekend project that never happened.

Then I realized: if an object store knows the content hash of every file, duplicates are just a query.

Built a tool to do that. Turns out, once you have an S3 API for local files, everything else comes for free—rclone, AWS CLI, any SDK. I set out to find duplicate photos and accidentally built a local S3 server.

```bash
shoebox ~/Photos
```

Your photos are now accessible via S3. Files stay where they are.

---

## The Problem

### Finding Duplicates is Surprisingly Hard

You have photos scattered across drives, backup folders, and downloads. Some are duplicates. Finding them is tedious:

- Filesystem tools compare by name, not content
- Cloud S3 has no duplicate detection
- Third-party tools require exporting data or running separate processes

When your object store knows the content hash of every file, finding duplicates is a query, not a project.

### Cloud S3 for Local Development is Wasteful

You're building an app that stores files in S3. To test it, you need:

- An AWS account
- Credentials managed securely
- Network connectivity
- Patience for latency
- Money for data transfer

For every PUT and GET during development, you're making round trips to the cloud. For files that exist only to be deleted when you're done testing.

### Existing Solutions Solve Different Problems

MinIO, SeaweedFS, and Garage are built for distributed storage—erasure coding, multi-node replication, cluster management. They solve a real problem: storing more data than fits on one machine.

But most people don't have that problem. They have a NAS, a laptop, maybe an external drive. For single-machine storage, these tools bring complexity you don't need.

LocalStack mocks AWS services. It doesn't provide real storage.

---

## The Shoebox Way

### Zero Configuration

```bash
shoebox ~/Photos
```

Your photos are now accessible via S3 API. Credentials are auto-generated and printed. No YAML. No environment variables. No documentation required.

### Files Stay Where They Are

Shoebox doesn't copy your files. Doesn't maintain a separate data directory. Doesn't require migration. Your files stay exactly where they are, organized exactly how you want them. Shoebox provides an S3 interface to them.

### Self-Contained Buckets

Each directory you serve is independent. It has its own `.shoebox/` folder with credentials and metadata. Move the directory? The credentials move with it. Back it up? Everything needed is included.

### Hybrid Access

Access your files via the filesystem *and* S3 API, interchangeably. Copy files in with `cp`. Download them with `aws s3 cp`. Edit them in your file manager. List them with `rclone`. Shoebox keeps the metadata synchronized.

---

## Who It's For

### Developers

- Test S3 integrations without cloud dependencies
- Run integration tests in CI without managing credentials
- Debug upload/download flows with files visible on disk
- Work offline

### Home Users

- Expose NAS storage to S3-compatible backup tools
- Use rclone's powerful sync against local directories
- Find duplicate files across scattered folders with a single query
- Share files temporarily with pre-signed URLs

### Application Authors

- Embed S3-compatible storage in desktop applications
- Add object storage to Rust web services
- Provide file upload endpoints without cloud dependencies

### Archivists & Digital Preservationists

- Verify file integrity with content hashes
- Detect bit rot by comparing stored hashes against recalculated values
- Find duplicates across archival collections
- Use S3-compatible tools for archival workflows

### Media Producers

- Manage video and audio assets alongside project files
- Access footage via S3 API for cloud-connected editing tools
- Find duplicate assets across projects

### Privacy-Conscious Users

- Keep files on local storage, not in the cloud
- No account required, no telemetry, no data leaving your machine
- Self-host everything with a single binary

---

## The Comparison

| Concern | Cloud S3 | MinIO | SeaweedFS | Garage | Shoebox |
|---------|----------|-------|-----------|--------|---------|
| Primary strength | Scalability, AWS ecosystem | High performance, enterprise | Small files, high throughput | Simplicity, geo-replication | Existing files, zero config |
| Best for | Production workloads | AI/ML, large data (TB/PB) | Data lakes, file storage | Edge/distributed, low ops | Local dev, NAS, home lab |
| Architecture | Managed service | Specialized nodes | Master/volume servers | Homogeneous nodes | Single process |
| Data protection | AWS managed | Erasure coding | Replication + EC | Replication (CRDTs) | Your filesystem (RAID) |
| Complexity | Low (managed) | Moderate to high | High | Low | Minimal |
| Setup | Account + IAM | Docker + config | Docker + config | Docker + config | Single command |
| Data location | Cloud | MinIO data dir | SeaweedFS volumes | Garage data dir | Your existing files |
| File visibility | S3 only | S3 only | S3 only | S3 only | Filesystem + S3 |
| Offline use | No | Yes | Yes | Yes | Yes |
| Binary size | N/A | ~200MB | ~40MB | ~25MB | ~10MB |
| Docker image | N/A | ~300MB | ~50MB | ~30MB | ~15MB |
| Credentials | IAM | Configured | Configured | Configured | Auto-generated |
| Per-directory isolation | No | Manual | No | No | Automatic |
| Duplicate detection | No | No | No | No | Built-in |
| Integrity checks | No | Yes (bitrot healing) | No | Yes (scrub) | Built-in (scheduled) |
| Max recommended scale | Unlimited | Petabytes | Petabytes | Petabytes | ~10TB |

---

## When to Use Shoebox

**Use Shoebox when:**
- You want S3 API access to local files
- You need a drop-in S3 replacement for development
- You're using S3 ecosystem tools against local storage
- You're embedding storage in an application
- You want fine-grained credential control without cloud IAM

**Use something else when:**
- You need distributed storage across multiple machines
- You have more than 10TB of data
- You need strong consistency (file exists on disk = immediately visible via API)
- You need enterprise S3 features (object lock, lifecycle policies, SNS/SQS notifications)
- You need the full AWS ecosystem (Lambda triggers, etc.)

See [When Not to Use Shoebox](when-not-to-use-shoebox.md) for details.

---

## In Practice

**Native:**
```bash
shoebox ~/Photos
```

**Docker:**
```bash
docker run -v ~/Photos:/data -p 9000:9000 ghcr.io/deepjoy/shoebox /data
```

Pick whichever fits your workflow.

**Development:** Point your app at `localhost:9000` instead of `s3.amazonaws.com`. When you're ready for production, change the endpoint back.

**Personal backup:** Run `shoebox ~/Documents ~/Pictures` on your NAS. Configure rclone once. Sync to Backblaze B2 with `rclone sync local:Documents remote:backup`.

**CI/CD:**
```yaml
services:
  shoebox:
    image: ghcr.io/deepjoy/shoebox
    volumes:
      - ./test-fixtures:/data
steps:
  - run: npm test  # S3_ENDPOINT=http://shoebox:9000
```

No secrets to manage. No cleanup required.

**Embedded:** Add `shoebox` as a dependency. Get an S3-compatible storage layer in a few lines of code.

---

## The Bottom Line

I wanted to find duplicate photos. I ended up building an S3 server.

When every file has a content hash in SQLite, duplicates are just a GROUP BY. But the real discovery: every S3 tool just works. rclone, AWS CLI, any SDK.

~10MB binary. Files stay where they are. One command.

```bash
shoebox ~/Photos
```
