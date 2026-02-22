# Scanner Architecture

The scanner is a background subsystem that discovers files on disk and progressively enriches their metadata in the SQLite catalog. It runs alongside the S3-compatible API server.

## Scan levels

The scanner uses a three-level progressive scan model. Each level builds on the previous one, and every object's `scan_level` column in the database records how far it has been scanned.

| Level | Name | Purpose |
|-------|------|---------|
| L1 | Discovery | Walk directory tree, insert new object records, detect deleted files |
| L2 | Metadata | stat() each file for size, mtime, ctime, inode, device_id |
| L3 | Content | Stream-read file, compute MD5 (ETag) and SHA-256 |

### L1 — Discovery (`scan_l1`)

- Walks the bucket root using `async_walkdir`, skipping the `.shoebox` metadata directory.
- Loads all known keys from the DB into memory upfront for O(1) lookups.
- For each file on disk within the `ScanScope`:
  - Skips if already in the database (unchanged).
  - Otherwise creates an `ObjectRecord` with UUID, key, parent directory, size, content type, and `scan_level = 1`.
- Batches inserts (1000 rows or 500ms flush timeout).
- For bucket-wide scans, detects **deleted files** — keys in the DB but no longer on disk — and removes them.
- Idempotent: running L1 twice with no filesystem changes produces zero new discoveries.

## Scan scope

Each job has a `ScanScope` that constrains which files it operates on:

| Scope | Description | L1 behavior |
|-------|-------------|-------------|
| `Bucket` | Entire bucket | Full directory walk + deletion detection |
| `Subtree { prefix }` | Files under a prefix | Walk filtered by prefix |
| `Files(Vec<String>)` | Specific keys | Skips L1 walk entirely |

## Database schema

The scanner uses two dedicated tables (from `migrations/004_scanner.sql`) plus columns on the existing `objects` table:

### Objects table additions

| Column | Type | Added by |
|--------|------|----------|
| `scan_level` | INTEGER | L1 (set to 1), L2 (2), L3 (3) |
| `file_mtime` | TEXT | L2 |
| `file_ctime` | TEXT | L2 (migration 004) |
| `inode` | INTEGER | L2 (migration 004) |
| `device_id` | INTEGER | L2 (migration 004) |
| `etag` | TEXT | L3 (MD5 hash) |
| `content_hash` | TEXT | L3 (SHA-256 hash) |

### scan_jobs table

Tracks scheduled and completed scan jobs with priority, scope, target level, and progress.

### bucket_scan_state table

Singleton row tracking aggregate scan progress per bucket: total files, files at each scan level, and timestamps of the last L1 and L3 scans.

## Source files

| File | Purpose |
|------|---------|
| [mod.rs](../../src/scanner/mod.rs) | Module exports |
| [levels.rs](../../src/scanner/levels.rs) | L1 scan implementation |
| [scope.rs](../../src/scanner/scope.rs) | Scan scope types (Bucket, Subtree, Files) |
| [platform.rs](../../src/scanner/platform.rs) | Cross-platform inode/device extraction |
