# Scanner Architecture

The scanner is a background subsystem that discovers files on disk and progressively enriches their metadata in the SQLite catalog. It runs alongside the S3-compatible API server.

## Scan levels

The scanner uses a three-level progressive scan model. Each level builds on the previous one, and every object's `scan_level` column in the database records how far it has been scanned.

| Level | Name | Purpose |
|-------|------|---------|
| L1 | Discovery | Walk directory tree, insert new object records, detect deleted files |
| L2 | Metadata | stat() each file for size, mtime, ctime, inode, device_id |
| L3 | Content | Stream-read file, compute MD5 (ETag) and SHA-256 |

## Source files

| File | Purpose |
|------|---------|
| [mod.rs](../../src/scanner/mod.rs) | Module exports |
