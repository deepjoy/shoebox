# Why Shoebox Does Not Implement Object Versioning

## The Shoebox Ethos

Shoebox's core promise is **zero-config, filesystem-backed object storage**. Your
files on disk *are* the source of truth. The `.shoebox/` directory contains only
derived metadata — credentials, a SQLite index, multipart upload temps — all of
which can be deleted and recreated by pointing shoebox at the same directory again.

This property is fundamental. It means:

- **No lock-in.** Stop running shoebox and your files are exactly where you left
  them. No export step, no migration, no data trapped in an opaque format.
- **Disaster recovery is trivial.** Back up your files with any tool (rsync,
  Time Machine, cp). The `.shoebox/` directory is disposable.
- **Mental model is simple.** One file on disk = one object in the API. `ls` and
  `aws s3 ls` agree.

## Why Versioning Breaks This

S3 object versioning stores previous versions of objects when they are
overwritten or deleted. Implementing this in shoebox would require:

1. **Opaque state that cannot be recreated from the filesystem.** Previous
   versions exist only in `.shoebox/versions/`. If you delete `.shoebox/` and
   rescan, all version history is gone. This violates the core property that
   `.shoebox/` is disposable derived state.

2. **Storage that grows silently and without bound.** Every overwrite copies the
   previous file into `.shoebox/versions/`. A user bulk-processing photos could
   double their disk usage with no visible indication. Retention policies
   (max versions, time-based expiry) add configuration complexity that conflicts
   with zero-config.

3. **Files that exist nowhere on the user's filesystem.** Versioned copies live
   in `.shoebox/versions/{object_id}/{version_id}` — opaque UUIDs that mean
   nothing outside of shoebox. The user cannot browse, back up, or reason about
   these files without shoebox-specific tooling.

4. **Broken symmetry between filesystem and API.** With versioning, `rm file.txt`
   on disk does not mean the object is gone — a delete marker is created and the
   file persists in version storage. The filesystem and the S3 API no longer
   agree on what exists. This is confusing for exactly the audience shoebox
   targets: people who want their files served as-is.

5. **Conflict with the scanner model.** The scanner's job is to reconcile the
   SQLite index with what's actually on disk. Versioning adds state that has no
   on-disk counterpart for the scanner to verify, creating a class of metadata
   that can drift without any way to self-heal.

## What To Use Instead

Versioning solves two problems: **accidental deletion** and **change history**.
Both are better handled by tools purpose-built for them:

- **Filesystem snapshots** (ZFS snapshots, Btrfs snapshots, macOS Time Machine,
  LVM snapshots) provide point-in-time recovery with zero application-level
  complexity. They are transparent to shoebox — the scanner sees the live
  filesystem and snapshots live outside its scope.

- **Backup tools** (rsync, restic, borgbackup) provide deduplicated, versioned
  backups of the entire directory. Since shoebox's files are plain files on disk,
  any backup tool works without special integration.

- **Git or git-annex** for content that benefits from explicit version tracking
  with commit messages and diffs.

These approaches keep versioning concerns outside of shoebox, where they belong.
Shoebox stays simple: serve what's on disk, index it efficiently, and get out of
the way.
