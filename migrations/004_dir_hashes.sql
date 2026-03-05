-- Directory hash: a composite hash of all files within a directory prefix.
-- Computed by hashing the sorted list of (relative_key, checksum_sha256) pairs.
-- Used for duplicate directory detection and directory comparison.
CREATE TABLE IF NOT EXISTS directory_hashes (
    id TEXT PRIMARY KEY,
    prefix TEXT NOT NULL UNIQUE,     -- directory prefix (e.g. "photos/2024/")
    dir_hash TEXT NOT NULL,          -- composite hash of all files in this prefix
    file_count INTEGER NOT NULL,     -- number of files in this directory
    total_size INTEGER NOT NULL,     -- sum of file sizes
    computed_at TEXT NOT NULL,        -- RFC 3339 timestamp
    stale BOOLEAN NOT NULL DEFAULT FALSE  -- set TRUE when any child file changes
);

CREATE INDEX IF NOT EXISTS idx_dir_hashes_hash ON directory_hashes(dir_hash);
CREATE INDEX IF NOT EXISTS idx_dir_hashes_stale ON directory_hashes(stale) WHERE stale = TRUE;
