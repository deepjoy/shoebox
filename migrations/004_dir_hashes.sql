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

-- Triggers to automatically mark directory hashes stale when objects change.
-- Uses prefix = parent_directory || '/' to match the directory_hashes.prefix format.

CREATE TRIGGER IF NOT EXISTS trg_objects_insert_dir_stale
AFTER INSERT ON objects
WHEN NEW.parent_directory != ''
BEGIN
    UPDATE directory_hashes SET stale = TRUE
    WHERE prefix = NEW.parent_directory || '/' AND stale = FALSE;
END;

CREATE TRIGGER IF NOT EXISTS trg_objects_delete_dir_stale
AFTER DELETE ON objects
WHEN OLD.parent_directory != ''
BEGIN
    UPDATE directory_hashes SET stale = TRUE
    WHERE prefix = OLD.parent_directory || '/' AND stale = FALSE;
END;

CREATE TRIGGER IF NOT EXISTS trg_objects_update_dir_stale
AFTER UPDATE ON objects
WHEN OLD.parent_directory != NEW.parent_directory
   OR OLD.checksum_sha256 IS NOT NEW.checksum_sha256
   OR OLD.key != NEW.key
BEGIN
    UPDATE directory_hashes SET stale = TRUE
    WHERE prefix IN (OLD.parent_directory || '/', NEW.parent_directory || '/')
      AND stale = FALSE;
END;
