-- Directories: tracks every parent_directory as a normalised row.
-- Also stores a composite hash of direct children for duplicate directory detection.
-- Integer PK (rowid alias) is used as the FK from objects.parent_dir_id.
CREATE TABLE IF NOT EXISTS directories (
    id INTEGER PRIMARY KEY,
    prefix TEXT NOT NULL UNIQUE,     -- directory prefix (e.g. "photos/2024/")
    dir_hash TEXT,                   -- composite hash (NULL until computed)
    file_count INTEGER,              -- number of direct children (NULL until computed)
    total_size INTEGER,              -- sum of child file sizes (NULL until computed)
    computed_at INTEGER,             -- Unix epoch nanoseconds (NULL until computed)
    stale BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_dirs_hash ON directories(dir_hash) WHERE dir_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_dirs_stale ON directories(stale) WHERE stale = TRUE;

-- Triggers to automatically mark directory hashes stale when objects change.
-- Uses parent_dir_id FK to match the directories row directly.

CREATE TRIGGER IF NOT EXISTS trg_objects_insert_dir_stale
AFTER INSERT ON objects
BEGIN
    UPDATE directories SET stale = TRUE
    WHERE id = NEW.parent_dir_id AND stale = FALSE;
END;

CREATE TRIGGER IF NOT EXISTS trg_objects_delete_dir_stale
AFTER DELETE ON objects
BEGIN
    UPDATE directories SET stale = TRUE
    WHERE id = OLD.parent_dir_id AND stale = FALSE;
END;

CREATE TRIGGER IF NOT EXISTS trg_objects_update_dir_stale
AFTER UPDATE ON objects
WHEN OLD.parent_dir_id != NEW.parent_dir_id
   OR OLD.checksum_sha256 IS NOT NEW.checksum_sha256
   OR OLD.key != NEW.key
BEGIN
    UPDATE directories SET stale = TRUE
    WHERE id IN (OLD.parent_dir_id, NEW.parent_dir_id)
      AND stale = FALSE;
END;
