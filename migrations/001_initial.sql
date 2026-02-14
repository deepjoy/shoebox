-- Object metadata
CREATE TABLE IF NOT EXISTS objects (
    id TEXT PRIMARY KEY,
    key TEXT NOT NULL UNIQUE,
    parent_directory TEXT NOT NULL,
    is_directory BOOLEAN NOT NULL DEFAULT FALSE,
    is_symlink BOOLEAN NOT NULL DEFAULT FALSE,
    symlink_target TEXT,

    -- L2 metadata (NULL until scanned)
    size INTEGER,
    file_mtime TEXT,

    -- L3 metadata (NULL until content-hashed)
    etag TEXT,
    content_hash TEXT,

    -- S3 metadata
    content_type TEXT DEFAULT 'application/octet-stream',
    last_modified TEXT NOT NULL,
    created_at TEXT NOT NULL,
    metadata TEXT,

    -- Scan state
    scan_level INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_objects_parent ON objects(parent_directory);
CREATE INDEX IF NOT EXISTS idx_objects_content_hash ON objects(content_hash);
