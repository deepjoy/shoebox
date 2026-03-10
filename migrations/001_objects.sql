-- Interned MIME types for content_type (small lookup table, ~60 rows).
CREATE TABLE IF NOT EXISTS content_types (
    id INTEGER PRIMARY KEY,
    mime TEXT NOT NULL UNIQUE
);

-- Object metadata
CREATE TABLE IF NOT EXISTS objects (
    id TEXT PRIMARY KEY,
    -- UNIQUE implies an implicit index; no explicit CREATE INDEX needed for key lookups.
    key TEXT NOT NULL UNIQUE,
    parent_dir_id INTEGER NOT NULL REFERENCES directories(id),
    is_symlink BOOLEAN NOT NULL DEFAULT FALSE,
    symlink_target TEXT,

    -- L2 metadata (NULL until scanned)
    size INTEGER,
    file_mtime INTEGER,             -- Unix epoch nanoseconds
    file_ctime INTEGER,             -- Unix epoch nanoseconds
    inode INTEGER,
    device_id INTEGER,

    -- L3 metadata (NULL until content-hashed)
    etag TEXT,                   -- stored WITH surrounding quotes, e.g. '"abc123"'

    -- S3 checksums (base64-encoded, NULL until content-hashed)
    checksum_sha256 TEXT,
    checksum_sha1 TEXT,
    checksum_crc32 TEXT,
    checksum_crc32c TEXT,

    -- S3 metadata
    content_type_id INTEGER REFERENCES content_types(id),
    last_modified INTEGER NOT NULL,  -- Unix epoch nanoseconds
    created_at INTEGER NOT NULL,     -- Unix epoch nanoseconds
    metadata TEXT,

    -- Scan state
    scan_level INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_objects_parent ON objects(parent_dir_id, key);
CREATE INDEX IF NOT EXISTS idx_objects_checksum_sha256 ON objects(checksum_sha256);
CREATE INDEX IF NOT EXISTS idx_objects_inode ON objects(inode, device_id) WHERE inode IS NOT NULL;
