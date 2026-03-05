-- Object metadata
CREATE TABLE IF NOT EXISTS objects (
    id TEXT PRIMARY KEY,
    -- UNIQUE implies an implicit index; no explicit CREATE INDEX needed for key lookups.
    key TEXT NOT NULL UNIQUE,
    parent_directory TEXT NOT NULL,
    is_directory BOOLEAN NOT NULL DEFAULT FALSE,
    is_symlink BOOLEAN NOT NULL DEFAULT FALSE,
    symlink_target TEXT,

    -- L2 metadata (NULL until scanned)
    size INTEGER,
    file_mtime TEXT,
    file_ctime TEXT,
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
    content_type TEXT DEFAULT 'application/octet-stream',
    last_modified TEXT NOT NULL,
    created_at TEXT NOT NULL,
    metadata TEXT,

    -- Scan state
    scan_level INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_objects_parent ON objects(parent_directory);
CREATE INDEX IF NOT EXISTS idx_objects_checksum_sha256 ON objects(checksum_sha256);
CREATE INDEX IF NOT EXISTS idx_objects_inode ON objects(inode, device_id) WHERE inode IS NOT NULL;
