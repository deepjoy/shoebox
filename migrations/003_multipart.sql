-- Phase 5: Multipart Upload Support
-- Tracks in-progress multipart uploads and uploaded parts

CREATE TABLE multipart_uploads (
    id TEXT PRIMARY KEY,
    key TEXT NOT NULL,
    initiated_at TEXT NOT NULL,
    content_type TEXT,
    metadata TEXT
);

CREATE TABLE parts (
    id TEXT PRIMARY KEY,
    upload_id TEXT NOT NULL REFERENCES multipart_uploads(id) ON DELETE CASCADE,
    part_number INTEGER NOT NULL,
    size INTEGER NOT NULL,
    etag TEXT NOT NULL,
    uploaded_at TEXT NOT NULL,
    UNIQUE (upload_id, part_number)
);

CREATE INDEX idx_parts_upload_id ON parts(upload_id);
CREATE INDEX idx_multipart_uploads_key ON multipart_uploads(key);
CREATE INDEX idx_multipart_uploads_initiated_at ON multipart_uploads(initiated_at);
