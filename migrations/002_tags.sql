-- Object tagging
CREATE TABLE IF NOT EXISTS object_tags (
    id TEXT PRIMARY KEY,
    object_id TEXT NOT NULL REFERENCES objects(id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    UNIQUE (object_id, key)
);

CREATE INDEX IF NOT EXISTS idx_object_tags_object ON object_tags(object_id);
CREATE INDEX IF NOT EXISTS idx_object_tags_key_value ON object_tags(key, value);
