-- Runtime-mutable bucket config (CORS rules, webhook configs).
-- Keyed by config type; values are JSON-serialized.
CREATE TABLE IF NOT EXISTS bucket_config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
