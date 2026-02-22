-- Phase 6: Scanner tables and L2 metadata columns

-- L2 metadata columns referenced by scan_l2
ALTER TABLE objects ADD COLUMN file_ctime TEXT;
ALTER TABLE objects ADD COLUMN inode INTEGER;
ALTER TABLE objects ADD COLUMN device_id INTEGER;

-- Scanner job tracking
CREATE TABLE IF NOT EXISTS scan_jobs (
    id TEXT PRIMARY KEY,
    priority INTEGER NOT NULL,            -- 0=realtime, 1=reconcile, 2=background
    scope_type TEXT NOT NULL,
    scope_data TEXT,                       -- JSON
    target_level INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    files_total INTEGER DEFAULT 0,
    files_completed INTEGER DEFAULT 0,
    last_processed_key TEXT
);

-- Singleton row tracking per-bucket scan progress
CREATE TABLE IF NOT EXISTS bucket_scan_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    total_files INTEGER NOT NULL DEFAULT 0,
    files_at_level_1 INTEGER NOT NULL DEFAULT 0,
    files_at_level_2 INTEGER NOT NULL DEFAULT 0,
    files_at_level_3 INTEGER NOT NULL DEFAULT 0,
    last_l1_scan_at TEXT,
    last_l3_scan_at TEXT,
    updated_at TEXT NOT NULL
);
