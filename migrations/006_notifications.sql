-- Webhook delivery log for debugging and auditing.
CREATE TABLE IF NOT EXISTS notification_delivery_log (
    id TEXT PRIMARY KEY,
    webhook_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    object_key TEXT NOT NULL,
    delivered_at TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    status TEXT NOT NULL DEFAULT 'pending'  -- pending, delivered, failed
);

CREATE INDEX IF NOT EXISTS idx_delivery_log_status ON notification_delivery_log(status, webhook_id);
