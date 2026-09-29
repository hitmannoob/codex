-- The webhook outbox: one row per event per subscribed endpoint, written in
-- the transaction that makes the change the event reports. `id` is the
-- `webhook-id` header, kept across retries so receivers can deduplicate.
-- `seq` orders each endpoint's deliveries and is never reused.
CREATE TABLE webhook_deliveries (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    endpoint_id TEXT NOT NULL,
    body TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    next_attempt_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    last_status_code INTEGER,
    last_error TEXT
);
CREATE INDEX webhook_deliveries_due ON webhook_deliveries (status, next_attempt_at);
