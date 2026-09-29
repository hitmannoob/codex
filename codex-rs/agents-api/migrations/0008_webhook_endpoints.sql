-- Webhook endpoint configuration. Signing secrets never enter this database:
-- they live in the encrypted secrets store, keyed by endpoint ID.
-- `created_seq` orders listings and is never reused.
CREATE TABLE webhook_endpoints (
    created_seq INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    data TEXT NOT NULL
);
