-- Idempotency-Key records for session input events, scoped to one session.
-- `state` is `pending` while a request executes, `completed` once its HTTP
-- outcome is stored for replay, and `unknown` when dispatch was interrupted
-- (backend loss or an API crash) and the outcome cannot be established.
CREATE TABLE input_requests (
    session_id TEXT NOT NULL,
    key TEXT NOT NULL,
    request TEXT NOT NULL,
    state TEXT NOT NULL,
    status INTEGER,
    message TEXT,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (session_id, key)
);
CREATE INDEX input_requests_created_at ON input_requests (created_at);
