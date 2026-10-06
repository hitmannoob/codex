-- Self-hosted environments, one per session that uses one. `data` is the
-- public environment object; executors register against `id`. `waiting` is set
-- while the session's input waits for the executor to connect.
CREATE TABLE environments (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL UNIQUE,
    data TEXT NOT NULL,
    waiting INTEGER NOT NULL DEFAULT 0
);
