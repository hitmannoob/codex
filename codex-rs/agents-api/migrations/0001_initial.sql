-- Baseline schema for the agents-api store.
--
-- `IF NOT EXISTS` lets this migration adopt a pre-migration database that was
-- created by the earlier unversioned `CREATE TABLE` startup path: the tables
-- already exist there, so applying the baseline is a no-op that only records
-- the migration in `_sqlx_migrations`. On a fresh database it creates the
-- schema. Never edit this file after release; add a new migration instead.

CREATE TABLE IF NOT EXISTS agents (
    id TEXT PRIMARY KEY,
    data TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    data TEXT NOT NULL,
    thread_id TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS tool_calls (
    session_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    call_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    action TEXT NOT NULL,
    status TEXT NOT NULL,
    result TEXT,
    PRIMARY KEY (session_id, turn_id, call_id)
);

CREATE TABLE IF NOT EXISTS public_sessions (
    id TEXT PRIMARY KEY,
    data TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS public_records (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    data TEXT NOT NULL,
    UNIQUE (session_id, kind, id)
);

CREATE INDEX IF NOT EXISTS public_records_page ON public_records (session_id, kind, seq);
