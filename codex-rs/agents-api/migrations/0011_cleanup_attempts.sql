-- Bounded retries for deleting a deleted session's worker thread. A failure
-- backs off, and an entry that keeps failing is given up after a limit, so a
-- permanent failure cannot retry and log on every later deletion.
ALTER TABLE session_cleanup ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE session_cleanup ADD COLUMN next_attempt_at INTEGER NOT NULL DEFAULT 0;
