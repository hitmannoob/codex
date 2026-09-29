-- Timing for trace export, in Unix milliseconds as the service observed it.
-- Turns and items record when they started and finished. Records saved before
-- this migration have none and are exported at their turn's boundaries.
ALTER TABLE public_records ADD COLUMN started_ms INTEGER;
ALTER TABLE public_records ADD COLUMN completed_ms INTEGER;

-- The usage of each model response, in the order Codex reported it. `turn_id`
-- is the root or subagent turn that made the request.
CREATE TABLE generations (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    input_tokens INTEGER NOT NULL,
    output_tokens INTEGER NOT NULL,
    total_tokens INTEGER NOT NULL
);
CREATE INDEX generations_turn ON generations (session_id, turn_id, seq);
