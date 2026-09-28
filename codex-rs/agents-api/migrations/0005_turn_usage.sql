-- Token usage attributed to each turn, plus each session's cumulative Codex
-- usage as of the last update applied. Codex reports a running total, so an
-- update adds only its increase over the stored total: repeated or replayed
-- totals add nothing.
CREATE TABLE turn_usage (
    session_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    input_tokens INTEGER NOT NULL,
    cached_tokens INTEGER NOT NULL,
    output_tokens INTEGER NOT NULL,
    reasoning_tokens INTEGER NOT NULL,
    total_tokens INTEGER NOT NULL,
    PRIMARY KEY (session_id, turn_id)
);

CREATE TABLE usage_totals (
    session_id TEXT PRIMARY KEY,
    input_tokens INTEGER NOT NULL,
    cached_tokens INTEGER NOT NULL,
    output_tokens INTEGER NOT NULL,
    reasoning_tokens INTEGER NOT NULL,
    total_tokens INTEGER NOT NULL
);
