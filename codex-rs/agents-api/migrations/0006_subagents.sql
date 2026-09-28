-- Subagents spawned inside a session. Each runs as its own Codex thread; its
-- turns and items are public records tagged with the subagent's ID and kept
-- apart from the session's root history. `created_seq` orders listings and is
-- never reused.
CREATE TABLE subagents (
    created_seq INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    id TEXT NOT NULL,
    thread_id TEXT NOT NULL UNIQUE,
    data TEXT NOT NULL,
    UNIQUE (session_id, id)
);

ALTER TABLE public_records ADD COLUMN subagent_id TEXT;
CREATE INDEX public_records_subagent ON public_records (session_id, subagent_id, kind, seq);

-- Codex reports a running usage total per thread, and each subagent is its own
-- thread, so stored totals move from sessions to threads. Existing totals
-- belong to each session's root thread.
CREATE TABLE thread_usage_totals (
    thread_id TEXT PRIMARY KEY,
    input_tokens INTEGER NOT NULL,
    cached_tokens INTEGER NOT NULL,
    output_tokens INTEGER NOT NULL,
    reasoning_tokens INTEGER NOT NULL,
    total_tokens INTEGER NOT NULL
);
INSERT INTO thread_usage_totals (thread_id, input_tokens, cached_tokens, output_tokens, reasoning_tokens, total_tokens)
SELECT s.thread_id, u.input_tokens, u.cached_tokens, u.output_tokens, u.reasoning_tokens, u.total_tokens
FROM usage_totals u JOIN sessions s ON s.id = u.session_id
WHERE s.thread_id IS NOT NULL;
DROP TABLE usage_totals;
