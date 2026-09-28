-- Preserve public session creation order across deletions and restarts, and
-- keep a durable queue of deleted sessions whose owned Codex threads still
-- need to be removed from the worker.
ALTER TABLE public_sessions ADD COLUMN created_seq INTEGER;
UPDATE public_sessions SET created_seq = rowid;
CREATE UNIQUE INDEX public_sessions_created_seq ON public_sessions (created_seq);
CREATE TABLE public_session_sequence (seq INTEGER PRIMARY KEY AUTOINCREMENT);
INSERT INTO public_session_sequence (seq) SELECT created_seq FROM public_sessions ORDER BY created_seq;
CREATE TRIGGER public_sessions_assign_created_seq AFTER INSERT ON public_sessions
WHEN NEW.created_seq IS NULL
BEGIN
    INSERT INTO public_session_sequence (seq) VALUES (NULL);
    UPDATE public_sessions SET created_seq = last_insert_rowid() WHERE id = NEW.id;
END;

CREATE TABLE session_cleanup (
    session_id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL
);
