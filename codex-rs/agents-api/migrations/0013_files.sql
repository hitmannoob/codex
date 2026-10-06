-- Uploaded files that environment files can name by `file_id`. Contents live
-- in DATA_DIRECTORY/files under `id`; `seq` orders listings and is never reused.
CREATE TABLE files (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    filename TEXT NOT NULL,
    purpose TEXT NOT NULL,
    bytes INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    expires_at INTEGER
);
