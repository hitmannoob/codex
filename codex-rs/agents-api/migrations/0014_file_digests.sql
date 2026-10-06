-- The SHA-256 of each upload's contents, checked before contents are served.
ALTER TABLE files ADD COLUMN sha256 TEXT;
