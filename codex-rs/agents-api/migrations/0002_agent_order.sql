-- Preserve creation order across updates, deletions, and database restarts.
ALTER TABLE agents ADD COLUMN created_seq INTEGER;
UPDATE agents SET created_seq = rowid;
CREATE UNIQUE INDEX agents_created_seq ON agents (created_seq);
CREATE TABLE agent_sequence (seq INTEGER PRIMARY KEY AUTOINCREMENT);
INSERT INTO agent_sequence (seq) SELECT created_seq FROM agents ORDER BY created_seq;
CREATE TRIGGER agents_assign_created_seq AFTER INSERT ON agents
WHEN NEW.created_seq IS NULL
BEGIN
    INSERT INTO agent_sequence (seq) VALUES (NULL);
    UPDATE agents SET created_seq = last_insert_rowid() WHERE id = NEW.id;
END;
