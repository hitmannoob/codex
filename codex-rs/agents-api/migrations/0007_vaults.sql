-- Vault and credential metadata. Secret values never enter this database:
-- they live in the encrypted secrets store, keyed by credential ID.
-- `created_seq` orders listings and is never reused.
CREATE TABLE vaults (
    created_seq INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    data TEXT NOT NULL
);

CREATE TABLE vault_credentials (
    created_seq INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    vault_id TEXT NOT NULL,
    data TEXT NOT NULL
);
CREATE INDEX vault_credentials_vault ON vault_credentials (vault_id, created_seq);

-- MCP credentials each session snapshotted from its vaults at creation. The
-- secret copies live in the secrets store under session-scoped names.
CREATE TABLE session_credentials (
    session_id TEXT NOT NULL,
    server_label TEXT NOT NULL,
    credential_id TEXT NOT NULL,
    PRIMARY KEY (session_id, server_label)
);
