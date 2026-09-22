use crate::resources::Agent;
use crate::resources::AgentConfig;
use crate::resources::Session;
use crate::resources::SessionCreateParams;
use codex_state::SqliteConfig;
use codex_utils_absolute_path::AbsolutePathBuf;
use sqlx::SqlitePool;
use uuid::Uuid;

pub(crate) struct Store(pub SqlitePool);

impl Store {
    pub async fn open(directory: AbsolutePathBuf) -> anyhow::Result<Self> {
        tokio::fs::create_dir_all(&directory).await?;
        let path = directory.join("agents-api.sqlite");
        let pool = SqliteConfig::from_sqlite_home(directory)
            .open_read_write_pool(path.as_path())
            .await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS agents (id TEXT PRIMARY KEY, data TEXT NOT NULL)")
            .execute(&pool)
            .await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS sessions (id TEXT PRIMARY KEY, data TEXT NOT NULL, thread_id TEXT UNIQUE)")
            .execute(&pool).await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS tool_calls (session_id TEXT NOT NULL, turn_id TEXT NOT NULL, call_id TEXT NOT NULL, request_id TEXT NOT NULL, action TEXT NOT NULL, status TEXT NOT NULL, result TEXT, PRIMARY KEY(session_id, turn_id, call_id))")
            .execute(&pool).await?;
        // JSON-RPC request waiters belong to the old connection, not the database.
        sqlx::query("UPDATE tool_calls SET status = 'unavailable' WHERE status IN ('pending', 'submitting')")
            .execute(&pool).await?;
        crate::records::initialize(&pool).await?;
        Ok(Self(pool))
    }

    pub async fn create_agent(&self, config: AgentConfig) -> anyhow::Result<Agent> {
        let agent = Agent {
            id: Uuid::new_v4().to_string(),
            config,
            created_at: crate::contract::now(),
        };
        sqlx::query("INSERT INTO agents (id, data) VALUES (?, ?)")
            .bind(&agent.id)
            .bind(serde_json::to_string(&agent)?)
            .execute(&self.0)
            .await?;
        Ok(agent)
    }

    pub async fn agent(&self, id: &str) -> anyhow::Result<Option<Agent>> {
        let data: Option<String> = sqlx::query_scalar("SELECT data FROM agents WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.0)
            .await?;
        data.map(|data| serde_json::from_str(&data))
            .transpose()
            .map_err(Into::into)
    }

    pub async fn create_session(
        &self,
        agent: Agent,
        params: SessionCreateParams,
    ) -> anyhow::Result<Session> {
        let session = Session {
            id: Uuid::new_v4().to_string(),
            agent,
            environment: params.environment,
            thread_id: None,
            required_actions: Vec::new(),
            unresolved_actions: Vec::new(),
        };
        sqlx::query("INSERT INTO sessions (id, data) VALUES (?, ?)")
            .bind(&session.id)
            .bind(serde_json::to_string(&session)?)
            .execute(&self.0)
            .await?;
        Ok(session)
    }

    pub async fn session(&self, id: &str) -> anyhow::Result<Option<Session>> {
        let row: Option<(String, Option<String>)> =
            sqlx::query_as("SELECT data, thread_id FROM sessions WHERE id = ?")
                .bind(id)
                .fetch_optional(&self.0)
                .await?;
        let Some((data, thread_id)) = row else {
            return Ok(None);
        };
        let mut session: Session = serde_json::from_str(&data)?;
        session.thread_id = thread_id;
        let actions: Vec<(String, String)> = sqlx::query_as("SELECT action, status FROM tool_calls WHERE session_id = ? AND status IN ('pending', 'unavailable', 'submitting') ORDER BY turn_id, call_id")
            .bind(id).fetch_all(&self.0).await?;
        for (action, status) in actions {
            if status == "pending" {
                session
                    .required_actions
                    .push(serde_json::from_str(&action)?);
            } else {
                session
                    .unresolved_actions
                    .push(serde_json::from_str(&action)?);
            }
        }
        Ok(Some(session))
    }
}

#[cfg(test)]
#[path = "store_tests.rs"]
mod tests;
