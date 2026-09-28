use crate::resources::Agent;
use crate::resources::AgentConfig;
use crate::resources::Session;
use crate::resources::SessionCreateParams;
use anyhow::Context;
use codex_state::SqliteConfig;
use codex_utils_absolute_path::AbsolutePathBuf;
use sqlx::SqlitePool;
use sqlx::migrate::Migrator;
use uuid::Uuid;

/// Ordered, transactional schema migrations for this crate's own database.
/// Migration `0001` adopts the pre-migration schema in place; later migrations
/// evolve it. Each runs in its own transaction, so a partial upgrade resumes
/// from the last committed migration.
static MIGRATOR: Migrator = sqlx_macros::migrate!("./migrations");

/// The store owns one API data directory. The second tuple field is an advisory
/// lock held for the store's lifetime so a second API process cannot open the
/// same directory and become a rival writer of the shared SQLite database —
/// including in external-worker mode, where no worker-home lock applies. It is
/// never read; the fd is retained only to hold the lock until the store drops.
pub(crate) struct Store(pub SqlitePool, #[allow(dead_code)] std::fs::File);

impl Store {
    pub async fn open(directory: AbsolutePathBuf) -> anyhow::Result<Self> {
        tokio::fs::create_dir_all(&directory).await?;
        // Claim exclusive ownership of the data directory before touching the
        // database, rejecting a second concurrent owner in any worker mode.
        let lock = std::fs::File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(directory.join("agents-api.lock").as_path())?;
        lock.try_lock()
            .context("data directory is already in use by another agents-api process")?;
        let path = directory.join("agents-api.sqlite");
        let pool = SqliteConfig::from_sqlite_home(directory)
            .open_read_write_pool(path.as_path())
            .await?;
        MIGRATOR.run(&pool).await?;
        // Startup recovery, not schema: a previous process may have exited with
        // in-flight work. JSON-RPC request waiters belong to the lost
        // connection, not the database, and interrupted public turns must not
        // appear healthy.
        sqlx::query("UPDATE tool_calls SET status = 'unavailable' WHERE status IN ('pending', 'submitting')")
            .execute(&pool).await?;
        // A keyed input request that was executing may or may not have reached
        // the worker, so its key reports an unknown outcome instead of re-running.
        sqlx::query("UPDATE input_requests SET state = 'unknown' WHERE state = 'pending'")
            .execute(&pool)
            .await?;
        crate::records::disconnected(&pool).await?;
        Ok(Self(pool, lock))
    }

    pub async fn create_agent(
        &self,
        config: AgentConfig,
        name: Option<String>,
        metadata: std::collections::BTreeMap<String, String>,
    ) -> anyhow::Result<Agent> {
        let now = crate::contract::now();
        let agent = Agent {
            id: Uuid::new_v4().to_string(),
            config,
            created_at: now,
            updated_at: now,
            name,
            metadata,
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

    pub async fn list_agents(
        &self,
        after: Option<&str>,
        order: &str,
        limit: i64,
    ) -> anyhow::Result<Option<(Vec<Agent>, bool)>> {
        let cursor: Option<i64> = if let Some(id) = after {
            match sqlx::query_scalar("SELECT created_seq FROM agents WHERE id = ?")
                .bind(id)
                .fetch_optional(&self.0)
                .await?
            {
                Some(seq) => Some(seq),
                None => return Ok(None),
            }
        } else {
            None
        };
        let rows: Vec<(String,)> = if order == "asc" {
            sqlx::query_as(
                "SELECT data FROM agents WHERE created_seq > ? ORDER BY created_seq ASC LIMIT ?",
            )
            .bind(cursor.unwrap_or(0))
            .bind(limit + 1)
            .fetch_all(&self.0)
            .await?
        } else {
            sqlx::query_as(
                "SELECT data FROM agents WHERE created_seq < ? ORDER BY created_seq DESC LIMIT ?",
            )
            .bind(cursor.unwrap_or(i64::MAX))
            .bind(limit + 1)
            .fetch_all(&self.0)
            .await?
        };
        let has_more = rows.len() as i64 > limit;
        let agents = rows
            .into_iter()
            .take(limit as usize)
            .map(|(data,)| serde_json::from_str(&data))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some((agents, has_more)))
    }

    pub async fn update_agent(&self, previous: &Agent, updated: &Agent) -> anyhow::Result<bool> {
        let Some(data): Option<String> = sqlx::query_scalar("SELECT data FROM agents WHERE id = ?")
            .bind(&previous.id)
            .fetch_optional(&self.0)
            .await?
        else {
            return Ok(false);
        };
        if serde_json::from_str::<Agent>(&data)? != *previous {
            return Ok(false);
        }
        Ok(
            sqlx::query("UPDATE agents SET data = ? WHERE id = ? AND data = ?")
                .bind(serde_json::to_string(updated)?)
                .bind(&previous.id)
                .bind(data)
                .execute(&self.0)
                .await?
                .rows_affected()
                == 1,
        )
    }

    pub async fn delete_agent(&self, id: &str) -> anyhow::Result<bool> {
        Ok(sqlx::query("DELETE FROM agents WHERE id = ?")
            .bind(id)
            .execute(&self.0)
            .await?
            .rows_affected()
            == 1)
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
