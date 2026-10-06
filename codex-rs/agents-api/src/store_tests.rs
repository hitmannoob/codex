use super::*;
use crate::resources::Environment;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn agent_pages_keep_creation_order_and_sessions_keep_snapshots() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let store = Store::open(AbsolutePathBuf::from_absolute_path(directory.path())?).await?;
    let config = AgentConfig {
        model: "model-a".into(),
        ..Default::default()
    };
    let first = store
        .create_agent(config.clone(), None, Default::default())
        .await?;
    let second = store
        .create_agent(config.clone(), None, Default::default())
        .await?;
    let session = store
        .create_session(
            first.clone(),
            SessionCreateParams {
                agent_id: first.id.clone(),
                environment: Environment::None,
            },
        )
        .await?;
    let mut changed = first.clone();
    changed.config.instructions = Some("changed".into());
    assert!(store.update_agent(&first, &changed).await?);
    let (page, more) = store.list_agents(None, "asc", 1).await?.unwrap();
    assert_eq!(page, vec![changed.clone()]);
    assert!(more);
    let (page, more) = store.list_agents(Some(&first.id), "asc", 1).await?.unwrap();
    assert_eq!(page, vec![second.clone()]);
    assert!(!more);
    assert!(store.delete_agent(&second.id).await?);
    let third = store.create_agent(config, None, Default::default()).await?;
    let (page, more) = store
        .list_agents(Some(&first.id), "asc", 10)
        .await?
        .unwrap();
    assert_eq!(page, vec![third]);
    assert!(!more);
    assert_eq!(store.session(&session.id).await?, Some(session.clone()));
    assert!(store.delete_agent(&first.id).await?);
    assert!(store.session(&session.id).await?.is_some());
    assert!(
        store
            .list_agents(Some(&second.id), "asc", 1)
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn empty_sessions_survive_restart_with_independent_configuration() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = AbsolutePathBuf::from_absolute_path(directory.path())?;
    let store = Store::open(path.clone()).await?;
    let agent = store
        .create_agent(
            AgentConfig {
                model: "model-a".into(),
                instructions: Some("original".into()),
                ..Default::default()
            },
            None,
            Default::default(),
        )
        .await?;
    let first = store
        .create_session(
            agent.clone(),
            SessionCreateParams {
                agent_id: agent.id.clone(),
                environment: Environment::None,
            },
        )
        .await?;
    let second = store
        .create_session(
            agent.clone(),
            SessionCreateParams {
                agent_id: agent.id.clone(),
                environment: Environment::None,
            },
        )
        .await?;
    assert_ne!(first.id, second.id);
    let mut updated = agent.clone();
    updated.config.instructions = Some("changed".into());
    sqlx::query("UPDATE agents SET data = ? WHERE id = ?")
        .bind(serde_json::to_string(&updated)?)
        .bind(&agent.id)
        .execute(&store.0)
        .await?;
    store.0.close().await;
    drop(store); // Release the data-directory lock before reopening.
    let reopened = Store::open(path).await?;
    assert_eq!(reopened.agent(&agent.id).await?, Some(updated));
    assert_eq!(reopened.session(&first.id).await?, Some(first));
    assert_eq!(reopened.session(&second.id).await?, Some(second));
    assert_eq!(reopened.session("missing").await?, None);
    reopened.0.close().await;
    Ok(())
}

#[tokio::test]
async fn migrations_record_a_ledger_and_adopt_a_legacy_database() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = AbsolutePathBuf::from_absolute_path(directory.path())?;
    let store = Store::open(path.clone()).await?;
    let applied: i64 = sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations")
        .fetch_one(&store.0)
        .await?;
    assert!(applied >= 1, "baseline migration must be recorded");
    let agent = store
        .create_agent(
            AgentConfig {
                model: "legacy-model".into(),
                instructions: Some("keep me".into()),
                ..Default::default()
            },
            None,
            Default::default(),
        )
        .await?;
    // Simulate a pre-migration database: the tables and rows exist, but there
    // is no migration ledger. Reopening must adopt the baseline in place
    // without recreating tables or dropping the existing row.
    sqlx::query("DROP TABLE agents").execute(&store.0).await?;
    sqlx::query("DROP TABLE agent_sequence")
        .execute(&store.0)
        .await?;
    sqlx::query("CREATE TABLE agents (id TEXT PRIMARY KEY, data TEXT NOT NULL)")
        .execute(&store.0)
        .await?;
    sqlx::query("INSERT INTO agents (id, data) VALUES (?, ?)")
        .bind(&agent.id)
        .bind(serde_json::to_string(&agent)?)
        .execute(&store.0)
        .await?;
    for statement in [
        "DROP TABLE public_sessions",
        "DROP TABLE public_session_sequence",
        "DROP TABLE session_cleanup",
        "DROP TABLE input_requests",
        "DROP TABLE turn_usage",
        "DROP TABLE thread_usage_totals",
        "DROP TABLE subagents",
        "DROP TABLE vaults",
        "DROP TABLE vault_credentials",
        "DROP TABLE session_credentials",
        "DROP TABLE webhook_endpoints",
        "DROP TABLE webhook_deliveries",
        "DROP TABLE generations",
        "DROP TABLE environments",
        "DROP TABLE files",
        "DROP TABLE public_records",
        "CREATE TABLE public_records (seq INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL, kind TEXT NOT NULL, id TEXT NOT NULL, turn_id TEXT NOT NULL, data TEXT NOT NULL, UNIQUE (session_id, kind, id))",
        "CREATE TABLE public_sessions (id TEXT PRIMARY KEY, data TEXT NOT NULL)",
        "INSERT INTO public_sessions (id, data) VALUES ('legacy-session', '{}')",
    ] {
        sqlx::query(statement).execute(&store.0).await?;
    }
    sqlx::query("DROP TABLE _sqlx_migrations")
        .execute(&store.0)
        .await?;
    store.0.close().await;
    drop(store); // Release the data-directory lock before reopening.
    let adopted = Store::open(path).await?;
    let applied: i64 = sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations")
        .fetch_one(&adopted.0)
        .await?;
    assert!(applied >= 1, "adoption must re-record the baseline");
    assert_eq!(adopted.agent(&agent.id).await?, Some(agent));
    let sessions: Vec<(String, i64)> =
        sqlx::query_as("SELECT id, created_seq FROM public_sessions")
            .fetch_all(&adopted.0)
            .await?;
    assert_eq!(sessions, vec![("legacy-session".to_string(), 1)]);
    adopted.0.close().await;
    Ok(())
}

#[tokio::test]
async fn data_directory_lock_rejects_a_second_owner() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = AbsolutePathBuf::from_absolute_path(directory.path())?;
    let first = Store::open(path.clone()).await?;
    let error = Store::open(path.clone())
        .await
        .err()
        .context("a second owner must be rejected")?;
    assert!(error.to_string().contains("already in use"), "{error}");
    // Releasing the owner frees the directory for a new process.
    drop(first);
    Store::open(path).await?;
    Ok(())
}
