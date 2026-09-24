use super::*;
use crate::resources::Environment;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn empty_sessions_survive_restart_with_independent_configuration() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = AbsolutePathBuf::from_absolute_path(directory.path())?;
    let store = Store::open(path.clone()).await?;
    let agent = store
        .create_agent(AgentConfig {
            model: "model-a".into(),
            instructions: "original".into(),
            ..Default::default()
        })
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
    updated.config.instructions = "changed".into();
    sqlx::query("UPDATE agents SET data = ? WHERE id = ?")
        .bind(serde_json::to_string(&updated)?)
        .bind(&agent.id)
        .execute(&store.0)
        .await?;
    store.0.close().await;
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
        .create_agent(AgentConfig {
            model: "legacy-model".into(),
            instructions: "keep me".into(),
            ..Default::default()
        })
        .await?;
    // Simulate a pre-migration database: the tables and rows exist, but there
    // is no migration ledger. Reopening must adopt the baseline in place
    // without recreating tables or dropping the existing row.
    sqlx::query("DROP TABLE _sqlx_migrations")
        .execute(&store.0)
        .await?;
    store.0.close().await;
    let adopted = Store::open(path).await?;
    let applied: i64 = sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations")
        .fetch_one(&adopted.0)
        .await?;
    assert!(applied >= 1, "adoption must re-record the baseline");
    assert_eq!(adopted.agent(&agent.id).await?, Some(agent));
    adopted.0.close().await;
    Ok(())
}
