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
