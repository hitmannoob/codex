//! Self-hosted environments: the executor a session runs its commands on,
//! reached through the service's environment registry.
use crate::State;

/// Whether a session owns this environment, so executors may register for it.
pub(crate) async fn exists(state: &State, environment_id: &str) -> anyhow::Result<bool> {
    Ok(
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM environments WHERE id = ?)")
            .bind(environment_id)
            .fetch_one(&state.store.0)
            .await?,
    )
}
