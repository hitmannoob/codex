//! Self-hosted environments: the executor a session runs its commands on,
//! reached through the service's environment registry.
//!
//! The caller owns the compute. A session records its environment, reports
//! the executor's connection as `agent.session.environment.*` events, and
//! attaches the environment to the worker before the session's first turn.
//! Input that arrives while the executor is offline waits up to five minutes
//! behind an `environment_connection` required action, then starts in order.
//! Input that times out is dropped, never replayed by a late connection.
use crate::ApiError;
use crate::State;
use crate::contract::invalid;
use crate::registry::ExecutorState;
use axum::Json;
use axum::extract::Path;
use axum::extract::State as Extract;
use axum::http::StatusCode;
use serde_json::Value;
use serde_json::json;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::time::Duration;

/// How long input waits for a disconnected executor.
const CONNECTION_WAIT: Duration = Duration::from_secs(5 * 60);
const MAX_PATH_BYTES: usize = 4096;
const TIMED_OUT: &str =
    "the environment did not connect within five minutes; the input waiting for it was dropped";
const RESTARTED: &str =
    "the service restarted while input waited for the environment; that input was dropped";

/// Input held while a session's executor is offline, by session ID.
#[derive(Default)]
pub(crate) struct Waits(Mutex<HashMap<String, Wait>>);

struct Wait {
    inputs: VecDeque<Vec<Value>>,
    /// Tells this wait apart from a later one for the same session.
    generation: u64,
}

/// The workspace directory of a `self_hosted` environment parameter.
pub(crate) fn parse(params: &Value) -> Result<String, ApiError> {
    let fields = params
        .as_object()
        .ok_or_else(|| invalid("environment must be an object"))?;
    if let Some(unknown) = fields.keys().find(|key| {
        !matches!(
            key.as_str(),
            "type" | "workspace_directory" | "capability_directories"
        )
    }) {
        return Err(invalid(format!(
            "unknown field in self_hosted environment: {unknown}"
        )));
    }
    match &params["capability_directories"] {
        Value::Null => {}
        Value::Array(directories) if directories.is_empty() => {}
        _ => {
            return Err(invalid(
                "capability_directories is not supported by this service yet",
            ));
        }
    }
    let directory = params["workspace_directory"]
        .as_str()
        .ok_or_else(|| invalid("workspace_directory is required for self_hosted environments"))?;
    // The executor may run another OS, so accept either absolute form and
    // leave resolution to it.
    let bytes = directory.as_bytes();
    let absolute = directory.starts_with('/')
        || directory.starts_with("\\\\")
        || (bytes.len() >= 3
            && bytes[0].is_ascii_alphabetic()
            && bytes[1] == b':'
            && matches!(bytes[2], b'\\' | b'/'));
    if !absolute || directory.len() > MAX_PATH_BYTES || directory.contains('\0') {
        return Err(invalid(format!(
            "workspace_directory must be an absolute path of at most {MAX_PATH_BYTES} bytes"
        )));
    }
    Ok(directory.to_owned())
}

/// Record a session's environment and return its public object.
pub(crate) async fn create(
    state: &State,
    session_id: &str,
    environment_id: &str,
    workspace_directory: &str,
    remote_url: String,
) -> anyhow::Result<Value> {
    let public = json!({"id": environment_id, "type": "self_hosted",
        "workspace_directory": workspace_directory, "capability_directories": [], "remote_url": remote_url});
    sqlx::query("INSERT INTO environments (id, session_id, data) VALUES (?, ?, ?)")
        .bind(environment_id)
        .bind(session_id)
        .bind(public.to_string())
        .execute(&state.store.0)
        .await?;
    Ok(public)
}

/// Whether a session owns this environment, so executors may register for it.
pub(crate) async fn exists(state: &State, environment_id: &str) -> anyhow::Result<bool> {
    Ok(
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM environments WHERE id = ?)")
            .bind(environment_id)
            .fetch_one(&state.store.0)
            .await?,
    )
}

pub(crate) async fn retrieve(
    Extract(state): Extract<Arc<State>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let data: Option<String> = sqlx::query_scalar("SELECT data FROM environments WHERE id = ?")
        .bind(&id)
        .fetch_optional(&state.store.0)
        .await
        .map_err(anyhow::Error::from)?;
    let data =
        data.ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "environment not found".into()))?;
    Ok(Json(
        serde_json::from_str(&data).map_err(anyhow::Error::from)?,
    ))
}

/// Broadcast an environment state change on the session's stream.
pub(crate) fn emit(
    state: &State,
    session_id: &str,
    environment_id: &str,
    status: &str,
    error: Option<Value>,
) {
    crate::records::emit(
        state,
        json!({"type": format!("agent.session.environment.{status}"), "session_id": session_id, "turn_id": null,
            "environment": {"id": environment_id, "type": "self_hosted", "status": status, "error": error}}),
    );
}

/// Report the executor connecting and disconnecting until the environment is
/// deleted.
pub(crate) fn watch(state: &Arc<State>, session_id: String, environment_id: String) {
    let mut executor = state.registry.watch(&environment_id);
    let state = Arc::downgrade(state);
    tokio::spawn(async move {
        let mut connected = *executor.borrow_and_update() == ExecutorState::Connected;
        // Deleting the environment drops its slot, which ends this loop.
        while executor.changed().await.is_ok() {
            let now = *executor.borrow_and_update() == ExecutorState::Connected;
            let Some(state) = state.upgrade() else {
                return;
            };
            if now != connected {
                connected = now;
                let status = if now { "connected" } else { "disconnected" };
                emit(
                    &state,
                    &session_id,
                    &environment_id,
                    status,
                    /*error*/ None,
                );
            }
        }
    });
}

/// Watch every stored environment again, and fail input that waited for an
/// executor when the service stopped: it was never dispatched.
pub(crate) async fn recover(state: &Arc<State>) -> anyhow::Result<()> {
    let rows: Vec<(String, String, bool)> =
        sqlx::query_as("SELECT id, session_id, waiting FROM environments")
            .fetch_all(&state.store.0)
            .await?;
    for (environment_id, session_id, waiting) in rows {
        watch(state, session_id.clone(), environment_id);
        if waiting {
            settle(state, &session_id, Some(("failed", Some(RESTARTED)))).await?;
        }
    }
    Ok(())
}

/// Add a session's environment to the current worker connection, once per
/// connection.
pub(crate) async fn attach(state: &State, environment_id: &str) -> Result<(), ApiError> {
    let attached = state.attached_environments()?;
    if crate::lock(&attached).contains(environment_id) {
        return Ok(());
    }
    let url = state.registry.harness_url().ok_or_else(|| {
        ApiError(
            StatusCode::NOT_IMPLEMENTED,
            "self-hosted environments are disabled; the operator must configure an environment key"
                .into(),
        )
    })?;
    state
        .rpc(
            "environment/add",
            json!({"environmentId": environment_id, "noiseRegistry": {
                "url": url, "environmentId": environment_id, "authToken": state.registry.harness_token()}}),
        )
        .await?;
    crate::lock(&attached).insert(environment_id.to_owned());
    Ok(())
}

/// Start a turn with this input, or hold it until the session's executor
/// connects.
pub(crate) async fn submit(
    state: &Arc<State>,
    session_id: &str,
    items: Vec<Value>,
) -> Result<(), ApiError> {
    let environment_id: Option<String> =
        sqlx::query_scalar("SELECT id FROM environments WHERE session_id = ?")
            .bind(session_id)
            .fetch_optional(&state.store.0)
            .await
            .map_err(anyhow::Error::from)?;
    let Some(environment_id) = environment_id else {
        crate::routes::start_turn(state, session_id, items).await?;
        return Ok(());
    };
    let admission = state.input_gates.lock(session_id).await;
    let connected = *state.registry.watch(&environment_id).borrow() == ExecutorState::Connected;
    let generation = state.registry.next_connection();
    let ready = {
        let mut waits = crate::lock(&state.waits.0);
        if let Some(wait) = waits.get_mut(session_id) {
            // Earlier input is still waiting or starting; keep the order.
            wait.inputs.push_back(items);
            return Ok(());
        }
        if connected {
            Some(items)
        } else {
            let inputs = VecDeque::from([items]);
            waits.insert(session_id.to_owned(), Wait { inputs, generation });
            None
        }
    };
    if let Some(items) = ready {
        drop(admission);
        crate::routes::start_turn(state, session_id, items).await?;
        return Ok(());
    }
    let mut tx = state
        .store
        .0
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(anyhow::Error::from)?;
    sqlx::query("UPDATE environments SET waiting = 1 WHERE id = ?")
        .bind(&environment_id)
        .execute(&mut *tx)
        .await
        .map_err(anyhow::Error::from)?;
    let event =
        crate::records::transition(&mut tx, session_id, "requires_action", /*error*/ None).await?;
    tx.commit().await.map_err(anyhow::Error::from)?;
    drop(admission);
    if let Some(event) = event {
        crate::records::emit(state, event);
    }
    tokio::spawn(wait(
        Arc::downgrade(state),
        session_id.to_owned(),
        environment_id,
        generation,
    ));
    Ok(())
}

/// Wait for the executor, then start the held input in order, or drop it when
/// the wait times out.
async fn wait(state: Weak<State>, session_id: String, environment_id: String, generation: u64) {
    let Some(mut executor) = state
        .upgrade()
        .map(|state| state.registry.watch(&environment_id))
    else {
        return;
    };
    let connected = tokio::time::timeout(
        CONNECTION_WAIT,
        executor.wait_for(|executor| *executor == ExecutorState::Connected),
    )
    .await
    .is_ok_and(|result| result.is_ok());
    drop(executor);
    let Some(state) = state.upgrade() else {
        return;
    };
    let current = |waits: &HashMap<String, Wait>| {
        waits
            .get(&session_id)
            .is_some_and(|wait| wait.generation == generation)
    };
    let admission = state.input_gates.lock(&session_id).await;
    {
        let mut waits = crate::lock(&state.waits.0);
        // A cancel already dropped this input.
        if !current(&waits) {
            return;
        }
        if !connected {
            waits.remove(&session_id);
        }
    }
    let dropped = if connected {
        None
    } else {
        Some(("failed", Some(TIMED_OUT)))
    };
    if let Err(error) = settle(&state, &session_id, dropped).await {
        tracing::warn!(
            error = format!("{error:#}"),
            "environment wait failed to settle"
        );
    }
    drop(admission);
    if !connected {
        let error = json!({"type": "environment_error", "code": "environment_connection_timeout", "message": TIMED_OUT});
        emit(&state, &session_id, &environment_id, "failed", Some(error));
        return;
    }
    // Input arriving meanwhile joins the queue, so it starts after this.
    loop {
        let items = {
            let mut waits = crate::lock(&state.waits.0);
            if !current(&waits) {
                return;
            }
            match waits
                .get_mut(&session_id)
                .and_then(|wait| wait.inputs.pop_front())
            {
                Some(items) => items,
                None => {
                    waits.remove(&session_id);
                    return;
                }
            }
        };
        if let Err(error) = crate::routes::start_turn(&state, &session_id, items).await {
            crate::lock(&state.waits.0).remove(&session_id);
            let _ =
                crate::records::session_status(&state, &session_id, "failed", Some(&error.1)).await;
            return;
        }
    }
}

/// End a session's wait for its executor. Input that was dropped moves the
/// session to `dropped` (status and error); input about to start leaves the
/// status to the turn.
async fn settle(
    state: &State,
    session_id: &str,
    dropped: Option<(&str, Option<&str>)>,
) -> anyhow::Result<()> {
    let mut tx = state.store.0.begin_with("BEGIN IMMEDIATE").await?;
    sqlx::query("UPDATE environments SET waiting = 0 WHERE session_id = ?")
        .bind(session_id)
        .execute(&mut *tx)
        .await?;
    let event = match dropped {
        Some((status, error)) => {
            crate::records::transition(&mut tx, session_id, status, error).await?
        }
        None => None,
    };
    tx.commit().await?;
    if let Some(event) = event {
        crate::records::emit(state, event);
    }
    Ok(())
}

/// Drop input still waiting for the executor, returning the session to idle.
/// Returns whether any input was waiting.
pub(crate) async fn cancel(state: &State, session_id: &str) -> Result<bool, ApiError> {
    let _admission = state.input_gates.lock(session_id).await;
    let waiting = crate::lock(&state.waits.0).remove(session_id).is_some();
    if waiting {
        settle(state, session_id, Some(("idle", /*error*/ None))).await?;
    }
    Ok(waiting)
}

/// Forget a deleted session's environment: its executor sockets close and the
/// worker stops using it. The caller's compute keeps running.
pub(crate) async fn forget(state: &State, environment_id: &str) {
    state.registry.remove(environment_id);
    let attached = match state.attached_environments() {
        Ok(attached) => attached,
        Err(_) => return,
    };
    if crate::lock(&attached).remove(environment_id)
        && let Err(error) = state
            .rpc(
                "environment/remove",
                json!({"environmentId": environment_id}),
            )
            .await
    {
        tracing::warn!(
            environment_id,
            error = error.1,
            "worker kept a deleted environment"
        );
    }
}
