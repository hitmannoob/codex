//! Public session management: listing, next-turn settings updates, and
//! deletion with durable cleanup of the Codex thread this service owns.
use crate::ApiError;
use crate::State;
use crate::contract::invalid;
use crate::resources::Agent;
use crate::resources::AgentConfig;
use axum::Json;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State as Extract;
use axum::http::StatusCode;
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;
use std::sync::Arc;

/// Internal broadcast marker that ends the deleted session's live streams.
/// Never forwarded to clients.
pub(crate) const DELETED: &str = "deleted";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ListParams {
    after: Option<String>,
    agent_id: Option<String>,
    limit: Option<u32>,
    order: Option<String>,
}

pub(crate) async fn list(
    Extract(state): Extract<Arc<State>>,
    Query(params): Query<ListParams>,
) -> Result<Json<Value>, ApiError> {
    let limit = params.limit.unwrap_or(/*default*/ 20);
    if !(1..=100).contains(&limit) {
        return Err(invalid("limit must be between 1 and 100"));
    }
    let ascending = match params.order.as_deref().unwrap_or("desc") {
        "asc" => true,
        "desc" => false,
        _ => return Err(invalid("order must be asc or desc")),
    };
    let pool = &state.store.0;
    let cursor: Option<i64> = match &params.after {
        Some(after) => Some(
            sqlx::query_scalar("SELECT created_seq FROM public_sessions WHERE id = ? AND (? IS NULL OR json_extract(data, '$.agent.id') = ?)")
                .bind(after).bind(&params.agent_id).bind(&params.agent_id)
                .fetch_optional(pool).await.map_err(anyhow::Error::from)?
                .ok_or_else(|| invalid("invalid session cursor"))?,
        ),
        None => None,
    };
    let query = if ascending {
        "SELECT id, data FROM public_sessions WHERE (? IS NULL OR json_extract(data, '$.agent.id') = ?) AND created_seq > ? ORDER BY created_seq ASC LIMIT ?"
    } else {
        "SELECT id, data FROM public_sessions WHERE (? IS NULL OR json_extract(data, '$.agent.id') = ?) AND created_seq < ? ORDER BY created_seq DESC LIMIT ?"
    };
    let rows: Vec<(String, String)> = sqlx::query_as(query)
        .bind(&params.agent_id)
        .bind(&params.agent_id)
        .bind(cursor.unwrap_or(if ascending { 0 } else { i64::MAX }))
        .bind(i64::from(limit) + 1)
        .fetch_all(pool)
        .await
        .map_err(anyhow::Error::from)?;
    let has_more = rows.len() > limit as usize;
    let mut data = Vec::new();
    for (id, session) in rows.into_iter().take(limit as usize) {
        let session = serde_json::from_str(&session).map_err(anyhow::Error::from)?;
        data.push(crate::records::decorate(pool, &id, session).await?);
    }
    Ok(Json(json!({
        "object":"list",
        "first_id":data.first().map(|session| &session["id"]),
        "last_id":data.last().map(|session| &session["id"]),
        "data":data,
        "has_more":has_more,
    })))
}

/// Replace metadata and next-turn model settings. A turn already running keeps
/// the settings it started with, because each turn reads the snapshot when it
/// starts.
pub(crate) async fn update(
    Extract(state): Extract<Arc<State>>,
    Path(id): Path<String>,
    Json(mut body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let fields = body
        .as_object_mut()
        .ok_or_else(|| invalid("session update must be an object"))?;
    if let Some(key) = fields
        .keys()
        .find(|key| !matches!(key.as_str(), "agent" | "metadata"))
    {
        return Err(invalid(format!("session field {key} cannot be updated")));
    }
    let metadata = fields
        .remove("metadata")
        .map(crate::configuration::metadata)
        .transpose()?;
    let settings = fields.remove("agent");
    // Status writers change only their own fields inside write transactions, so
    // this read-modify-write cannot lose or be lost to a concurrent transition.
    let mut tx = state
        .store
        .0
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(anyhow::Error::from)?;
    let row: Option<(String, String)> = sqlx::query_as("SELECT json_extract(s.data, '$.agent'), p.data FROM sessions s JOIN public_sessions p ON p.id = s.id WHERE s.id = ?")
        .bind(&id).fetch_optional(&mut *tx).await.map_err(anyhow::Error::from)?;
    let (agent, public) =
        row.ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "session not found".into()))?;
    let mut agent: Agent = serde_json::from_str(&agent).map_err(anyhow::Error::from)?;
    let mut public: Value = serde_json::from_str(&public).map_err(anyhow::Error::from)?;
    if let Some(settings) = settings {
        agent.config = next_turn_settings(agent.config, settings)?;
        sqlx::query("UPDATE sessions SET data = json_set(data, '$.agent', json(?)) WHERE id = ?")
            .bind(serde_json::to_string(&agent).map_err(anyhow::Error::from)?)
            .bind(&id)
            .execute(&mut *tx)
            .await
            .map_err(anyhow::Error::from)?;
        public["agent"] = crate::contract::agent(&agent);
    }
    if let Some(metadata) = metadata {
        public["metadata"] = json!(metadata);
    }
    sqlx::query("UPDATE public_sessions SET data = ? WHERE id = ?")
        .bind(public.to_string())
        .bind(&id)
        .execute(&mut *tx)
        .await
        .map_err(anyhow::Error::from)?;
    let public = crate::records::decorate(&mut *tx, &id, public).await?;
    tx.commit().await.map_err(anyhow::Error::from)?;
    Ok(Json(public))
}

fn next_turn_settings(config: AgentConfig, patch: Value) -> Result<AgentConfig, ApiError> {
    let Value::Object(mut patch) = patch else {
        return Err(invalid("agent must be an object"));
    };
    if let Some(key) = patch
        .keys()
        .find(|key| !matches!(key.as_str(), "model" | "reasoning" | "service_tier"))
    {
        return Err(invalid(format!(
            "session agent field {key} cannot be updated"
        )));
    }
    if let Some(reasoning) = patch.get_mut("reasoning") {
        let fields = reasoning
            .as_object()
            .ok_or_else(|| invalid("reasoning must be an object"))?;
        if fields.keys().any(|key| key != "effort") {
            return Err(invalid("only reasoning effort can be updated"));
        }
        // An omitted effort is kept, and the snapshot's summary setting is never
        // changed by a session update.
        let current = config.reasoning.as_ref();
        *reasoning = json!({
            "effort": fields.get("effort").cloned().unwrap_or_else(|| json!(current.and_then(|r| r.effort.as_ref()))),
            "summary": current.and_then(|r| r.summary.as_ref()),
        });
    }
    crate::configuration::configure(config, Value::Object(patch))
}

/// Remove a session once its execution has ended. The worker thread is deleted
/// afterwards from a durable queue; the caller's compute is never touched.
pub(crate) async fn delete(
    Extract(state): Extract<Arc<State>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let running = || {
        ApiError(
            StatusCode::CONFLICT,
            "cancel the running turn before deleting the session".into(),
        )
    };
    // Hold input admission so no turn can start between the check and removal.
    let admission = state.input_gates.lock(&id).await;
    let session = crate::records::session(&state, &id).await?;
    if matches!(
        session["status"].as_str(),
        Some("in_progress" | "requires_action")
    ) {
        return Err(running());
    }
    let thread_id: Option<String> =
        sqlx::query_scalar("SELECT thread_id FROM sessions WHERE id = ?")
            .bind(&id)
            .fetch_one(&state.store.0)
            .await
            .map_err(anyhow::Error::from)?;
    if let Some(thread_id) = &thread_id {
        // Public status can trail the worker, whose thread status is authoritative.
        let thread = state
            .rpc("thread/read", json!({"threadId": thread_id}))
            .await?;
        if thread["thread"]["status"]["type"] == "active" {
            return Err(running());
        }
    }
    let mut tx = state
        .store
        .0
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(anyhow::Error::from)?;
    for statement in [
        "DELETE FROM public_records WHERE session_id = ?",
        "DELETE FROM tool_calls WHERE session_id = ?",
        "DELETE FROM input_requests WHERE session_id = ?",
        "DELETE FROM turn_usage WHERE session_id = ?",
        "DELETE FROM usage_totals WHERE session_id = ?",
        "DELETE FROM public_sessions WHERE id = ?",
        "DELETE FROM sessions WHERE id = ?",
    ] {
        sqlx::query(statement)
            .bind(&id)
            .execute(&mut *tx)
            .await
            .map_err(anyhow::Error::from)?;
    }
    if let Some(thread_id) = &thread_id {
        sqlx::query("INSERT INTO session_cleanup (session_id, thread_id) VALUES (?, ?)")
            .bind(&id)
            .bind(thread_id)
            .execute(&mut *tx)
            .await
            .map_err(anyhow::Error::from)?;
    }
    tx.commit().await.map_err(anyhow::Error::from)?;
    drop(admission);
    let _ = state
        .public_events
        .send(json!({"type": DELETED, "session_id": id}));
    let cleaner = Arc::clone(&state);
    tokio::spawn(async move {
        if let Err(error) = cleanup(&cleaner).await {
            eprintln!("agents-api: session cleanup failed: {error:#}");
        }
    });
    Ok(Json(
        json!({"id":id,"object":"agent.session.deleted","deleted":true}),
    ))
}

/// Delete the worker threads of deleted sessions. A queue entry is removed only
/// after the worker confirms deletion, so an API crash or backend loss retries
/// on the next connection; a thread that is already gone counts as removed.
pub(crate) async fn cleanup(state: &State) -> anyhow::Result<()> {
    let pending: Vec<(String, String)> =
        sqlx::query_as("SELECT session_id, thread_id FROM session_cleanup")
            .fetch_all(&state.store.0)
            .await?;
    for (session_id, thread_id) in pending {
        match state
            .rpc("thread/delete", json!({"threadId": thread_id}))
            .await
        {
            Ok(_) => {}
            // An earlier attempt deleted it but did not record completion.
            Err(error)
                if error.0 == StatusCode::BAD_GATEWAY
                    && error.1.starts_with("thread not found") => {}
            Err(error) => {
                eprintln!(
                    "agents-api: deleting thread {thread_id} of session {session_id} failed: {}",
                    error.1
                );
                continue;
            }
        }
        sqlx::query("DELETE FROM session_cleanup WHERE session_id = ?")
            .bind(&session_id)
            .execute(&state.store.0)
            .await?;
    }
    Ok(())
}
