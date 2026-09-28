//! Turn outcomes the worker cannot report itself: documented failure codes for
//! Codex errors, and turns that fail before the worker starts them.
use crate::ApiError;
use crate::State;
use serde_json::Value;
use serde_json::json;
use uuid::Uuid;

/// Map a Codex turn error to the documented failure categories; kinds without
/// a public counterpart are reported as `internal_error`.
pub(crate) fn turn_error(error: &Value) -> Value {
    let info = &error["codexErrorInfo"];
    let kind = info.as_str().or_else(|| {
        info.as_object()
            .and_then(|info| info.keys().next())
            .map(String::as_str)
    });
    let status = info
        .as_object()
        .and_then(|info| info.values().next())
        .and_then(|details| details["httpStatusCode"].as_u64());
    let code = match kind {
        Some("contextWindowExceeded") => "context_length_exceeded",
        Some("sessionBudgetExceeded") => "session_budget_exceeded",
        Some("usageLimitExceeded") => "usage_limit_exceeded",
        Some("rateLimitExceeded") => "rate_limit_exceeded",
        Some("serverOverloaded") => "server_overloaded",
        Some("cyberPolicy") => "cyber_policy",
        Some(
            "httpConnectionFailed"
            | "responseStreamConnectionFailed"
            | "responseStreamDisconnected",
        ) => "connection_failed",
        Some("responseTooManyFailedAttempts") if status == Some(429) => "rate_limit_exceeded",
        Some("internalServerError" | "responseTooManyFailedAttempts") => "server_error",
        Some("unauthorized") => "authentication_error",
        Some("badRequest") => "invalid_request",
        Some("sandboxError") => "sandbox_error",
        Some("activeTurnNotSteerable") => "active_turn_not_steerable",
        _ => "internal_error",
    };
    json!({"code":code,"message":error["message"].as_str().unwrap_or("turn failed")})
}

/// Record a turn that failed before the worker could start it, such as when a
/// required MCP server cannot initialize. The worker never assigned it an ID,
/// so it gets one here, and the session returns to idle as after any failed
/// turn. Returns `false` for sessions without public records.
pub(crate) async fn fail_unstarted_turn(
    state: &State,
    id: &str,
    error: Value,
) -> Result<bool, ApiError> {
    let mut tx = state
        .store
        .0
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(anyhow::Error::from)?;
    let agent_id: Option<String> = sqlx::query_scalar(
        "SELECT json_extract(data, '$.agent.id') FROM public_sessions WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(anyhow::Error::from)?;
    let Some(agent_id) = agent_id else {
        return Ok(false);
    };
    let now = crate::contract::now();
    let turn_id = format!("turn_{}", Uuid::new_v4());
    let queued = json!({"id":turn_id,"object":"agent.session.turn","session_id":id,"agent_id":agent_id,"created_at":now,
        "started_at":null,"completed_at":null,"status":"queued","usage":null,"subagent_id":null,"error":null});
    let mut failed = queued.clone();
    failed["status"] = json!("failed");
    failed["completed_at"] = json!(now);
    failed["error"] = error.clone();
    crate::records::save(&mut *tx, id, "turn", &failed, &turn_id).await?;
    let mut events = vec![
        json!({"type":"agent.session.turn.created","session_id":id,"turn_id":turn_id,"turn":queued}),
        json!({"type":"error","session_id":id,"error":{"type":"error","code":error["code"],"message":error["message"],"param":null}}),
        json!({"type":"agent.session.turn.failed","session_id":id,"turn_id":turn_id,"turn":failed}),
    ];
    events.extend(crate::records::transition(&mut tx, id, "idle", /*error*/ None).await?);
    tx.commit().await.map_err(anyhow::Error::from)?;
    for event in events {
        crate::records::emit(state, event);
    }
    Ok(true)
}
