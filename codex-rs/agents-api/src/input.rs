//! Session input events: whole-batch validation, in-order execution, and
//! `Idempotency-Key` request deduplication. Deduplication replays a recorded
//! HTTP outcome; it cannot prove whether interrupted execution happened, so a
//! request interrupted during dispatch leaves its key with an unknown outcome
//! rather than being executed again.
use crate::ApiError;
use crate::State;
use crate::contract::invalid;
use crate::resources::ToolResult;
use axum::Json;
use axum::extract::Path;
use axum::extract::State as Extract;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;
use std::collections::HashSet;
use std::sync::Arc;

/// Most input events accepted in one request.
const MAX_EVENTS: usize = 32;
/// Most combined text bytes in one user message.
const MAX_TEXT_BYTES: usize = 8192;
/// How long an `Idempotency-Key` outcome stays available for replay.
const KEY_RETENTION_SECS: u64 = 24 * 60 * 60;

/// A call's item event can reach clients before the worker's request registers
/// it, and the pinned SDK retries a tool result rejected with exactly this
/// message prefix and the `invalid_request_error` code.
pub(crate) const UNKNOWN_PENDING_CALL: &str = "Unknown pending tool call: ";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Inputs {
    events: Vec<Input>,
}

#[derive(Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum Input {
    #[serde(rename = "agent.session.input.message")]
    Message { input: Value },
    #[serde(rename = "agent.session.input.cancel")]
    Cancel,
    #[serde(rename = "agent.session.input.tool_result")]
    ToolResult {
        call_id: String,
        turn_id: String,
        success: bool,
        output: Option<Value>,
        error: Option<String>,
    },
}

/// A validated input event, ready to execute.
enum Event {
    Message(Vec<Value>),
    Cancel,
    ToolResult { turn_id: String, result: ToolResult },
}

/// Convert a string or one user message into Codex input items. The single
/// message boundary is preserved: several messages are rejected, not merged.
pub(crate) fn message(input: Value) -> Result<Vec<Value>, ApiError> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Message {
        role: String,
        content: Vec<Content>,
        #[serde(rename = "type")]
        kind: Option<String>,
    }
    #[derive(Deserialize)]
    #[serde(tag = "type", deny_unknown_fields)]
    enum Content {
        #[serde(rename = "input_text")]
        Text { text: String },
        #[serde(rename = "input_image")]
        Image { image_url: String },
    }
    let content = if let Value::String(text) = input {
        vec![Content::Text { text }]
    } else {
        let messages: Vec<Message> =
            serde_json::from_value(input).map_err(|e| invalid(e.to_string()))?;
        let [message] = <[Message; 1]>::try_from(messages)
            .map_err(|_| invalid("exactly one user message is currently supported"))?;
        if message.role != "user" || message.kind.is_some_and(|kind| kind != "message") {
            return Err(invalid("input must be a user message"));
        }
        message.content
    };
    let mut items = Vec::new();
    let mut text_bytes = 0;
    let mut substantive = false;
    for content in content {
        items.push(match content {
            Content::Text { text } => {
                text_bytes += text.len();
                substantive |= !text.trim().is_empty();
                json!({"type": "text", "text": text})
            }
            Content::Image { image_url } => {
                // Codex prepares inline images itself and rejects remote URLs.
                if !image_url.starts_with("data:image/") {
                    return Err(invalid(
                        "input_image requires a data:image URL; remote image URLs are not supported",
                    ));
                }
                substantive = true;
                json!({"type": "image", "url": image_url})
            }
        });
    }
    if !substantive || text_bytes > MAX_TEXT_BYTES {
        return Err(invalid(format!(
            "input must contain non-blank text or an image, with at most {MAX_TEXT_BYTES} text bytes"
        )));
    }
    Ok(items)
}

/// Validate a whole batch before any event runs. A batch holds tool results
/// plus at most one message or one cancel event, never both, and resolves each
/// call once; events then run in array order.
fn plan(events: Vec<Input>) -> Result<Vec<Event>, ApiError> {
    if events.is_empty() || events.len() > MAX_EVENTS {
        return Err(invalid(format!(
            "events must contain 1-{MAX_EVENTS} input events"
        )));
    }
    if events
        .iter()
        .filter(|event| matches!(event, Input::Message { .. } | Input::Cancel))
        .count()
        > 1
    {
        return Err(invalid(
            "a request may contain at most one message or cancel event",
        ));
    }
    let mut calls = HashSet::new();
    events
        .into_iter()
        .map(|event| {
            Ok(match event {
                Input::Message { input } => Event::Message(message(input)?),
                Input::Cancel => Event::Cancel,
                Input::ToolResult {
                    call_id,
                    turn_id,
                    success,
                    output,
                    error,
                } => {
                    if !calls.insert((turn_id.clone(), call_id.clone())) {
                        return Err(invalid(format!(
                            "tool call {call_id} is resolved more than once"
                        )));
                    }
                    let output = crate::actions::result_output(success, output, error)?;
                    Event::ToolResult {
                        turn_id,
                        result: ToolResult {
                            call_id,
                            success,
                            output,
                        },
                    }
                }
            })
        })
        .collect()
}

/// Check the session state every event relies on, so a batch that cannot be
/// accepted is rejected before anything reaches the worker.
async fn check(state: &State, id: &str, events: &[Event]) -> Result<(), ApiError> {
    if !state.connected() {
        return Err(crate::disconnected_error());
    }
    for event in events {
        let Event::ToolResult { turn_id, result } = event else {
            continue;
        };
        let row: Option<(String, Option<String>)> = sqlx::query_as(
            "SELECT status, result FROM tool_calls WHERE session_id = ? AND turn_id = ? AND call_id = ?",
        )
        .bind(id)
        .bind(turn_id)
        .bind(&result.call_id)
        .fetch_optional(&state.store.0)
        .await
        .map_err(anyhow::Error::from)?;
        let Some((status, previous)) = row else {
            return Err(invalid(format!("{UNKNOWN_PENDING_CALL}{}", result.call_id)));
        };
        let identical =
            previous == Some(serde_json::to_string(result).map_err(anyhow::Error::from)?);
        if status != "pending" && !(status == "submitted" && identical) {
            return Err(ApiError(
                StatusCode::CONFLICT,
                format!("tool call is {status}; result cannot be submitted"),
            ));
        }
    }
    Ok(())
}

async fn execute(state: &Arc<State>, id: &str, events: Vec<Event>) -> Result<(), ApiError> {
    for event in events {
        match event {
            Event::Message(items) => {
                crate::routes::start_turn(state, id, items).await?;
            }
            // Cancelling stops all of the session's running work: its own turn
            // and any turn a subagent is running.
            Event::Cancel => {
                for (thread_id, turn_id, subagent) in
                    crate::records::running_turns(state, id).await?
                {
                    match state
                        .rpc(
                            "turn/interrupt",
                            json!({"threadId": thread_id, "turnId": turn_id}),
                        )
                        .await
                    {
                        Ok(_) => {}
                        // A subagent's turn can end on its own before the interrupt.
                        Err(error) if subagent && error.0 == StatusCode::BAD_GATEWAY => {}
                        Err(error) => return Err(error),
                    }
                }
            }
            Event::ToolResult { turn_id, result } => {
                crate::actions::resolve_call(state, id.to_owned(), turn_id, result).await?;
            }
        }
    }
    Ok(())
}

pub(crate) async fn create(
    Extract(state): Extract<Arc<State>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<Value>,
) -> Result<StatusCode, ApiError> {
    let key = headers
        .get("idempotency-key")
        .map(|key| {
            key.to_str()
                .ok()
                .filter(|key| (1..=255).contains(&key.len()))
                .map(str::to_owned)
                .ok_or_else(|| invalid("Idempotency-Key must be 1-255 visible ASCII characters"))
        })
        .transpose()?;
    let Inputs { events } =
        serde_json::from_value(request.clone()).map_err(|e| invalid(e.to_string()))?;
    let events = plan(events)?;
    crate::records::session(&state, &id).await?;
    let Some(key) = key else {
        check(&state, &id, &events).await?;
        execute(&state, &id, events).await?;
        return Ok(StatusCode::ACCEPTED);
    };
    if let Claim::Replay(outcome) = claim(&state, &id, &key, &request).await? {
        return outcome;
    }
    if let Err(error) = check(&state, &id, &events).await {
        // Nothing was dispatched, so the key stays free for a corrected retry.
        sqlx::query("DELETE FROM input_requests WHERE session_id = ? AND key = ?")
            .bind(&id)
            .bind(&key)
            .execute(&state.store.0)
            .await
            .map_err(anyhow::Error::from)?;
        return Err(error);
    }
    let outcome = execute(&state, &id, events).await;
    let (record, status, message) = match &outcome {
        Ok(()) => ("completed", StatusCode::ACCEPTED, None),
        // Dispatch may have reached the worker before the connection or
        // process failed; whether it took effect cannot be established.
        Err(error)
            if matches!(
                error.0,
                StatusCode::SERVICE_UNAVAILABLE | StatusCode::INTERNAL_SERVER_ERROR
            ) =>
        {
            ("unknown", error.0, Some(error.1.as_str()))
        }
        Err(error) => ("completed", error.0, Some(error.1.as_str())),
    };
    sqlx::query("UPDATE input_requests SET state = ?, status = ?, message = ? WHERE session_id = ? AND key = ?")
        .bind(record).bind(i64::from(status.as_u16())).bind(message).bind(&id).bind(&key)
        .execute(&state.store.0).await.map_err(anyhow::Error::from)?;
    outcome.map(|()| StatusCode::ACCEPTED)
}

enum Claim {
    New,
    Replay(Result<StatusCode, ApiError>),
}

/// Claim an unused key for this request, or return the stored outcome of the
/// identical request that used it. Keys are scoped to one session.
async fn claim(state: &State, id: &str, key: &str, request: &Value) -> Result<Claim, ApiError> {
    let now = crate::contract::now();
    let mut tx = state
        .store
        .0
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(anyhow::Error::from)?;
    sqlx::query("DELETE FROM input_requests WHERE created_at < ?")
        .bind(now.saturating_sub(KEY_RETENTION_SECS) as i64)
        .execute(&mut *tx)
        .await
        .map_err(anyhow::Error::from)?;
    let row: Option<(String, String, Option<i64>, Option<String>)> = sqlx::query_as(
        "SELECT request, state, status, message FROM input_requests WHERE session_id = ? AND key = ?",
    )
    .bind(id)
    .bind(key)
    .fetch_optional(&mut *tx)
    .await
    .map_err(anyhow::Error::from)?;
    let Some((stored, record, status, message)) = row else {
        sqlx::query("INSERT INTO input_requests (session_id, key, request, state, created_at) VALUES (?, ?, ?, 'pending', ?)")
            .bind(id).bind(key).bind(request.to_string()).bind(now as i64)
            .execute(&mut *tx).await.map_err(anyhow::Error::from)?;
        tx.commit().await.map_err(anyhow::Error::from)?;
        return Ok(Claim::New);
    };
    tx.commit().await.map_err(anyhow::Error::from)?;
    if serde_json::from_str::<Value>(&stored).map_err(anyhow::Error::from)? != *request {
        return Err(invalid(
            "Idempotency-Key was already used with a different request",
        ));
    }
    match (record.as_str(), status) {
        ("completed", Some(status)) => {
            let status = StatusCode::from_u16(u16::try_from(status).map_err(anyhow::Error::from)?)
                .map_err(anyhow::Error::from)?;
            Ok(Claim::Replay(if status == StatusCode::ACCEPTED {
                Ok(status)
            } else {
                Err(ApiError(status, message.unwrap_or_default()))
            }))
        }
        ("pending", _) => Err(ApiError(
            StatusCode::CONFLICT,
            "a request with this Idempotency-Key is still in progress".into(),
        )),
        _ => Err(ApiError(
            StatusCode::CONFLICT,
            "the outcome of the request with this Idempotency-Key is unknown; read the session and submit again with a new key".into(),
        )),
    }
}
