use crate::ApiError;
use crate::State;
use crate::contract::invalid;
use crate::resources::RequiredAction;
use crate::resources::ToolResult;
use axum::Json;
use axum::extract::Path;
use axum::extract::State as Extract;
use axum::http::StatusCode;
use codex_app_server_client::AppServerClient;
use codex_app_server_protocol::DynamicToolCallParams;
use codex_app_server_protocol::RequestId;
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::oneshot;

pub(crate) struct Submission {
    session_id: String,
    turn_id: String,
    result: ToolResult,
    reply: oneshot::Sender<Result<Value, ApiError>>,
}

/// Most UTF-8 bytes of text in one public function result. Codex silently
/// truncates longer tool output (10,000 bytes, or 10,000 tokens for catalogued
/// models), so a larger result is rejected rather than shortened unseen.
const MAX_RESULT_TEXT_BYTES: usize = 10_000;

/// Validate a public function result and return its stored form: the output
/// (a string or `input_text`/`input_image` parts) on success, or the error
/// message on failure.
pub(crate) fn result_output(
    success: bool,
    output: Option<Value>,
    error: Option<String>,
) -> Result<Value, ApiError> {
    #[derive(Deserialize)]
    #[serde(tag = "type", deny_unknown_fields)]
    enum Part {
        #[serde(rename = "input_text")]
        Text { text: String },
        #[serde(rename = "input_image")]
        Image { image_url: String },
    }
    let (stored, text_bytes) = match (success, output, error) {
        (true, _, Some(_)) => {
            return Err(invalid("a successful tool result cannot contain an error"));
        }
        (false, Some(_), _) => {
            return Err(invalid(
                "a failed tool result reports its message in error, not output",
            ));
        }
        (true, None, None) => (json!(""), 0),
        (true, Some(Value::String(text)), None) => {
            let bytes = text.len();
            (Value::String(text), bytes)
        }
        (true, Some(parts), None) => {
            let parsed: Vec<Part> = serde_json::from_value(parts.clone()).map_err(|_| {
                invalid("output must be a string or an array of input_text and input_image parts")
            })?;
            let mut bytes = 0;
            for part in parsed {
                match part {
                    Part::Text { text } => bytes += text.len(),
                    // Codex prepares inline images itself and rejects remote URLs.
                    Part::Image { image_url } if !image_url.starts_with("data:image/") => {
                        return Err(invalid(
                            "input_image requires a data:image URL; remote image URLs are not supported",
                        ));
                    }
                    Part::Image { .. } => {}
                }
            }
            (parts, bytes)
        }
        (false, None, error) => {
            let error = error.unwrap_or_default();
            let bytes = error.len();
            (Value::String(error), bytes)
        }
    };
    if text_bytes > MAX_RESULT_TEXT_BYTES {
        return Err(invalid(format!(
            "tool result text must be at most {MAX_RESULT_TEXT_BYTES} bytes"
        )));
    }
    Ok(stored)
}

/// Codex content items for a stored result: a string is one text item and
/// public content parts map one to one. Other JSON, which only the prototype
/// route accepts, is sent as its serialized text.
fn content_items(output: &Value) -> Value {
    let parts = output.as_array().and_then(|parts| {
        parts
            .iter()
            .map(|part| match part["type"].as_str() {
                Some("input_text") => Some(json!({"type": "inputText", "text": part["text"]})),
                Some("input_image") => {
                    Some(json!({"type": "inputImage", "imageUrl": part["image_url"]}))
                }
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
    });
    match (parts, output) {
        (Some(parts), _) => json!(parts),
        (None, Value::String(text)) => json!([{"type": "inputText", "text": text}]),
        (None, other) => json!([{"type": "inputText", "text": other.to_string()}]),
    }
}

/// Apply the prototype limit on serialized tool output.
pub(crate) fn validate_output(output: &Value) -> Result<(), ApiError> {
    if serde_json::to_vec(output)
        .map_err(anyhow::Error::from)?
        .len()
        > 1024
    {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "tool output must be at most 1024 bytes".into(),
        ));
    }
    Ok(())
}

/// Prototype route: any JSON output, within the prototype size limit.
pub(crate) async fn submit(
    Extract(state): Extract<Arc<State>>,
    Path((session_id, turn_id)): Path<(String, String)>,
    Json(result): Json<ToolResult>,
) -> Result<Json<Value>, ApiError> {
    validate_output(&result.output)?;
    Ok(Json(
        resolve_call(&state, session_id, turn_id, result).await?,
    ))
}

/// Hand a validated result to the connection that owns the call and wait for
/// its receipt.
pub(crate) async fn resolve_call(
    state: &State,
    session_id: String,
    turn_id: String,
    result: ToolResult,
) -> Result<Value, ApiError> {
    let (reply, received) = oneshot::channel();
    let submissions = state.submissions().ok_or_else(crate::disconnected_error)?;
    submissions
        .send(Submission {
            session_id,
            turn_id,
            result,
            reply,
        })
        .await
        .map_err(|_| crate::disconnected_error())?;
    received.await.map_err(|_| {
        ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "tool result delivery unknown".into(),
        )
    })?
}

pub(crate) async fn register(
    state: &Arc<State>,
    request_id: &RequestId,
    params: DynamicToolCallParams,
) -> anyhow::Result<()> {
    let session_id: String = sqlx::query_scalar("SELECT id FROM sessions WHERE thread_id = ?")
        .bind(&params.thread_id)
        .fetch_one(&state.store.0)
        .await?;
    let session = state
        .store
        .session(&session_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing session"))?;
    anyhow::ensure!(
        params.namespace.is_none()
            && session
                .agent
                .config
                .tools
                .iter()
                .filter_map(crate::agent_tools::Tool::function)
                .any(|tool| tool.name == params.tool),
        "unregistered function"
    );
    anyhow::ensure!(
        session.required_actions.len() < 32 && serde_json::to_vec(&params.arguments)?.len() <= 8192,
        "too many or oversized pending calls"
    );
    let action = RequiredAction {
        turn_id: params.turn_id,
        call_id: params.call_id,
        name: params.tool,
        arguments: params.arguments,
    };
    sqlx::query("INSERT INTO tool_calls (session_id, turn_id, call_id, request_id, action, status) VALUES (?, ?, ?, ?, ?, 'pending')")
        .bind(&session_id).bind(&action.turn_id).bind(&action.call_id).bind(serde_json::to_string(request_id)?)
        .bind(serde_json::to_string(&action)?).execute(&state.store.0).await?;
    let event = json!({"method": "session.requires_action", "params": {"threadId": params.thread_id, "sessionId": session_id, "action": action}});
    crate::records::notification(state, &event)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e.1))?;
    let _ = state.events.send(event);
    Ok(())
}

pub(crate) async fn resolve(state: &State, client: &AppServerClient, submission: Submission) {
    let result = deliver(state, client, &submission).await;
    let _ = submission.reply.send(result);
}

// Runs inside the connection's pump task, which is the sole processor of its
// submissions and holds the matching `client`. The pump exits before any
// replacement connection is installed, so this delivery cannot cross into a
// different backend generation.
async fn deliver(
    state: &State,
    client: &AppServerClient,
    submission: &Submission,
) -> Result<Value, ApiError> {
    let Submission {
        session_id,
        turn_id,
        result,
        ..
    } = submission;
    let row: Option<(String, String, Option<String>)> = sqlx::query_as("SELECT request_id, status, result FROM tool_calls WHERE session_id = ? AND turn_id = ? AND call_id = ?")
        .bind(session_id).bind(turn_id).bind(&result.call_id).fetch_optional(&state.store.0).await.map_err(anyhow::Error::from)?;
    let Some((request_id, status, previous)) = row else {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            "tool call not found".into(),
        ));
    };
    let serialized = serde_json::to_string(result).map_err(anyhow::Error::from)?;
    let receipt = json!({"turnId": turn_id, "callId": result.call_id, "status": "submitted"});
    if status == "submitted" && previous.as_deref() == Some(serialized.as_str()) {
        return Ok(receipt);
    }
    if status != "pending" {
        return Err(ApiError(
            StatusCode::CONFLICT,
            format!("tool call is {status}; result cannot be submitted"),
        ));
    }
    sqlx::query("UPDATE tool_calls SET status = 'submitting', result = ? WHERE session_id = ? AND turn_id = ? AND call_id = ?")
        .bind(serialized).bind(session_id).bind(turn_id).bind(&result.call_id).execute(&state.store.0).await.map_err(anyhow::Error::from)?;
    let request_id = serde_json::from_str(&request_id).map_err(anyhow::Error::from)?;
    let delivered = client
        .resolve_server_request(
            request_id,
            json!({"success": result.success, "contentItems": content_items(&result.output)}),
        )
        .await;
    let status = if delivered.is_ok() {
        "submitted"
    } else {
        "unavailable"
    };
    sqlx::query(
        "UPDATE tool_calls SET status = ? WHERE session_id = ? AND turn_id = ? AND call_id = ?",
    )
    .bind(status)
    .bind(session_id)
    .bind(turn_id)
    .bind(&result.call_id)
    .execute(&state.store.0)
    .await
    .map_err(anyhow::Error::from)?;
    delivered.map_err(|_| {
        ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "tool result delivery unknown".into(),
        )
    })?;
    let public: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM public_sessions WHERE id = ?)")
            .bind(session_id)
            .fetch_one(&state.store.0)
            .await
            .map_err(anyhow::Error::from)?;
    if public
        && state
            .store
            .session(session_id)
            .await?
            .is_some_and(|s| s.required_actions.is_empty())
    {
        let turn: Option<String> = sqlx::query_scalar("UPDATE public_records SET data = json_set(data, '$.status', 'in_progress') WHERE session_id = ? AND kind = 'turn' AND id = ? AND json_extract(data, '$.status') = 'waiting' RETURNING data")
            .bind(session_id).bind(turn_id).fetch_optional(&state.store.0).await.map_err(anyhow::Error::from)?;
        crate::records::session_status(state, session_id, "in_progress", /*error*/ None).await?;
        if let Some(turn) = turn {
            let turn: Value = serde_json::from_str(&turn).map_err(anyhow::Error::from)?;
            crate::records::emit(
                state,
                json!({"type":"agent.session.turn.in_progress","session_id":session_id,"turn_id":turn_id,"turn":turn}),
            );
        }
    }
    Ok(receipt)
}

pub(crate) async fn notification(state: &State, value: &Value) -> anyhow::Result<()> {
    if value["method"] == "serverRequest/resolved" {
        sqlx::query("UPDATE tool_calls SET status = 'cancelled' WHERE status = 'pending' AND request_id = ? AND session_id IN (SELECT id FROM sessions WHERE thread_id = ?)")
            .bind(serde_json::to_string(&value["params"]["requestId"])?)
            .bind(value["params"]["threadId"].as_str()).execute(&state.store.0).await?;
    } else if value["method"] == "turn/completed" {
        sqlx::query("UPDATE tool_calls SET status = 'cancelled' WHERE status = 'pending' AND turn_id = ? AND session_id IN (SELECT id FROM sessions WHERE thread_id = ?)")
            .bind(value["params"]["turn"]["id"].as_str())
            .bind(value["params"]["threadId"].as_str()).execute(&state.store.0).await?;
    }
    Ok(())
}
