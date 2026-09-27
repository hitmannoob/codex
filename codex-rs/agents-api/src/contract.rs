//! The text/function subset of the Agents API, pinned to openai-python 3.17.0.
use crate::ApiError;
use crate::State;
use crate::resources::Agent;
use crate::resources::AgentConfig;
use crate::resources::Environment;
use crate::resources::InputParams;
use crate::resources::SessionCreateParams;
use crate::resources::ToolResult;
use axum::Json;
use axum::Router;
use axum::extract::Path;
use axum::extract::State as Extract;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::response::Sse;
use axum::response::sse::Event;
use axum::response::sse::KeepAlive;
use axum::routing::get;
use axum::routing::post;
use futures::StreamExt;
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;
use std::convert::Infallible;
use std::sync::Arc;
use tokio_stream::wrappers::BroadcastStream;
use uuid::Uuid;

pub(crate) fn router() -> Router<Arc<State>> {
    Router::new()
        .route("/v1/agents/sessions", post(create))
        .route("/v1/agents/sessions/{id}", get(read))
        .route("/v1/agents/sessions/{id}/events", get(stream).post(input))
        .route("/v1/agents/sessions/{id}/items", get(crate::records::items))
        .route("/v1/agents/sessions/{id}/turns", get(crate::records::turns))
        .route(
            "/v1/agents/sessions/{id}/turns/{turn_id}",
            get(crate::records::turn),
        )
}

pub(crate) fn invalid(message: impl Into<String>) -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, message.into())
}

pub(crate) fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub(crate) use crate::configuration::agent;
pub(crate) use crate::configuration::configure;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Create {
    environment: Environment,
    agent_id: Option<String>,
    agent: Option<Value>,
    input: Value,
    #[serde(default)]
    stream: bool,
    metadata: Option<std::collections::BTreeMap<String, String>>,
    vault_ids: Option<Vec<String>>,
}

// Preserve a single user-message boundary; reject unsupported batches explicitly.
fn text(input: Value) -> Result<String, ApiError> {
    let input = if let Some(text) = input.as_str() {
        text.to_owned()
    } else {
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
        }
        let messages: Vec<Message> =
            serde_json::from_value(input).map_err(|e| invalid(e.to_string()))?;
        if messages.len() != 1 {
            return Err(invalid("exactly one user message is currently supported"));
        }
        let message = messages
            .into_iter()
            .next()
            .ok_or_else(|| invalid("input required"))?;
        if message.role != "user" || message.kind.is_some_and(|kind| kind != "message") {
            return Err(invalid("input must be a user message"));
        }
        message
            .content
            .into_iter()
            .map(|Content::Text { text }| text)
            .collect::<Vec<_>>()
            .join("")
    };
    if input.trim().is_empty() || input.len() > 8192 {
        return Err(invalid("input must be 1-8192 bytes"));
    }
    Ok(input)
}

async fn create(
    Extract(state): Extract<Arc<State>>,
    Json(params): Json<Create>,
) -> Result<Response, ApiError> {
    if !matches!(params.environment, Environment::None) {
        return Err(invalid(
            "only environment none is implemented on this API path",
        ));
    }
    if params.vault_ids.is_some_and(|ids| !ids.is_empty()) {
        return Err(invalid("vaults are not implemented"));
    }
    let input = text(params.input)?;
    let metadata = params.metadata.unwrap_or_default();
    if metadata.len() > 16
        || metadata
            .iter()
            .any(|(key, value)| key.chars().count() > 64 || value.chars().count() > 512)
    {
        return Err(invalid("metadata exceeds documented limits"));
    }
    let mut saved = match params.agent_id {
        Some(id) => state
            .store
            .agent(&id)
            .await?
            .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "agent not found".into()))?,
        None => Agent {
            id: Uuid::new_v4().to_string(),
            config: AgentConfig::default(),
            created_at: now(),
            updated_at: now(),
            name: None,
            metadata: Default::default(),
        },
    };
    let mut patch = params.agent.unwrap_or_else(|| json!({}));
    if let Some(object) = patch.as_object_mut()
        && let Some(name) = object.remove("name")
    {
        saved.name = serde_json::from_value(name).map_err(|e| invalid(e.to_string()))?;
    }
    saved.config = configure(saved.config, patch)?;
    crate::configuration::validate_execution(&saved.config)?;
    let session = state
        .store
        .create_session(
            saved,
            SessionCreateParams {
                agent_id: String::new(),
                environment: Environment::None,
            },
        )
        .await?;
    let public = json!({"id":session.id,"object":"agent.session","agent":agent(&session.agent),
        "created_at":now(),"last_active_at":now(),"environment":{"type":"none"},"metadata":metadata,
        "vault_ids":[],"required_actions":[],"status":"in_progress","error":null,"usage":null});
    crate::records::save_session(&state.store.0, &session.id, &public).await?;
    let receiver = state.public_events.subscribe();
    crate::records::emit(
        &state,
        json!({"type":"agent.session.created","session":public}),
    );
    if let Err(error) = crate::routes::input(
        Extract(Arc::clone(&state)),
        Path(session.id.clone()),
        Json(InputParams { input }),
    )
    .await
    {
        crate::records::session_status(&state, &session.id, "failed", Some(&error.1)).await?;
        return Err(error);
    }
    if params.stream {
        Ok(sse(receiver, session.id))
    } else {
        Ok(Json(crate::records::session(&state, &session.id).await?).into_response())
    }
}

async fn read(
    Extract(state): Extract<Arc<State>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(crate::records::session(&state, &id).await?))
}

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
        output: Option<String>,
        error: Option<String>,
    },
}

async fn input(
    Extract(state): Extract<Arc<State>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(params): Json<Inputs>,
) -> Result<StatusCode, ApiError> {
    if headers.contains_key("idempotency-key") {
        return Err(invalid("event idempotency keys are not implemented"));
    }
    if params.events.len() != 1 {
        return Err(invalid("exactly one input event is currently supported"));
    }
    crate::records::session(&state, &id).await?;
    match params
        .events
        .into_iter()
        .next()
        .ok_or_else(|| invalid("event required"))?
    {
        Input::Message { input } => {
            let _ = crate::routes::input(
                Extract(state),
                Path(id),
                Json(InputParams {
                    input: text(input)?,
                }),
            )
            .await?;
        }
        Input::Cancel => {
            if let Some(turn) = crate::records::active_turn(&state, &id).await? {
                let _ = crate::routes::cancel(Extract(state), Path((id, turn))).await?;
            }
        }
        Input::ToolResult {
            call_id,
            turn_id,
            success,
            output,
            error,
        } => {
            if success && error.is_some() {
                return Err(invalid("a successful tool result cannot contain an error"));
            }
            if output.is_some() && error.is_some() {
                return Err(invalid(
                    "combined function output and error are not implemented",
                ));
            }
            let _ = crate::actions::submit(
                Extract(state),
                Path((id, turn_id)),
                Json(ToolResult {
                    call_id,
                    success,
                    output: json!(error.or(output).unwrap_or_default()),
                }),
            )
            .await?;
        }
    }
    Ok(StatusCode::ACCEPTED)
}

async fn stream(
    Extract(state): Extract<Arc<State>>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let receiver = state.public_events.subscribe();
    crate::records::session(&state, &id).await?;
    if !state.connected() {
        return Err(crate::disconnected_error());
    }
    Ok(sse(receiver, id))
}

fn sse(receiver: tokio::sync::broadcast::Receiver<Value>, id: String) -> Response {
    // Streams are live only. End on lag/disconnect so clients recover via saved state.
    let stream = BroadcastStream::new(receiver)
        .take_while(|v| futures::future::ready(matches!(v, Ok(v) if v["type"] != "disconnect")))
        .filter_map(move |v| {
            let event = v
                .ok()
                .filter(|v| v["session_id"] == id || v["session"]["id"] == id)
                .map(|v| {
                    Ok::<_, Infallible>(
                        Event::default()
                            .event(v["type"].as_str().unwrap_or_default())
                            .data(v.to_string()),
                    )
                });
            futures::future::ready(event)
        });
    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}
