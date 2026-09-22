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

pub(crate) fn agent(agent: &Agent) -> Value {
    json!({"id":agent.id,"model":agent.config.model,"instructions":agent.config.instructions,
        "name":null,"multi_agent":{"enabled":false,"max_concurrent_subagents":0},
        "reasoning":{"effort":agent.config.reasoning.as_ref().map(|r| &r.effort),"summary":null},
        "service_tier":"auto","text":{"format":{"type":"text"},"verbosity":"medium"},
        "tools":agent.config.tools.iter().map(|t| json!({"type":"function","name":t.name,"description":t.description,"parameters":t.parameters,"defer_loading":false})).collect::<Vec<_>>()})
}

pub(crate) fn configure(mut config: AgentConfig, patch: Value) -> Result<AgentConfig, ApiError> {
    let fields = patch
        .as_object()
        .ok_or_else(|| invalid("agent must be an object"))?;
    for (key, value) in fields {
        match key.as_str() {
            "model" => {
                config.model = value
                    .as_str()
                    .ok_or_else(|| invalid("model must be a string"))?
                    .into()
            }
            "instructions" => {
                config.instructions = if value.is_null() {
                    String::new()
                } else {
                    value
                        .as_str()
                        .ok_or_else(|| invalid("instructions must be a string or null"))?
                        .into()
                }
            }
            "tools" => {
                config.tools.clear();
                if !value.is_null() {
                    for tool in value
                        .as_array()
                        .ok_or_else(|| invalid("tools must be an array or null"))?
                    {
                        let mut tool = tool.clone();
                        if tool["type"] != "function" {
                            return Err(invalid(
                                "only function tools are implemented on this API path",
                            ));
                        }
                        tool.as_object_mut()
                            .ok_or_else(|| invalid("invalid function"))?
                            .remove("type");
                        if tool.get("defer_loading") == Some(&json!(false)) {
                            tool.as_object_mut()
                                .ok_or_else(|| invalid("invalid function"))?
                                .remove("defer_loading");
                        }
                        config.tools.push(
                            serde_json::from_value(tool).map_err(|e| invalid(e.to_string()))?,
                        );
                    }
                }
            }
            "reasoning" => {
                config.reasoning = if value.is_null() {
                    None
                } else {
                    let reasoning: crate::resources::Reasoning =
                        serde_json::from_value(value.clone())
                            .map_err(|e| invalid(e.to_string()))?;
                    if !matches!(
                        reasoning.effort.as_str(),
                        "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max"
                    ) {
                        return Err(invalid("unsupported reasoning effort"));
                    }
                    Some(reasoning)
                }
            }
            _ => return Err(invalid(format!("agent field {key} is not implemented"))),
        }
    }
    if config.model.trim().is_empty()
        || config.model.len() > 256
        || config.instructions.len() > 1024
    {
        return Err(invalid(
            "model must be 1-256 bytes; instructions must be at most 1024 bytes",
        ));
    }
    if !config.mcp_servers.is_empty() {
        return Err(invalid(
            "saved MCP configurations are not supported on this API path",
        ));
    }
    if config.reasoning.as_ref().is_some_and(|r| {
        !matches!(
            r.effort.as_str(),
            "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max"
        )
    }) {
        return Err(invalid(
            "saved reasoning effort is not supported on this API path",
        ));
    }
    crate::capabilities::validate(&config)?;
    Ok(config)
}

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
        },
    };
    saved.config = configure(saved.config, params.agent.unwrap_or_else(|| json!({})))?;
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
    crate::records::save_session(&state, &session.id, &public).await?;
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
    if !state.connected.load(std::sync::atomic::Ordering::Acquire) {
        return Err(ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "app-server disconnected".into(),
        ));
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
