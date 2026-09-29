//! The text/function subset of the Agents API, pinned to openai-python 3.17.0.
use crate::ApiError;
use crate::State;
use crate::resources::Agent;
use crate::resources::AgentConfig;
use crate::resources::Environment;
use crate::resources::SessionCreateParams;
use axum::Json;
use axum::Router;
use axum::extract::Path;
use axum::extract::State as Extract;
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
        .route(
            "/v1/agents/sessions",
            post(create).get(crate::sessions::list),
        )
        .route(
            "/v1/agents/sessions/{id}",
            get(read)
                .post(crate::sessions::update)
                .delete(crate::sessions::delete),
        )
        .route(
            "/v1/agents/sessions/{id}/events",
            get(stream).post(crate::input::create),
        )
        .route("/v1/agents/sessions/{id}/items", get(crate::records::items))
        .route("/v1/agents/sessions/{id}/turns", get(crate::records::turns))
        .route("/v1/agents/sessions/{id}/traces", get(crate::traces::list))
        .route(
            "/v1/agents/sessions/{id}/turns/{turn_id}",
            get(crate::records::turn),
        )
        .route(
            "/v1/agents/sessions/{id}/subagents",
            get(crate::subagents::list),
        )
        .route(
            "/v1/agents/sessions/{id}/subagents/{subagent_id}",
            get(crate::subagents::retrieve),
        )
        .route(
            "/v1/agents/sessions/{id}/subagents/{subagent_id}/items",
            get(crate::subagents::items),
        )
        .route(
            "/v1/agents/sessions/{id}/subagents/{subagent_id}/turns",
            get(crate::subagents::turns),
        )
        .route(
            "/v1/agents/sessions/{id}/subagents/{subagent_id}/turns/{turn_id}",
            get(crate::subagents::turn),
        )
        .route(
            "/v1/agents/sessions/{id}/subagents/{subagent_id}/turns/{turn_id}/items",
            get(crate::subagents::turn_items),
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
    environment: Value,
    agent_id: Option<String>,
    agent: Option<Value>,
    input: Option<Value>,
    #[serde(default)]
    stream: bool,
    #[serde(default)]
    metadata: Value,
    vault_ids: Option<Vec<String>>,
}

async fn create(
    Extract(state): Extract<Arc<State>>,
    Json(params): Json<Create>,
) -> Result<Response, ApiError> {
    // Checked here rather than by deserialization, so errors name the public
    // environment types instead of the prototype's.
    match params.environment["type"].as_str() {
        Some("none")
            if params
                .environment
                .as_object()
                .is_some_and(|fields| fields.len() == 1) => {}
        Some("none") => return Err(invalid("environment none takes no other fields")),
        Some(kind @ ("openai_hosted" | "self_hosted")) => {
            return Err(invalid(format!(
                "environment type {kind} is not implemented; use none"
            )));
        }
        _ => {
            return Err(invalid(
                "environment.type must be none, openai_hosted, or self_hosted",
            ));
        }
    }
    // The pinned SDK requires initial input for environment `none`.
    let input = match params.input {
        None | Some(Value::Null) => None,
        Some(Value::Array(items)) if items.is_empty() => None,
        Some(input) => Some(crate::input::message(input)?),
    }
    .ok_or_else(|| invalid("input is required when environment.type is none"))?;
    let metadata = crate::configuration::metadata(params.metadata)?;
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
    // Resolve worker configuration now so an unreachable MCP server or an
    // unsupported tier is rejected before a session is created.
    crate::capabilities::overrides(
        &state,
        &saved.config,
        &Environment::None,
        &Default::default(),
    )
    .await?;
    let vault_ids = crate::credentials::vaults(&state, params.vault_ids).await?;
    let credentials = crate::credentials::resolve(&state, &saved.config, &vault_ids).await?;
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
        "vault_ids":vault_ids,"required_actions":[],"status":"in_progress","error":null,"usage":null});
    crate::records::create_session(&state.store.0, &public).await?;
    if let Err(error) = crate::credentials::snapshot(&state, &session.id, credentials).await {
        crate::records::session_status(&state, &session.id, "failed", Some(&error.1)).await?;
        return Err(error);
    }
    let receiver = state.public_events.subscribe();
    crate::records::emit(
        &state,
        json!({"type":"agent.session.created","session":public}),
    );
    if let Err(error) = crate::routes::start_turn(&state, &session.id, input).await {
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

#[cfg(test)]
#[path = "contract_tests.rs"]
mod tests;

fn sse(receiver: tokio::sync::broadcast::Receiver<Value>, id: String) -> Response {
    // Streams are live only. End on lag, backend loss, or deletion of this
    // session so clients recover via saved state.
    let owner = id.clone();
    let stream = BroadcastStream::new(receiver)
        .take_while(move |v| {
            futures::future::ready(matches!(v, Ok(v) if v["type"] != "disconnect"
                && !(v["type"] == crate::sessions::DELETED && v["session_id"] == owner)))
        })
        .filter_map(move |v| {
            let event = v
                .ok()
                .filter(|v| {
                    v["session_id"] == id
                        || v["session"]["id"] == id
                        || v["subagent"]["session_id"] == id
                })
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
