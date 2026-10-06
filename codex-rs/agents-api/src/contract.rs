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
        .route(
            "/v1/agents/sessions/{id}/artifacts",
            get(crate::artifacts::list),
        )
        .route(
            "/v1/agents/sessions/{id}/artifacts/{artifact_id}",
            get(crate::artifacts::missing).delete(crate::artifacts::missing),
        )
        .route(
            "/v1/agents/sessions/{id}/artifacts/{artifact_id}/content",
            get(crate::artifacts::missing),
        )
        .route(
            "/v1/agents/environments/{id}",
            get(crate::environments::retrieve),
        )
        .route(
            "/v1/agents/environments/{id}/files",
            post(crate::environment_files::create)
                .get(crate::environment_files::list)
                // Base64 inflates an inline file by a third.
                .layer(axum::extract::DefaultBodyLimit::max(
                    crate::environment_files::MAX_INLINE_BYTES / 3 * 4 + 64 * 1024,
                )),
        )
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
    headers: HeaderMap,
    Json(params): Json<Create>,
) -> Result<Response, ApiError> {
    // Checked here rather than by deserialization, so errors name the public
    // environment types instead of the prototype's.
    let mut environment = Environment::None;
    match params.environment["type"].as_str() {
        Some("none")
            if params
                .environment
                .as_object()
                .is_some_and(|fields| fields.len() == 1) => {}
        Some("none") => return Err(invalid("environment none takes no other fields")),
        Some("openai_hosted") => {
            return Err(invalid(
                "environment type openai_hosted is not supported by this service; use none",
            ));
        }
        Some("self_hosted") => {
            environment = crate::environments::parse(&params.environment)?;
            if state.registry.harness_url().is_none() {
                return Err(ApiError(
                    StatusCode::NOT_IMPLEMENTED,
                    "self-hosted environments are disabled; the operator must configure an environment key".into(),
                ));
            }
        }
        _ => {
            return Err(invalid(
                "environment.type must be none, openai_hosted, or self_hosted",
            ));
        }
    }
    // The pinned SDK requires initial input for environment `none`; a
    // self-hosted session may start idle.
    let input = match params.input {
        None | Some(Value::Null) => None,
        Some(Value::Array(items)) if items.is_empty() => None,
        Some(input) => Some(crate::input::message(input)?),
    };
    let self_hosted = matches!(environment, Environment::SelfHosted { .. });
    if input.is_none() && !self_hosted {
        return Err(invalid("input is required when environment.type is none"));
    }
    let metadata = crate::configuration::metadata(params.metadata)?;
    let saved_agent = params.agent_id.clone();
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
    // Supplied tools replace the saved agent's, and their env values with them.
    let mut stdio_env = match (crate::stdio_env::take(&mut patch)?, &saved_agent) {
        (Some(values), _) => values,
        (None, Some(agent_id)) => {
            crate::stdio_env::load(&state, crate::stdio_env::agent_name(agent_id)).await?
        }
        (None, None) => Default::default(),
    };
    saved.config = configure(saved.config, patch)?;
    stdio_env.retain(|label, _| {
        saved.config.tools.iter().any(|tool| {
            matches!(tool, crate::agent_tools::Tool::Capability(crate::agent_tools::CapabilityTool::Mcp {
                server_label, transport: crate::agent_tools::McpTransport::Stdio { .. }, ..
            }) if server_label == label)
        })
    });
    if !stdio_env.is_empty() && !state.secrets.configured() {
        return Err(ApiError(
            StatusCode::NOT_IMPLEMENTED,
            "stdio MCP env values require the operator to configure a vault passphrase; use env_vars to inherit them from the executor".into(),
        ));
    }
    crate::configuration::validate_execution(&saved.config, &environment)?;
    // Resolve worker configuration now so an unreachable MCP server or an
    // unsupported tier is rejected before a session is created.
    crate::capabilities::overrides(
        &state,
        &saved.config,
        &environment,
        &Default::default(),
        &stdio_env,
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
                environment: environment.clone(),
            },
        )
        .await?;
    let public_environment = match &environment {
        Environment::SelfHosted {
            id,
            cwd,
            capability_directories,
        } => {
            let remote_url = crate::registry::remote_url(&headers);
            crate::environments::create(
                &state,
                &session.id,
                id,
                cwd,
                capability_directories,
                remote_url,
            )
            .await?
        }
        Environment::None | Environment::Local { .. } => json!({"type":"none"}),
    };
    // Input to a self-hosted session sets the status once it is submitted.
    let status = if self_hosted { "idle" } else { "in_progress" };
    let public = json!({"id":session.id,"object":"agent.session","agent":agent(&session.agent),
        "created_at":now(),"last_active_at":now(),"environment":public_environment,"metadata":metadata,
        "vault_ids":vault_ids,"required_actions":[],"status":status,"error":null,"usage":null});
    crate::records::create_session(&state.store.0, &public).await?;
    let snapshot = async {
        crate::credentials::snapshot(&state, &session.id, credentials).await?;
        let name = crate::stdio_env::session_name(&session.id);
        crate::stdio_env::save(&state, name, &stdio_env).await
    };
    if let Err(error) = snapshot.await {
        crate::records::session_status(&state, &session.id, "failed", Some(&error.1)).await?;
        return Err(error);
    }
    let receiver = state.public_events.subscribe();
    crate::records::emit(
        &state,
        json!({"type":"agent.session.created","session":public}),
    );
    if let Environment::SelfHosted { id, .. } = &environment {
        crate::environments::emit(&state, &session.id, id, "pending", /*error*/ None);
        crate::environments::watch(&state, session.id.clone(), id.clone());
    }
    if let Some(input) = input
        && let Err(error) = crate::environments::submit(&state, &session.id, input).await
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
