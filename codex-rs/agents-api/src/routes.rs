use crate::ApiError;
use crate::State;
use crate::resources::Agent;
use crate::resources::AgentConfig;
use crate::resources::Environment;
use crate::resources::InputParams;
use crate::resources::PageParams;
use crate::resources::Session;
use crate::resources::SessionCreateParams;
use axum::Json;
use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::Request;
use axum::extract::State as Extract;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::middleware;
use axum::middleware::Next;
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

pub(crate) fn router(state: Arc<State>) -> Router {
    Router::new()
        .route("/v1/agents", post(create_agent).get(list_agents))
        .route(
            "/v1/agents/{id}",
            get(read_agent).post(update_agent).delete(delete_agent),
        )
        .route("/v1/sessions", post(create_session))
        .route("/v1/sessions/{id}", get(read_session))
        .route("/v1/sessions/{id}/input", post(input))
        .route("/v1/sessions/{id}/turns", get(turns))
        .route("/v1/sessions/{id}/turns/{turn_id}/cancel", post(cancel))
        .route(
            "/v1/sessions/{id}/turns/{turn_id}/tool-results",
            post(crate::actions::submit),
        )
        .route("/v1/sessions/{id}/events", get(events))
        .merge(crate::contract::router())
        .merge(crate::vaults::router())
        .merge(crate::webhooks::router())
        .merge(crate::registry::router())
        // Route layers see the matched route template, which labels metrics.
        .route_layer(middleware::from_fn(crate::telemetry::route))
        .layer(DefaultBodyLimit::max(/*limit*/ 16 * 1024))
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            authorize,
        ))
        // Outermost, so rebuilt error responses and rejected credentials also
        // carry a request ID.
        .layer(middleware::from_fn(crate::telemetry::observe))
        .with_state(state)
}

async fn authorize(
    Extract(state): Extract<Arc<State>>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    // Executors and the worker's harness present their own credentials, which
    // the registry checks; the API token never reaches them.
    if request.uri().path().starts_with("/registry/") {
        return Ok(next.run(request).await);
    }
    // Webhook endpoint requests come from the SDK without the beta header.
    let compatible = request.uri().path().starts_with("/v1/agents/sessions")
        || request.uri().path().starts_with("/v1/webhook_")
        || request
            .headers()
            .get("openai-beta")
            .and_then(|h| h.to_str().ok())
            == Some("agents=v1");
    let expected = format!("Bearer {}", state.token);
    if request
        .headers()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        != Some(expected.as_str())
    {
        let error = ApiError(StatusCode::UNAUTHORIZED, "bearer token required".into());
        return if compatible {
            Ok(error.public_response())
        } else {
            Err(error)
        };
    }
    let response = next.run(request).await;
    if compatible && (response.status().is_client_error() || response.status().is_server_error()) {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), /*limit*/ 16384)
            .await
            .map_err(anyhow::Error::from)?;
        let message = serde_json::from_slice::<Value>(&bytes)
            .ok()
            .and_then(|v| v["error"].as_str().map(str::to_owned))
            .unwrap_or_else(|| String::from_utf8_lossy(&bytes).into_owned());
        let status = if status == StatusCode::UNPROCESSABLE_ENTITY {
            StatusCode::BAD_REQUEST
        } else {
            status
        };
        return Ok(ApiError(status, message).public_response());
    }
    Ok(response)
}

async fn create_agent(
    Extract(state): Extract<Arc<State>>,
    headers: HeaderMap,
    Json(value): Json<Value>,
) -> Result<Response, ApiError> {
    let compatible = headers.get("openai-beta").and_then(|h| h.to_str().ok()) == Some("agents=v1");
    let mut value = value;
    let (name, metadata) = if compatible {
        take_agent_details(&mut value, None, Default::default())?
    } else {
        (None, Default::default())
    };
    let config: AgentConfig = if compatible {
        crate::contract::configure(AgentConfig::default(), value)?
    } else {
        serde_json::from_value(value).map_err(|e| crate::contract::invalid(e.to_string()))?
    };
    if config.model.trim().is_empty()
        || config.model.len() > 256
        || config.instructions.as_deref().unwrap_or_default().len() > 1024
    {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "model must be 1-256 bytes; instructions must be at most 1024 bytes".into(),
        ));
    }
    crate::capabilities::validate(&config)?;
    let agent = state.store.create_agent(config, name, metadata).await?;
    Ok(if compatible {
        Json(public_agent(&agent)).into_response()
    } else {
        (StatusCode::CREATED, Json(agent)).into_response()
    })
}

async fn read_agent(
    Extract(state): Extract<Arc<State>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let agent = state
        .store
        .agent(&id)
        .await?
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "agent not found".into()))?;
    Ok(Json(
        if headers.get("openai-beta").and_then(|h| h.to_str().ok()) == Some("agents=v1") {
            public_agent(&agent)
        } else {
            serde_json::to_value(agent).map_err(anyhow::Error::from)?
        },
    ))
}

fn public_agent(agent: &Agent) -> Value {
    let mut value = crate::contract::agent(agent);
    value["object"] = json!("agent");
    value["created_at"] = json!(agent.created_at);
    value["updated_at"] = json!(if agent.updated_at == 0 {
        agent.created_at
    } else {
        agent.updated_at
    });
    value["metadata"] = json!(agent.metadata);
    value["name"] = json!(agent.name);
    value
}

fn take_agent_details(
    value: &mut Value,
    previous_name: Option<String>,
    previous_metadata: std::collections::BTreeMap<String, String>,
) -> Result<(Option<String>, std::collections::BTreeMap<String, String>), ApiError> {
    let object = value
        .as_object_mut()
        .ok_or_else(|| crate::contract::invalid("agent must be an object"))?;
    let name = match object.remove("name") {
        None => previous_name,
        Some(Value::Null) => None,
        Some(Value::String(name)) => Some(name),
        Some(_) => return Err(crate::contract::invalid("name must be a string or null")),
    };
    let metadata = match object.remove("metadata") {
        None => previous_metadata,
        Some(value) => crate::configuration::metadata(value)?,
    };
    Ok((name, metadata))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentListParams {
    after: Option<String>,
    limit: Option<u32>,
    order: Option<String>,
}

async fn list_agents(
    Extract(state): Extract<Arc<State>>,
    Query(params): Query<AgentListParams>,
) -> Result<Json<Value>, ApiError> {
    let limit = params.limit.unwrap_or(20);
    if !(1..=100).contains(&limit) {
        return Err(crate::contract::invalid("limit must be between 1 and 100"));
    }
    let order = params.order.as_deref().unwrap_or("desc");
    if !matches!(order, "asc" | "desc") {
        return Err(crate::contract::invalid("order must be asc or desc"));
    }
    let (agents, has_more) = state
        .store
        .list_agents(params.after.as_deref(), order, i64::from(limit))
        .await?
        .ok_or_else(|| crate::contract::invalid("invalid agent cursor"))?;
    Ok(Json(json!({
        "object":"list",
        "first_id":agents.first().map(|agent| &agent.id),
        "last_id":agents.last().map(|agent| &agent.id),
        "data":agents.iter().map(public_agent).collect::<Vec<_>>(),
        "has_more":has_more,
    })))
}

async fn update_agent(
    Extract(state): Extract<Arc<State>>,
    Path(id): Path<String>,
    Json(mut patch): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let previous = state
        .store
        .agent(&id)
        .await?
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "agent not found".into()))?;
    let (name, metadata) =
        take_agent_details(&mut patch, previous.name.clone(), previous.metadata.clone())?;
    let config = crate::contract::configure(previous.config.clone(), patch)?;
    let mut updated = previous.clone();
    updated.config = config;
    updated.name = name;
    updated.metadata = metadata;
    updated.updated_at = crate::contract::now();
    if !state.store.update_agent(&previous, &updated).await? {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "agent changed during update".into(),
        ));
    }
    Ok(Json(public_agent(&updated)))
}

async fn delete_agent(
    Extract(state): Extract<Arc<State>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    if !state.store.delete_agent(&id).await? {
        return Err(ApiError(StatusCode::NOT_FOUND, "agent not found".into()));
    }
    Ok(Json(
        json!({"id":id,"object":"agent.deleted","deleted":true}),
    ))
}

async fn create_session(
    Extract(state): Extract<Arc<State>>,
    Json(params): Json<SessionCreateParams>,
) -> Result<(StatusCode, Json<Session>), ApiError> {
    let agent = state
        .store
        .agent(&params.agent_id)
        .await?
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "agent not found".into()))?;
    Ok((
        StatusCode::CREATED,
        Json(state.store.create_session(agent, params).await?),
    ))
}

async fn read_session(
    Extract(state): Extract<Arc<State>>,
    Path(id): Path<String>,
) -> Result<Json<Session>, ApiError> {
    state
        .store
        .session(&id)
        .await?
        .map(Json)
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "session not found".into()))
}

async fn input(
    Extract(state): Extract<Arc<State>>,
    Path(id): Path<String>,
    Json(params): Json<InputParams>,
) -> Result<Json<Value>, ApiError> {
    if params.input.trim().is_empty() || params.input.len() > 8192 {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "input must be 1-8192 bytes".into(),
        ));
    }
    Ok(Json(
        start_turn(
            &state,
            &id,
            vec![json!({"type": "text", "text": params.input})],
        )
        .await?,
    ))
}

/// Start a turn with Codex user input items, or steer the active turn.
pub(crate) async fn start_turn(
    state: &Arc<State>,
    id: &str,
    items: Vec<Value>,
) -> Result<Value, ApiError> {
    // Serialize this session's bootstrap and submission, not the model/tool
    // execution that follows; other sessions are admitted concurrently.
    let _admission = state.input_gates.lock(id).await;
    let Json(session) = read_session(Extract(Arc::clone(state)), Path(id.to_owned())).await?;
    crate::configuration::validate_execution(&session.agent.config, &session.environment)?;
    let tokens = crate::credentials::load(state, id).await?;
    let config =
        crate::capabilities::overrides(state, &session.agent.config, &session.environment, &tokens)
            .await?;
    let loaded = state.loaded_threads()?;
    if let Environment::SelfHosted { id, .. } = &session.environment {
        crate::environments::attach(state, id).await?;
    }
    // The caller's own compute is the boundary for a self-hosted environment,
    // which runs commands unsandboxed. Resuming does not keep the thread's
    // sandbox, so both start and resume name it.
    let sandbox = match &session.environment {
        Environment::None | Environment::Local { .. } => "read-only",
        Environment::SelfHosted { .. } => "danger-full-access",
    };
    let thread = async {
        Ok::<_, ApiError>(if let Some(thread_id) = session.thread_id {
        // Resume once per connection. Per-turn settings travel with
        // `turn/start`, and resuming a just-started thread can race the first
        // write of its rollout.
        if !crate::lock(&loaded).contains(&thread_id) {
            state
                .rpc(
                    "thread/resume",
                    json!({"threadId": thread_id, "excludeTurns": true, "config": config,
                        "approvalPolicy": "never", "sandbox": sandbox,
                        "serviceTier": null,
                        "model": session.agent.config.model, "developerInstructions": session.agent.config.instructions.as_deref().unwrap_or_default()}),
                )
                .await?;
            crate::lock(&loaded).insert(thread_id.clone());
        }
        thread_id
    } else {
        let environments = match &session.environment {
            Environment::None => json!([]),
            Environment::Local { cwd } => json!([{"environmentId": "local", "cwd": cwd}]),
            Environment::SelfHosted { id, cwd, .. } => json!([{"environmentId": id, "cwd": cwd}]),
        };
        // Skills in the caller's capability directories, found on the executor.
        // Codex keeps the selection when the thread resumes.
        let capability_roots = match &session.environment {
            Environment::SelfHosted { id, capability_directories, .. } => capability_directories.iter().enumerate()
                .map(|(index, path)| json!({"id": format!("capability-{index}"),
                    "location": {"type": "environment", "environmentId": id, "path": path}}))
                .collect(),
            Environment::None | Environment::Local { .. } => Vec::new(),
        };
        let response = state
            .rpc(
                "thread/start",
                json!({
                    "model": session.agent.config.model,
                    "developerInstructions": session.agent.config.instructions.as_deref().unwrap_or_default(),
                    "serviceTier": null,
                    "environments": environments,
                    "selectedCapabilityRoots": capability_roots,
                    "config": config,
                    "dynamicTools": session.agent.config.tools.iter().filter_map(crate::agent_tools::Tool::function).map(|tool| json!({
                        "type": "function", "name": tool.name, "description": tool.description, "inputSchema": tool.parameters
                    })).collect::<Vec<_>>(),
                    "approvalPolicy": "never", "sandbox": sandbox, "ephemeral": false,
                }),
            )
            .await?;
        let thread_id = response
            .pointer("/thread/id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("thread/start omitted thread id"))?
            .to_owned();
        // An explicit name materializes the thread before its ID is committed here.
        state
            .rpc(
                "thread/name/set",
                json!({"threadId": thread_id, "name": format!("API session {id}")}),
            )
            .await?;
        sqlx::query("UPDATE sessions SET thread_id = ? WHERE id = ?")
            .bind(&thread_id)
            .bind(id)
            .execute(&state.store.0)
            .await
            .map_err(anyhow::Error::from)?;
        crate::lock(&loaded).insert(thread_id.clone());
        thread_id
    })
    }
    .await;
    let thread_id = match thread {
        Ok(thread_id) => thread_id,
        // Codex will not start or resume a thread whose required MCP server
        // cannot initialize; the contract reports that as a failed turn.
        Err(error)
            if error.0 == StatusCode::BAD_GATEWAY
                && error
                    .1
                    .contains("required MCP servers failed to initialize") =>
        {
            let failure = json!({"code": "connection_failed", "message": "a required MCP server failed to initialize"});
            if crate::turns::fail_unstarted_turn(state, id, failure).await? {
                return Ok(json!({}));
            }
            return Err(error);
        }
        Err(error) => return Err(error),
    };
    state
            .rpc(
                "turn/start",
                json!({"threadId": thread_id, "input": items,
                    // `thread/resume` does not restore a thread's environment
                    // selection, so a self-hosted session names it every turn.
                    "environments": match &session.environment {
                        Environment::SelfHosted { id, cwd, .. } => Some(json!([{"environmentId": id, "cwd": cwd}])),
                        Environment::None | Environment::Local { .. } => None,
                    },
                    // A complete settings value clears inherited effort; effort:null alone is a no-op.
                    "collaborationMode":{"mode":"default","settings":{
                        "model":session.agent.config.model,
                        "reasoning_effort":session.agent.config.reasoning.as_ref().and_then(|r| r.effort.as_ref()),
                        "developer_instructions":""}},
                    "summary": session.agent.config.reasoning.as_ref().and_then(|r| r.summary.as_deref()).unwrap_or("none"),
                    "serviceTier": match session.agent.config.service_tier.as_deref() {
                        // `null` means Codex's explicit default; `auto` clears the
                        // inherited tier without selecting standard routing.
                        None | Some("auto") => Some("auto"),
                        Some("fast") => Some("priority"),
                        Some(tier) => Some(tier),
                    },
                    "outputSchema": match session.agent.config.text.as_ref().and_then(|t| t.format.as_ref()) {
                        Some(crate::configuration::TextFormat::JsonSchema { schema }) => Some(schema),
                        None | Some(crate::configuration::TextFormat::Text) => None,
                    },
                }),
            )
            .await
}

async fn turns(
    Extract(state): Extract<Arc<State>>,
    Path(id): Path<String>,
    Query(page): Query<PageParams>,
) -> Result<Json<Value>, ApiError> {
    let Json(session) = read_session(Extract(Arc::clone(&state)), Path(id)).await?;
    let Some(thread_id) = session.thread_id else {
        return Ok(Json(
            json!({"data": [], "nextCursor": null, "backwardsCursor": null}),
        ));
    };
    Ok(Json(state.rpc("thread/turns/list", json!({"threadId": thread_id, "cursor": page.cursor, "limit": page.limit.unwrap_or(/*default*/ 20).clamp(/*min*/ 1, /*max*/ 100), "itemsView": "summary"})).await?))
}

pub(crate) async fn cancel(
    Extract(state): Extract<Arc<State>>,
    Path((id, turn_id)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    let Json(session) = read_session(Extract(Arc::clone(&state)), Path(id)).await?;
    let thread_id = session
        .thread_id
        .ok_or_else(|| ApiError(StatusCode::CONFLICT, "session has no turn".into()))?;
    Ok(Json(
        state
            .rpc(
                "turn/interrupt",
                json!({"threadId": thread_id, "turnId": turn_id}),
            )
            .await?,
    ))
}

async fn events(
    Extract(state): Extract<Arc<State>>,
    Path(id): Path<String>,
) -> Result<Sse<impl futures::Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let receiver = state.events.subscribe();
    let _ = read_session(Extract(Arc::clone(&state)), Path(id.clone())).await?;
    if !state.connected() {
        return Err(crate::disconnected_error());
    }
    let stream = BroadcastStream::new(receiver).filter_map(move |result| {
        let state = Arc::clone(&state);
        let id = id.clone();
        async move {
            let value = match result {
                Ok(value) => value,
                Err(error) => json!({"method": "stream/lagged", "error": error.to_string()}),
            };
            let method = value["method"].as_str().unwrap_or("event");
            let own_event = if let Some(thread_id) = value.pointer("/params/threadId").and_then(Value::as_str) {
                matches!(state.store.session(&id).await, Ok(Some(session)) if session.thread_id.as_deref() == Some(thread_id))
            } else {
                method.starts_with("stream/")
            };
            own_event.then(|| Ok(Event::default().event(method).data(value.to_string())))
        }
    });
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}
