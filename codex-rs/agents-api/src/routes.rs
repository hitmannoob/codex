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
use serde_json::Value;
use serde_json::json;
use std::convert::Infallible;
use std::sync::Arc;
use tokio_stream::wrappers::BroadcastStream;

pub(crate) fn router(state: Arc<State>) -> Router {
    Router::new()
        .route("/v1/agents", post(create_agent))
        .route("/v1/agents/{id}", get(read_agent))
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
        .layer(DefaultBodyLimit::max(/*limit*/ 16 * 1024))
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            authorize,
        ))
        .with_state(state)
}

async fn authorize(
    Extract(state): Extract<Arc<State>>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let compatible = request.uri().path().starts_with("/v1/agents/sessions")
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
    let config: AgentConfig = if compatible {
        crate::contract::configure(AgentConfig::default(), value)?
    } else {
        serde_json::from_value(value).map_err(|e| crate::contract::invalid(e.to_string()))?
    };
    if config.model.trim().is_empty()
        || config.model.len() > 256
        || config.instructions.len() > 1024
    {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "model must be 1-256 bytes; instructions must be at most 1024 bytes".into(),
        ));
    }
    crate::capabilities::validate(&config)?;
    let agent = state.store.create_agent(config).await?;
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
    value["updated_at"] = json!(agent.created_at);
    value["metadata"] = json!({});
    value
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

pub(crate) async fn input(
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
    // Serialize bootstrap and submission, not the model/tool execution that follows.
    let _permit = state
        .input_gate
        .acquire()
        .await
        .map_err(anyhow::Error::from)?;
    let Json(session) = read_session(Extract(Arc::clone(&state)), Path(id.clone())).await?;
    let config =
        crate::capabilities::overrides(&state, &session.agent.config, &session.environment).await?;
    let thread_id = if let Some(thread_id) = session.thread_id {
        state
            .rpc(
                "thread/resume",
                json!({"threadId": thread_id, "excludeTurns": true, "config": config,
                    "model": session.agent.config.model, "developerInstructions": session.agent.config.instructions}),
            )
            .await?;
        thread_id
    } else {
        let environments = match &session.environment {
            Environment::None => json!([]),
            Environment::Local { cwd } => json!([{"environmentId": "local", "cwd": cwd}]),
        };
        let response = state
            .rpc(
                "thread/start",
                json!({
                    "model": session.agent.config.model,
                    "developerInstructions": session.agent.config.instructions,
                    "environments": environments,
                    "config": config,
                    "dynamicTools": session.agent.config.tools.iter().map(|tool| json!({
                        "type": "function", "name": tool.name, "description": tool.description, "inputSchema": tool.parameters
                    })).collect::<Vec<_>>(),
                    "approvalPolicy": "never", "sandbox": "read-only", "ephemeral": false,
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
            .bind(&id)
            .execute(&state.store.0)
            .await
            .map_err(anyhow::Error::from)?;
        thread_id
    };
    Ok(Json(
        state
            .rpc(
                "turn/start",
                json!({"threadId": thread_id, "input": [{"type": "text", "text": params.input}]}),
            )
            .await?,
    ))
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
    if !state.connected.load(std::sync::atomic::Ordering::Acquire) {
        return Err(ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "app-server disconnected".into(),
        ));
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
