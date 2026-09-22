mod actions;
mod capabilities;
mod contract;
mod records;
mod resources;
mod routes;
mod store;

use axum::Json;
use axum::Router;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use codex_app_server_client::AppServerClient;
use codex_app_server_client::AppServerEvent;
use codex_app_server_client::AppServerRequestHandle;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::ServerRequest;
use codex_utils_absolute_path::AbsolutePathBuf;
use serde_json::Value;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::sync::Semaphore;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use uuid::Uuid;

struct State {
    store: store::Store,
    backend: AppServerRequestHandle,
    input_gate: Semaphore,
    events: broadcast::Sender<Value>,
    public_events: broadcast::Sender<Value>,
    connected: AtomicBool,
    token: String,
    submissions: mpsc::Sender<actions::Submission>,
}

impl State {
    async fn rpc(&self, method: &str, params: Value) -> Result<Value, ApiError> {
        if !self.connected.load(Ordering::Acquire) {
            return Err(ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "app-server disconnected".into(),
            ));
        }
        let request = serde_json::from_value(
            json!({"id": Uuid::new_v4().to_string(), "method": method, "params": params}),
        )
        .map_err(anyhow::Error::from)?;
        self.backend
            .request(request)
            .await
            .map_err(anyhow::Error::from)?
            .map_err(|error| ApiError(StatusCode::BAD_GATEWAY, error.message))
    }
}

struct ApiError(StatusCode, String);

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        eprintln!("agents-api: {error:#}");
        Self(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal API error".into(),
        )
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (self.0, Json(json!({"error": self.1}))).into_response()
    }
}

impl ApiError {
    fn public_response(self) -> axum::response::Response {
        let kind = if self.0.is_server_error() {
            "server_error"
        } else {
            "invalid_request_error"
        };
        (
            self.0,
            Json(json!({"error": {"message":self.1,"type":kind,"param":null,"code":null}})),
        )
            .into_response()
    }
}

/// HTTP facade over a dedicated Codex app-server connection.
/// Dropping this owner requests shutdown; call `shutdown` to wait for completion.
pub struct AgentsApi {
    router: Router,
    worker: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
    stop: Option<oneshot::Sender<()>>,
}

impl AgentsApi {
    pub async fn new(
        mut client: AppServerClient,
        directory: AbsolutePathBuf,
        token: String,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            token.len() >= 32,
            "API token must contain at least 32 bytes"
        );
        let (submissions, mut submissions_rx) = mpsc::channel(/*buffer*/ 32);
        let state = Arc::new(State {
            store: store::Store::open(directory).await?,
            backend: client.request_handle(),
            input_gate: Semaphore::new(/*permits*/ 1),
            events: broadcast::channel(/*capacity*/ 128).0,
            public_events: broadcast::channel(/*capacity*/ 128).0,
            connected: AtomicBool::new(/*v*/ true),
            token,
            submissions,
        });
        let router = routes::router(Arc::clone(&state));
        let (stop, mut stopping) = oneshot::channel();
        let worker = tokio::spawn(async move {
            loop {
                let event = tokio::select! {
                    biased;
                    event = client.next_event() => event,
                    _ = &mut stopping => break,
                    Some(submission) = submissions_rx.recv() => {
                        actions::resolve(&state, &client, submission).await;
                        continue;
                    }
                };
                let Some(event) = event else { break };
                match event {
                    AppServerEvent::ServerNotification(notification) => {
                        if let Ok(value) = serde_json::to_value(notification) {
                            if actions::notification(&state, &value).await.is_err() {
                                break;
                            }
                            if records::notification(&state, &value).await.is_err() {
                                break;
                            }
                            let _ = state.events.send(value);
                        }
                    }
                    AppServerEvent::ServerRequest(request) => {
                        let request_id = request.id().clone();
                        if let ServerRequest::DynamicToolCall { params, .. } = *request
                            && actions::register(&state, &request_id, params).await.is_ok()
                        {
                            continue;
                        }
                        let _ = client
                            .reject_server_request(
                                request_id,
                                JSONRPCErrorError {
                                    code: -32601,
                                    message: "unsupported or invalid interactive request".into(),
                                    data: None,
                                },
                            )
                            .await;
                    }
                    AppServerEvent::Lagged { skipped } => {
                        // Missing backend notifications leave the public projection incomplete.
                        let _ = state.events.send(
                            json!({"method": "stream/lagged", "params": {"skipped": skipped}}),
                        );
                        break;
                    }
                    AppServerEvent::Disconnected { .. } => break,
                }
            }
            state.connected.store(/*val*/ false, Ordering::Release);
            let _ = sqlx::query("UPDATE tool_calls SET status = 'unavailable' WHERE status IN ('pending', 'submitting')")
                .execute(&state.store.0).await;
            let _ = state.events.send(json!({"method": "stream/disconnected"}));
            let _ = records::disconnected(&state.store.0).await;
            let _ = state.public_events.send(json!({"type":"disconnect"}));
            client.shutdown().await
        });
        Ok(Self {
            router,
            worker: Some(worker),
            stop: Some(stop),
        })
    }

    pub fn router(&self) -> Router {
        self.router.clone()
    }

    /// Close the backend connection, allowing Codex to flush session state.
    pub async fn shutdown(mut self) -> anyhow::Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(worker) = self.worker.take() {
            worker.await??;
        }
        Ok(())
    }
}

impl Drop for AgentsApi {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}
