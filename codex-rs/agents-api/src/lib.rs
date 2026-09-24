mod actions;
mod capabilities;
mod contract;
mod reconcile;
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
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use tokio::sync::Semaphore;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::watch;
use uuid::Uuid;

/// The live backend connection routing state. Exactly one pump task consumes
/// each connection, and pumps never overlap: a replacement is installed only
/// after the previous pump has exited and recorded its disconnect bookkeeping,
/// so pending function rows always belong to the connection that created them.
struct Backend {
    handle: AppServerRequestHandle,
    submissions: mpsc::Sender<actions::Submission>,
    cancel: watch::Sender<bool>,
}

struct State {
    store: store::Store,
    backend: Mutex<Option<Backend>>,
    input_gate: Semaphore,
    events: broadcast::Sender<Value>,
    public_events: broadcast::Sender<Value>,
    token: String,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl State {
    async fn rpc(&self, method: &str, params: Value) -> Result<Value, ApiError> {
        // Clone the handle out of the lock; backend I/O must not hold it.
        let handle = lock(&self.backend)
            .as_ref()
            .map(|backend| backend.handle.clone())
            .ok_or_else(disconnected_error)?;
        let request = serde_json::from_value(
            json!({"id": Uuid::new_v4().to_string(), "method": method, "params": params}),
        )
        .map_err(anyhow::Error::from)?;
        // A transport failure means this connection is gone: report it as a
        // disconnection (503) rather than an internal error, so a request that
        // races a worker crash sees the documented recovery response. A
        // server-side JSON-RPC error is a genuine upstream failure (502).
        handle
            .request(request)
            .await
            .map_err(|_| disconnected_error())?
            .map_err(|error| ApiError(StatusCode::BAD_GATEWAY, error.message))
    }

    fn connected(&self) -> bool {
        lock(&self.backend).is_some()
    }

    fn submissions(&self) -> Option<mpsc::Sender<actions::Submission>> {
        lock(&self.backend)
            .as_ref()
            .map(|backend| backend.submissions.clone())
    }
}

fn disconnected_error() -> ApiError {
    ApiError(
        StatusCode::SERVICE_UNAVAILABLE,
        "app-server disconnected".into(),
    )
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
    state: Arc<State>,
    router: Router,
    pump: Mutex<Option<tokio::task::JoinHandle<std::io::Result<()>>>>,
    reconnect_gate: Semaphore,
    stop: watch::Sender<bool>,
}

impl AgentsApi {
    pub async fn new(
        client: AppServerClient,
        directory: AbsolutePathBuf,
        token: String,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            token.len() >= 32,
            "API token must contain at least 32 bytes"
        );
        let state = Arc::new(State {
            store: store::Store::open(directory).await?,
            backend: Mutex::new(None),
            input_gate: Semaphore::new(/*permits*/ 1),
            events: broadcast::channel(/*capacity*/ 128).0,
            public_events: broadcast::channel(/*capacity*/ 128).0,
            token,
        });
        let router = routes::router(Arc::clone(&state));
        let api = Self {
            state,
            router,
            pump: Mutex::new(None),
            reconnect_gate: Semaphore::new(/*permits*/ 1),
            stop: watch::channel(/*stopping*/ false).0,
        };
        api.reconnect(client).await?;
        Ok(api)
    }

    /// Replace the backend connection.
    ///
    /// The previous connection is retired first: its pump exits, marks its
    /// unresolvable function calls `unavailable`, and shuts its own client
    /// down before the replacement starts, so stale callbacks cannot resolve
    /// work belonging to the new connection. Durable retrieval keeps serving
    /// throughout. A caller-owned worker process behind the old connection is
    /// never terminated by this API.
    pub async fn reconnect(&self, client: AppServerClient) -> anyhow::Result<()> {
        let _replacing = self
            .reconnect_gate
            .acquire()
            .await
            .map_err(anyhow::Error::from)?;
        let previous = lock(&self.state.backend).take();
        if let Some(previous) = previous {
            let _ = previous.cancel.send(true);
        }
        let retired = lock(&self.pump).take();
        if let Some(retired) = retired {
            retired.await??;
        }
        let handle = client.request_handle();
        let (submissions, submissions_rx) = mpsc::channel(/*buffer*/ 32);
        let (cancel, cancelled) = watch::channel(/*cancelled*/ false);
        let identity = submissions.clone();
        *lock(&self.state.backend) = Some(Backend {
            handle,
            submissions,
            cancel,
        });
        let task = tokio::spawn(pump(
            Arc::clone(&self.state),
            client,
            self.stop.subscribe(),
            cancelled,
            submissions_rx,
            identity,
        ));
        *lock(&self.pump) = Some(task);
        // Correct any turns a prior disconnect failed provisionally against the
        // now-reachable rollout history. Best-effort: a failure here must not
        // fail the reconnect, since the service is otherwise ready.
        if let Err(error) = reconcile::run(&self.state).await {
            eprintln!("agents-api: reconciliation failed: {error:#}");
        }
        Ok(())
    }

    pub fn router(&self) -> Router {
        self.router.clone()
    }

    /// Close the backend connection, allowing Codex to flush session state.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        let _ = self.stop.send(true);
        let pump = lock(&self.pump).take();
        if let Some(pump) = pump {
            pump.await??;
        }
        Ok(())
    }
}

impl Drop for AgentsApi {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

/// Consumes one backend connection's notifications until it disconnects, the
/// API stops, or a replacement retires it. On exit it stops routing mutations
/// to this connection, then records the disconnect exactly once; `reconnect`
/// awaits that completion before a new connection may create function rows.
async fn pump(
    state: Arc<State>,
    mut client: AppServerClient,
    mut stopping: watch::Receiver<bool>,
    mut cancelled: watch::Receiver<bool>,
    mut submissions: mpsc::Receiver<actions::Submission>,
    identity: mpsc::Sender<actions::Submission>,
) -> std::io::Result<()> {
    let mut lost = false;
    loop {
        let event = tokio::select! {
            biased;
            event = client.next_event() => event,
            _ = stopping.changed() => break,
            _ = cancelled.changed() => break,
            Some(submission) = submissions.recv() => {
                actions::resolve(&state, &client, submission).await;
                continue;
            }
        };
        let Some(event) = event else {
            lost = true;
            break;
        };
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
                lost = true;
                let _ = state
                    .events
                    .send(json!({"method": "stream/lagged", "params": {"skipped": skipped}}));
                break;
            }
            AppServerEvent::Disconnected { .. } => {
                lost = true;
                break;
            }
        }
    }
    // Stop routing mutations to this connection before recording the loss.
    // A replacement may already have taken the slot; only clear our own entry.
    {
        let mut slot = lock(&state.backend);
        if slot
            .as_ref()
            .is_some_and(|backend| backend.submissions.same_channel(&identity))
        {
            *slot = None;
        }
    }
    if let Err(error) = disconnect_bookkeeping(&state).await {
        eprintln!("agents-api: disconnect bookkeeping failed: {error:#}");
    }
    match client.shutdown().await {
        Err(error) if lost => {
            eprintln!("agents-api: backend cleanup after connection loss: {error}");
            Ok(())
        }
        result => result,
    }
}

/// The lost connection's JSON-RPC waiters cannot be recreated, so its pending
/// function calls become unavailable and its interrupted public work is marked
/// failed. Runs once per retired connection, before any replacement serves.
async fn disconnect_bookkeeping(state: &State) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE tool_calls SET status = 'unavailable' WHERE status IN ('pending', 'submitting')",
    )
    .execute(&state.store.0)
    .await?;
    records::disconnected(&state.store.0).await?;
    let _ = state.events.send(json!({"method": "stream/disconnected"}));
    let _ = state.public_events.send(json!({"type":"disconnect"}));
    Ok(())
}
