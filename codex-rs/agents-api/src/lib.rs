mod actions;
mod agent_tools;
mod artifacts;
mod capabilities;
mod configuration;
mod contract;
mod credentials;
mod environment_files;
mod environments;
mod files;
mod gates;
mod input;
mod mcp;
mod otlp;
mod reconcile;
mod records;
mod registry;
mod rendezvous;
mod resources;
mod routes;
mod secrets;
mod sessions;
mod stdio_env;
mod store;
mod streaming;
mod subagents;
mod telemetry;
mod traces;
mod turns;
mod upload;
mod usage;
mod vaults;
mod webhook_delivery;
mod webhook_outbox;
mod webhooks;

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
use std::collections::HashSet;
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
    /// Threads started or resumed on this connection. A replacement connection
    /// starts empty, so each thread is resumed once per connection.
    loaded: Arc<Mutex<HashSet<String>>>,
    /// Self-hosted environments added to this connection's worker.
    environments: Arc<Mutex<HashSet<String>>>,
}

struct State {
    store: store::Store,
    backend: Mutex<Option<Backend>>,
    input_gates: gates::Gates,
    streams: streaming::Streams,
    /// Lower-case MCP hosts the operator allows to be internal or plain http.
    mcp_hosts: Mutex<HashSet<String>>,
    /// Lower-case webhook hosts the operator allows to be internal or plain http.
    webhook_hosts: Mutex<HashSet<String>>,
    /// Encrypted credential values, available once the operator supplies a passphrase.
    secrets: secrets::Secrets,
    /// Wakes the webhook dispatcher after a commit that may have queued deliveries.
    webhook_wake: tokio::sync::Notify,
    /// The environment registry and rendezvous state for self-hosted environments.
    registry: registry::Registry,
    /// Input waiting for a self-hosted executor to connect.
    waits: environments::Waits,
    /// Each session's most recently started turn, as (thread ID, turn ID),
    /// for a cancel that arrives before Codex reports the turn.
    started_turns: Mutex<std::collections::HashMap<String, (String, String)>>,
    /// Turns cancelled before Codex made them active; interrupted when their
    /// start is recorded.
    pending_cancels: Mutex<HashSet<String>>,
    /// Uploaded file contents.
    files: files::Files,
    /// This service's own connections to self-hosted executors, for
    /// environment files.
    executors: codex_exec_server::EnvironmentManager,
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

    fn loaded_threads(&self) -> Result<Arc<Mutex<HashSet<String>>>, ApiError> {
        lock(&self.backend)
            .as_ref()
            .map(|backend| Arc::clone(&backend.loaded))
            .ok_or_else(disconnected_error)
    }

    fn attached_environments(&self) -> Result<Arc<Mutex<HashSet<String>>>, ApiError> {
        lock(&self.backend)
            .as_ref()
            .map(|backend| Arc::clone(&backend.environments))
            .ok_or_else(disconnected_error)
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
        tracing::error!(error = format!("{error:#}"), "internal error");
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
        // The pinned SDK recognizes the pending-call registration race by code.
        let code = self
            .1
            .starts_with(input::UNKNOWN_PENDING_CALL)
            .then_some("invalid_request_error");
        (
            self.0,
            Json(json!({"error": {"message":self.1,"type":kind,"param":null,"code":code}})),
        )
            .into_response()
    }
}

/// HTTP facade over a dedicated Codex app-server connection.
/// Dropping this owner requests shutdown; call `shutdown` to wait for completion.
pub struct AgentsApi {
    state: Arc<State>,
    directory: AbsolutePathBuf,
    router: Router,
    pump: Mutex<Option<tokio::task::JoinHandle<std::io::Result<()>>>>,
    dispatcher: tokio::task::JoinHandle<()>,
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
            store: store::Store::open(directory.clone()).await?,
            backend: Mutex::new(None),
            input_gates: gates::Gates::default(),
            streams: streaming::Streams::default(),
            mcp_hosts: Mutex::default(),
            webhook_hosts: Mutex::default(),
            secrets: secrets::Secrets::default(),
            webhook_wake: tokio::sync::Notify::new(),
            registry: registry::Registry::default(),
            waits: environments::Waits::default(),
            started_turns: Mutex::default(),
            pending_cancels: Mutex::default(),
            files: files::Files(directory.join("files").to_path_buf()),
            executors: codex_exec_server::EnvironmentManager::without_environments(
                codex_http_client::HttpClientFactory::new(
                    codex_http_client::OutboundProxyPolicy::ReqwestDefault,
                ),
            ),
            events: broadcast::channel(/*capacity*/ 128).0,
            // Text deltas arrive at token rate; a consumer that falls this far
            // behind is closed and recovers from saved records.
            public_events: broadcast::channel(/*capacity*/ 1024).0,
            token,
        });
        environments::recover(&state).await?;
        files::sweep(&state).await?;
        let router = routes::router(Arc::clone(&state));
        let stop = watch::channel(/*stopping*/ false).0;
        let dispatcher = tokio::spawn(webhook_outbox::dispatch(
            Arc::clone(&state),
            stop.subscribe(),
        ));
        let api = Self {
            state,
            directory,
            router,
            pump: Mutex::new(None),
            dispatcher,
            reconnect_gate: Semaphore::new(/*permits*/ 1),
            stop,
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
            loaded: Arc::default(),
            environments: Arc::default(),
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
        tracing::info!("backend connected");
        telemetry::count(telemetry::BACKEND_CONNECTION, &[("event", "connected")]);
        if let Err(error) = reconcile::run(&self.state).await {
            tracing::warn!(error = format!("{error:#}"), "reconciliation failed");
        }
        // Retry thread deletions a previous connection or process left queued.
        if let Err(error) = sessions::cleanup(&self.state).await {
            tracing::warn!(error = format!("{error:#}"), "session cleanup failed");
        }
        Ok(())
    }

    pub fn router(&self) -> Router {
        self.router.clone()
    }

    /// Enable vault credentials, encrypting their values in the data directory
    /// under this passphrase. The same passphrase must be supplied on every
    /// start; one that cannot read the credentials stored earlier is refused.
    pub async fn configure_vault(&self, passphrase: String) -> anyhow::Result<()> {
        anyhow::ensure!(
            passphrase.len() >= 32,
            "vault passphrase must contain at least 32 bytes"
        );
        let stored = self
            .state
            .secrets
            .configure(self.directory.to_path_buf(), passphrase)
            .await?;
        let removed = credentials::sweep(&self.state, stored).await?;
        if removed > 0 {
            tracing::info!(removed, "removed secrets left by interrupted deletions");
        }
        Ok(())
    }

    /// Accept self-hosted executors that authenticate with this environment
    /// key. It grants only the registry endpoints, never the API. The worker
    /// reaches the registry at `registry_url`, this service's own address
    /// followed by `/registry`.
    pub fn configure_environments(
        &self,
        environment_key: String,
        registry_url: String,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            environment_key.len() >= 32,
            "environment key must contain at least 32 bytes"
        );
        anyhow::ensure!(
            environment_key != self.state.token,
            "environment key must differ from the API token"
        );
        self.state.registry.configure(environment_key, registry_url);
        Ok(())
    }

    /// How long input waits for a self-hosted executor to connect before it is
    /// dropped. The contract's five minutes is the default.
    pub fn set_environment_connection_wait(&self, wait: std::time::Duration) {
        *lock(&self.state.waits.limit) = wait;
    }

    /// Allow public MCP servers on these hosts even when they resolve to
    /// loopback, private, or link-local addresses, or use plain http.
    pub fn allow_mcp_hosts(&self, hosts: impl IntoIterator<Item = String>) {
        lock(&self.state.mcp_hosts).extend(hosts.into_iter().map(|host| host.to_ascii_lowercase()));
    }

    /// Allow webhook deliveries to these hosts even when they resolve to
    /// loopback, private, or link-local addresses, or use plain http.
    pub fn allow_webhook_hosts(&self, hosts: impl IntoIterator<Item = String>) {
        lock(&self.state.webhook_hosts)
            .extend(hosts.into_iter().map(|host| host.to_ascii_lowercase()));
    }

    /// Close the backend connection, allowing Codex to flush session state.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        let _ = self.stop.send(true);
        // An interrupted delivery stays queued and is repeated after restart.
        self.dispatcher.abort();
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
                    if let Err(error) = streaming::notification(&state, &value).await {
                        tracing::warn!(error = format!("{error:#}"), "stream event dropped");
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
    if lost {
        tracing::warn!("backend connection lost");
        telemetry::count(telemetry::BACKEND_CONNECTION, &[("event", "lost")]);
    }
    if let Err(error) = disconnect_bookkeeping(&state).await {
        tracing::error!(
            error = format!("{error:#}"),
            "disconnect bookkeeping failed"
        );
    }
    match client.shutdown().await {
        Err(error) if lost => {
            tracing::warn!(%error, "backend cleanup after connection loss failed");
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
    state.streams.clear();
    let _ = state.events.send(json!({"method": "stream/disconnected"}));
    let _ = state.public_events.send(json!({"type":"disconnect"}));
    Ok(())
}
