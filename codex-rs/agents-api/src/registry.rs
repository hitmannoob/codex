//! The environment registry that self-hosted executors (`codex exec-server
//! --remote <base>/registry --environment-id <id>`) and the worker's harness
//! talk to, following the contract the exec-server implements:
//! - `POST cloud/environment/{id}/register`: an executor registers its Noise key
//!   and learns its rendezvous websocket URL.
//! - `POST cloud/environment/{id}/connect`: the harness learns the executor's
//!   key, a rendezvous URL, and a short-lived harness key authorization.
//! - `POST cloud/environment/{id}/validate`: the executor checks that a harness
//!   key was authorized by `connect` before completing a Noise handshake.
//!
//! Executors authenticate with the operator's environment key; the harness
//! authenticates with a token only the worker holds. Traffic itself flows
//! through [`crate::rendezvous`], encrypted end to end.
use crate::State;
use axum::Json;
use axum::Router;
use axum::extract::Path;
use axum::extract::State as Extract;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use axum::routing::post;
use codex_exec_server::EnvironmentRegistryConnectRequest;
use codex_exec_server::EnvironmentRegistryConnectResponse;
use codex_exec_server::EnvironmentRegistryHarnessKeyValidationRequest;
use codex_exec_server::EnvironmentRegistryHarnessKeyValidationResponse;
use codex_exec_server::EnvironmentRegistryRegistrationRequest;
use codex_exec_server::EnvironmentRegistryRegistrationResponse;
use codex_exec_server::NoiseChannelPublicKey;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::sync::watch;
use uuid::Uuid;

/// The only security profile the exec-server's Noise transport speaks.
pub(crate) const SECURITY_PROFILE: &str = "noise_hybrid_ik_v1";
/// How long a harness key authorization from `connect` stays usable.
const AUTHORIZATION_TTL: Duration = Duration::from_secs(5 * 60);
/// Authorizations kept per environment; older ones are dropped first.
const MAX_AUTHORIZATIONS: usize = 64;

/// Where an environment's executor stands, as the registry observes it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExecutorState {
    /// No executor has registered since this process started.
    Unregistered,
    /// Registered, but its rendezvous websocket is not open.
    Registered,
    Connected,
}

pub(crate) struct Registration {
    pub(crate) id: String,
    pub(crate) executor_public_key: NoiseChannelPublicKey,
    /// Authenticates the executor's rendezvous websocket, which carries no
    /// authorization header.
    pub(crate) executor_token: String,
}

struct Authorization {
    harness_public_key: NoiseChannelPublicKey,
    registration_id: String,
    issued: Instant,
}

/// One environment's registry and rendezvous state.
pub(crate) struct Slot {
    pub(crate) registration: Option<Registration>,
    /// The connected executor socket's outbound frames, and a generation that
    /// tells a replaced connection apart from its successor.
    pub(crate) executor: Option<(u64, mpsc::Sender<Vec<u8>>)>,
    /// Harness sockets by connection id.
    pub(crate) harnesses: HashMap<u64, mpsc::Sender<Vec<u8>>>,
    /// The harness connection that claimed each relay stream.
    pub(crate) streams: HashMap<String, u64>,
    authorizations: HashMap<String, Authorization>,
    state: watch::Sender<ExecutorState>,
}

impl Slot {
    fn new() -> Self {
        Self {
            registration: None,
            executor: None,
            harnesses: HashMap::new(),
            streams: HashMap::new(),
            authorizations: HashMap::new(),
            state: watch::channel(ExecutorState::Unregistered).0,
        }
    }

    pub(crate) fn set_state(&self, state: ExecutorState) {
        self.state.send_if_modified(|current| {
            let changed = *current != state;
            *current = state;
            changed
        });
    }

    /// Whether a harness websocket may use this authorization.
    pub(crate) fn authorizes(&self, token: &str) -> bool {
        self.authorizations
            .get(token)
            .is_some_and(|authorization| authorization.issued.elapsed() < AUTHORIZATION_TTL)
    }
}

/// Registry state shared by the HTTP endpoints and rendezvous sockets.
pub(crate) struct Registry {
    environment_key: OnceLock<String>,
    /// The registry URL the worker's harness uses; it reaches this process
    /// directly rather than through any public address.
    harness_url: OnceLock<String>,
    harness_token: String,
    slots: Mutex<HashMap<String, Slot>>,
    next_connection: std::sync::atomic::AtomicU64,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            environment_key: OnceLock::new(),
            harness_url: OnceLock::new(),
            harness_token: format!("harness_{}", Uuid::new_v4().simple()),
            slots: Mutex::new(HashMap::new()),
            next_connection: std::sync::atomic::AtomicU64::new(1),
        }
    }
}

impl Registry {
    /// Accept executors that present this key, and attach the worker's harness
    /// through `harness_url`. Only the first configuration takes effect.
    pub(crate) fn configure(&self, environment_key: String, harness_url: String) {
        let _ = self.environment_key.set(environment_key);
        let _ = self.harness_url.set(harness_url);
    }

    /// The registry URL for the worker's harness, once environments are enabled.
    pub(crate) fn harness_url(&self) -> Option<&str> {
        self.harness_url.get().map(String::as_str)
    }

    /// The token the worker's harness presents to `connect`.
    pub(crate) fn harness_token(&self) -> &str {
        &self.harness_token
    }

    pub(crate) fn next_connection(&self) -> u64 {
        self.next_connection
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// Run `f` on an environment's slot, creating it when absent. Only callers
    /// that know the environment exists create slots.
    fn with_new_slot<T>(&self, environment_id: &str, f: impl FnOnce(&mut Slot) -> T) -> T {
        let mut slots = crate::lock(&self.slots);
        f(slots
            .entry(environment_id.to_owned())
            .or_insert_with(Slot::new))
    }

    /// Run `f` on an environment's slot, if it has one.
    pub(crate) fn with_slot<T>(
        &self,
        environment_id: &str,
        f: impl FnOnce(&mut Slot) -> T,
    ) -> Option<T> {
        crate::lock(&self.slots).get_mut(environment_id).map(f)
    }

    /// Watch an existing environment's executor state.
    pub(crate) fn watch(&self, environment_id: &str) -> watch::Receiver<ExecutorState> {
        self.with_new_slot(environment_id, |slot| slot.state.subscribe())
    }

    /// Forget an environment, closing its sockets by dropping their senders.
    pub(crate) fn remove(&self, environment_id: &str) {
        crate::lock(&self.slots).remove(environment_id);
    }
}

pub(crate) fn router() -> Router<Arc<State>> {
    Router::new()
        .route("/registry/cloud/environment/{id}/register", post(register))
        .route("/registry/cloud/environment/{id}/connect", post(connect))
        .route("/registry/cloud/environment/{id}/validate", post(validate))
        .route(
            "/registry/rendezvous/{id}/executor",
            get(crate::rendezvous::executor),
        )
        .route(
            "/registry/rendezvous/{id}/harness",
            get(crate::rendezvous::harness),
        )
}

/// A registry error in the shape the exec-server parses.
pub(crate) fn error(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        Json(json!({"error":{"code":code,"message":message}})),
    )
        .into_response()
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}

/// Check the executor's environment key and that the environment belongs to a
/// session. Comparison runs in constant time.
async fn executor_allowed(
    state: &State,
    headers: &HeaderMap,
    environment_id: &str,
) -> Result<(), Response> {
    let Some(key) = state.registry.environment_key.get() else {
        return Err(error(
            StatusCode::NOT_IMPLEMENTED,
            "environments_disabled",
            "self-hosted environments require the operator to configure an environment key",
        ));
    };
    if !bearer(headers).is_some_and(|token| constant_time_eq(token, key)) {
        return Err(error(
            StatusCode::UNAUTHORIZED,
            "invalid_environment_key",
            "invalid environment key",
        ));
    }
    if !crate::environments::exists(state, environment_id)
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "registry error",
            )
        })?
    {
        return Err(error(
            StatusCode::NOT_FOUND,
            "environment_not_found",
            "no session uses this environment",
        ));
    }
    Ok(())
}

fn constant_time_eq(left: &str, right: &str) -> bool {
    left.len() == right.len()
        && left
            .bytes()
            .zip(right.bytes())
            .fold(0u8, |difference, (a, b)| difference | (a ^ b))
            == 0
}

/// The URL of `path` on the host the caller reached, with `schemes` as the
/// (secure, plain) pair chosen by `x-forwarded-proto`.
fn url_for(headers: &HeaderMap, schemes: (&str, &str), path: &str) -> String {
    let host = headers
        .get(axum::http::header::HOST)
        .and_then(|host| host.to_str().ok())
        .unwrap_or("127.0.0.1");
    let secure = headers
        .get("x-forwarded-proto")
        .and_then(|proto| proto.to_str().ok())
        == Some("https");
    let scheme = if secure { schemes.0 } else { schemes.1 };
    format!("{scheme}://{host}{path}")
}

fn websocket_url(headers: &HeaderMap, path: &str) -> String {
    url_for(headers, ("wss", "ws"), path)
}

/// The registry URL executors pass to `codex exec-server --remote`.
pub(crate) fn remote_url(headers: &HeaderMap) -> String {
    url_for(headers, ("https", "http"), "/registry")
}

async fn register(
    Extract(state): Extract<Arc<State>>,
    Path(environment_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<EnvironmentRegistryRegistrationRequest>,
) -> Response {
    if let Err(response) = executor_allowed(&state, &headers, &environment_id).await {
        return response;
    }
    if request.security_profile != SECURITY_PROFILE {
        return error(
            StatusCode::BAD_REQUEST,
            "unsupported_security_profile",
            "only noise_hybrid_ik_v1 is supported",
        );
    }
    let registration_id = format!("reg_{}", Uuid::new_v4().simple());
    let executor_token = format!("exe_{}", Uuid::new_v4().simple());
    let url = websocket_url(
        &headers,
        &format!(
            "/registry/rendezvous/{environment_id}/executor?registration={registration_id}&token={executor_token}"
        ),
    );
    // A new registration replaces the previous one; its executor socket and
    // outstanding authorizations no longer apply.
    state.registry.with_new_slot(&environment_id, |slot| {
        slot.registration = Some(Registration {
            id: registration_id.clone(),
            executor_public_key: request.executor_public_key,
            executor_token,
        });
        slot.executor = None;
        slot.authorizations.clear();
        slot.set_state(ExecutorState::Registered);
    });
    tracing::info!(environment_id, "executor registered");
    Json(EnvironmentRegistryRegistrationResponse {
        environment_id,
        url,
        security_profile: SECURITY_PROFILE.to_owned(),
        executor_registration_id: registration_id,
    })
    .into_response()
}

async fn connect(
    Extract(state): Extract<Arc<State>>,
    Path(environment_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<EnvironmentRegistryConnectRequest>,
) -> Response {
    if !bearer(&headers)
        .is_some_and(|token| constant_time_eq(token, state.registry.harness_token()))
    {
        return error(
            StatusCode::UNAUTHORIZED,
            "invalid_harness_token",
            "invalid harness token",
        );
    }
    let token = format!("hka_{}", Uuid::new_v4().simple());
    let bundle = state
        .registry
        .with_slot(&environment_id, |slot| {
            let registration = slot.registration.as_ref()?;
            slot.executor.as_ref()?;
            slot.authorizations
                .retain(|_, authorization| authorization.issued.elapsed() < AUTHORIZATION_TTL);
            if slot.authorizations.len() >= MAX_AUTHORIZATIONS
                && let Some(oldest) = slot
                    .authorizations
                    .iter()
                    .min_by_key(|(_, authorization)| authorization.issued)
                    .map(|(token, _)| token.clone())
            {
                slot.authorizations.remove(&oldest);
            }
            slot.authorizations.insert(
                token.clone(),
                Authorization {
                    harness_public_key: request.harness_public_key.clone(),
                    registration_id: registration.id.clone(),
                    issued: Instant::now(),
                },
            );
            Some((
                registration.id.clone(),
                registration.executor_public_key.clone(),
            ))
        })
        .flatten();
    let Some((registration_id, executor_public_key)) = bundle else {
        // The harness retries this code while it waits for an executor.
        return error(
            StatusCode::CONFLICT,
            "environment_offline",
            "no executor is connected for this environment",
        );
    };
    let url = websocket_url(
        &headers,
        &format!("/registry/rendezvous/{environment_id}/harness?authorization={token}"),
    );
    Json(EnvironmentRegistryConnectResponse {
        environment_id,
        url,
        security_profile: SECURITY_PROFILE.to_owned(),
        executor_registration_id: registration_id,
        executor_public_key,
        harness_key_authorization: token,
    })
    .into_response()
}

async fn validate(
    Extract(state): Extract<Arc<State>>,
    Path(environment_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<EnvironmentRegistryHarnessKeyValidationRequest>,
) -> Response {
    if let Err(response) = executor_allowed(&state, &headers, &environment_id).await {
        return response;
    }
    // Each authorization completes one handshake for the key it was issued to,
    // against the registration current when it was issued.
    let valid = state
        .registry
        .with_slot(&environment_id, |slot| {
            match slot
                .authorizations
                .remove(&request.harness_key_authorization)
            {
                Some(authorization) => {
                    authorization.issued.elapsed() < AUTHORIZATION_TTL
                        && authorization.harness_public_key == request.harness_public_key
                        && authorization.registration_id == request.executor_registration_id
                        && slot.registration.as_ref().is_some_and(|registration| {
                            registration.id == request.executor_registration_id
                        })
                }
                None => false,
            }
        })
        .unwrap_or(false);
    Json(EnvironmentRegistryHarnessKeyValidationResponse { valid }).into_response()
}
