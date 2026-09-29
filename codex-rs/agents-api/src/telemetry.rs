//! Operational telemetry for operators, kept apart from the public API's usage
//! and trace records. Events and spans go through `tracing`, correlated by
//! request, session, and turn IDs. Metrics go to the process-global
//! `codex-otel` client, which records nothing until the operator configures
//! an exporter, so library code can record them unconditionally.
use axum::extract::MatchedPath;
use axum::extract::Request;
use axum::http::HeaderValue;
use axum::middleware::Next;
use axum::response::Response;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;
use uuid::Uuid;

/// HTTP requests by method, route template, and status.
pub(crate) const HTTP_REQUEST: &str = "agents_api.http.request";
pub(crate) const HTTP_REQUEST_DURATION: &str = "agents_api.http.request.duration_ms";
/// Finished turns by status and agent type (root or subagent).
pub(crate) const TURN: &str = "agents_api.turn";
pub(crate) const TURN_DURATION: &str = "agents_api.turn.duration_ms";
/// Finished tool calls by item type and status.
pub(crate) const TOOL_CALL: &str = "agents_api.tool.call";
/// Webhook delivery attempts by outcome: delivered, retry, or failed.
pub(crate) const WEBHOOK_DELIVERY: &str = "agents_api.webhook.delivery";
/// Backend connection changes: connected or lost.
pub(crate) const BACKEND_CONNECTION: &str = "agents_api.backend.connection";
/// Deleted-session cleanups by outcome.
pub(crate) const SESSION_CLEANUP: &str = "agents_api.session.cleanup";

pub(crate) fn count(name: &str, tags: &[(&str, &str)]) {
    if let Some(metrics) = codex_otel::global() {
        let _ = metrics.counter(name, /*inc*/ 1, tags);
    }
}

pub(crate) fn elapsed(name: &str, duration: Duration, tags: &[(&str, &str)]) {
    if let Some(metrics) = codex_otel::global() {
        let _ = metrics.record_duration(name, duration, tags);
    }
}

/// The matched route template, noted by [`route`] once routing succeeds. The
/// outer [`observe`] reads it after the response is built, because error
/// responses are rebuilt on the way out and would lose anything attached to
/// them.
#[derive(Clone, Default)]
struct Route(Arc<OnceLock<String>>);

/// Outermost layer: give every request an ID, returned as `x-request-id` as
/// OpenAI's API does (including on errors and rejected credentials), and a
/// span that correlates the request's events; then record its outcome.
#[tracing::instrument(name = "http.request", skip_all, fields(
    method = %request.method(),
    route = tracing::field::Empty,
    request_id = tracing::field::Empty,
    status = tracing::field::Empty,
))]
pub(crate) async fn observe(mut request: Request, next: Next) -> Response {
    let span = tracing::Span::current();
    let method = request.method().to_string();
    let request_id = format!("req_{}", Uuid::new_v4().simple());
    span.record("request_id", request_id.as_str());
    let matched = Route::default();
    request.extensions_mut().insert(matched.clone());
    let started = Instant::now();
    let mut response = next.run(request).await;
    let status = response.status();
    // Route templates hold braces, which metric tags do not allow.
    let route =
        codex_otel::sanitize_metric_tag_value(matched.0.get().map_or("unmatched", String::as_str));
    span.record("route", route.as_str());
    span.record("status", status.as_u16());
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert("x-request-id", value);
    }
    let tags = [
        ("method", method.as_str()),
        ("route", route.as_str()),
        ("status", status.as_str()),
    ];
    count(HTTP_REQUEST, &tags);
    elapsed(HTTP_REQUEST_DURATION, started.elapsed(), &tags);
    if status.is_server_error() {
        tracing::warn!(status = status.as_u16(), "request failed");
    }
    response
}

/// Route layer: note the matched route template for [`observe`].
pub(crate) async fn route(request: Request, next: Next) -> Response {
    if let (Some(path), Some(matched)) = (
        request.extensions().get::<MatchedPath>(),
        request.extensions().get::<Route>(),
    ) {
        let _ = matched.0.set(path.as_str().to_owned());
    }
    next.run(request).await
}
