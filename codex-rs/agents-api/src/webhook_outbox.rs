//! The webhook outbox and its dispatcher. Session changes queue one delivery
//! per subscribed endpoint in the transaction that commits the change, so an
//! event is never lost or sent for a change that did not happen. Delivery is
//! at least once: an attempt cut short by a crash or shutdown is repeated, with
//! the same `webhook-id`, so receivers can drop duplicates.
use crate::State;
use crate::contract::now;
use axum::http::StatusCode;
use futures::StreamExt;
use serde_json::Value;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use uuid::Uuid;

/// How long a delivery is retried before it is given up, as the webhook guide
/// documents.
const RETRY_WINDOW_SECS: i64 = 72 * 60 * 60;
/// First retry delay; each later one doubles, up to [`MAX_BACKOFF_SECS`].
const FIRST_BACKOFF_SECS: i64 = 5;
const MAX_BACKOFF_SECS: i64 = 60 * 60;
/// Longest the dispatcher sleeps between checks when nothing wakes it.
const IDLE_POLL: Duration = Duration::from_secs(/*secs*/ 5);
/// Deliveries claimed per round, and endpoints served at once.
const BATCH: i64 = 64;
const CONCURRENT_ENDPOINTS: usize = 8;
/// How long finished deliveries are kept.
const RETENTION_SECS: i64 = 7 * 24 * 60 * 60;

/// Queue `event_type` for every endpoint subscribed to it, inside the
/// transaction that makes the change it reports.
pub(crate) async fn enqueue(
    connection: &mut sqlx::SqliteConnection,
    event_type: &str,
    data: Value,
) -> anyhow::Result<()> {
    let endpoints: Vec<String> = sqlx::query_scalar("SELECT id FROM webhook_endpoints WHERE EXISTS (SELECT 1 FROM json_each(data, '$.event_types') WHERE value = ?) ORDER BY created_seq")
        .bind(event_type)
        .fetch_all(&mut *connection)
        .await?;
    if endpoints.is_empty() {
        return Ok(());
    }
    let body = crate::webhook_delivery::event(event_type, data).to_string();
    let now = now() as i64;
    for endpoint in endpoints {
        sqlx::query("INSERT INTO webhook_deliveries (id, endpoint_id, body, status, attempts, next_attempt_at, created_at) VALUES (?, ?, ?, 'pending', 0, ?, ?)")
            .bind(format!("wh_{}", Uuid::new_v4().simple()))
            .bind(endpoint)
            .bind(&body)
            .bind(now)
            .bind(now)
            .execute(&mut *connection)
            .await?;
    }
    Ok(())
}

/// Queue the webhook event for a session status, if the status has one.
pub(crate) async fn session_status(
    connection: &mut sqlx::SqliteConnection,
    session_id: &str,
    status: &str,
) -> anyhow::Result<()> {
    match status {
        "requires_action" => {
            enqueue(
                connection,
                "agent.session.action_required",
                json!({"id":session_id,"required_action":{"type":"function_call"}}),
            )
            .await
        }
        "in_progress" | "idle" | "failed" => {
            enqueue(
                connection,
                &format!("agent.session.{status}"),
                json!({"id":session_id}),
            )
            .await
        }
        _ => Ok(()),
    }
}

/// Deliver due webhooks until the API stops. A new event wakes the dispatcher;
/// otherwise it sleeps until the next retry is due, at most [`IDLE_POLL`].
pub(crate) async fn dispatch(state: Arc<State>, mut stopping: watch::Receiver<bool>) {
    while !*stopping.borrow() {
        let wait = match round(&state).await {
            Ok(wait) => wait,
            Err(error) => {
                tracing::error!(error = format!("{error:#}"), "webhook dispatch failed");
                IDLE_POLL
            }
        };
        tokio::select! {
            _ = stopping.changed() => {}
            _ = state.webhook_wake.notified() => {}
            _ = tokio::time::sleep(wait) => {}
        }
    }
}

struct Due {
    id: String,
    endpoint_id: String,
    body: String,
    attempts: i64,
    created_at: i64,
}

/// Attempt the due deliveries, each endpoint's in order and endpoints
/// concurrently, then return how long to wait for the next one.
async fn round(state: &State) -> anyhow::Result<Duration> {
    let pool = &state.store.0;
    let rows: Vec<(String, String, String, i64, i64)> = sqlx::query_as("SELECT id, endpoint_id, body, attempts, created_at FROM webhook_deliveries WHERE status = 'pending' AND next_attempt_at <= ? ORDER BY seq LIMIT ?")
        .bind(now() as i64)
        .bind(BATCH)
        .fetch_all(pool)
        .await?;
    let mut endpoints: Vec<Vec<Due>> = Vec::new();
    for (id, endpoint_id, body, attempts, created_at) in rows {
        let due = Due {
            id,
            endpoint_id,
            body,
            attempts,
            created_at,
        };
        match endpoints
            .iter_mut()
            .find(|group| group[0].endpoint_id == due.endpoint_id)
        {
            Some(group) => group.push(due),
            None => endpoints.push(vec![due]),
        }
    }
    let results: Vec<anyhow::Result<()>> = futures::stream::iter(endpoints)
        .map(|group| deliver(state, group))
        .buffer_unordered(CONCURRENT_ENDPOINTS)
        .collect()
        .await;
    results.into_iter().collect::<anyhow::Result<()>>()?;
    sqlx::query("DELETE FROM webhook_deliveries WHERE status != 'pending' AND created_at < ?")
        .bind(now() as i64 - RETENTION_SECS)
        .execute(pool)
        .await?;
    let next: Option<i64> = sqlx::query_scalar(
        "SELECT min(next_attempt_at) FROM webhook_deliveries WHERE status = 'pending'",
    )
    .fetch_one(pool)
    .await?;
    let wait = next.map_or(IDLE_POLL, |next| {
        Duration::from_secs(u64::try_from(next - now() as i64).unwrap_or_default())
    });
    Ok(wait.min(IDLE_POLL))
}

/// Attempt one endpoint's due deliveries in event order. A failed delivery is
/// retried on its own schedule, so later events can arrive before it. When the
/// receiver cannot be reached, the endpoint's later deliveries wait for that
/// retry instead of each spending an attempt on the same outage.
async fn deliver(state: &State, deliveries: Vec<Due>) -> anyhow::Result<()> {
    let pool = &state.store.0;
    for delivery in deliveries {
        let endpoint = crate::webhooks::endpoint(state, &delivery.endpoint_id)
            .await
            .map_err(|error| anyhow::anyhow!(error.1))?;
        let Some(endpoint) = endpoint else {
            // The endpoint was deleted after this round claimed the delivery.
            sqlx::query("DELETE FROM webhook_deliveries WHERE id = ?")
                .bind(&delivery.id)
                .execute(pool)
                .await?;
            continue;
        };
        let outcome =
            match crate::webhook_delivery::signing_secrets(state, &delivery.endpoint_id).await {
                Ok(secrets) => {
                    crate::webhook_delivery::send(
                        state,
                        endpoint["url"].as_str().unwrap_or_default(),
                        &secrets,
                        &delivery.id,
                        &delivery.body,
                    )
                    .await
                }
                Err(error) => Err(error),
            };
        let attempts = delivery.attempts + 1;
        let (status_code, error) = match outcome {
            Ok(status_code) if (200..300).contains(&status_code) => {
                crate::telemetry::count(
                    crate::telemetry::WEBHOOK_DELIVERY,
                    &[("outcome", "delivered")],
                );
                sqlx::query("UPDATE webhook_deliveries SET status = 'delivered', attempts = ?, last_status_code = ?, last_error = NULL WHERE id = ?")
                    .bind(attempts)
                    .bind(i64::from(status_code))
                    .bind(&delivery.id)
                    .execute(pool)
                    .await?;
                continue;
            }
            Ok(status_code) => (Some(i64::from(status_code)), None),
            Err(error) => (None, Some(error)),
        };
        let now = now() as i64;
        if now - delivery.created_at >= RETRY_WINDOW_SECS {
            tracing::warn!(
                delivery_id = delivery.id,
                endpoint_id = delivery.endpoint_id,
                attempts,
                "gave up webhook delivery"
            );
            crate::telemetry::count(crate::telemetry::WEBHOOK_DELIVERY, &[("outcome", "failed")]);
            sqlx::query("UPDATE webhook_deliveries SET status = 'failed', attempts = ?, last_status_code = ?, last_error = ? WHERE id = ?")
                .bind(attempts)
                .bind(status_code)
                .bind(error.as_ref().map(|error| error.1.as_str()))
                .bind(&delivery.id)
                .execute(pool)
                .await?;
            continue;
        }
        crate::telemetry::count(crate::telemetry::WEBHOOK_DELIVERY, &[("outcome", "retry")]);
        let backoff = FIRST_BACKOFF_SECS
            .saturating_mul(1 << (attempts - 1).clamp(0, 20))
            .min(MAX_BACKOFF_SECS);
        let next_attempt_at = now + backoff;
        sqlx::query("UPDATE webhook_deliveries SET attempts = ?, next_attempt_at = ?, last_status_code = ?, last_error = ? WHERE id = ?")
            .bind(attempts)
            .bind(next_attempt_at)
            .bind(status_code)
            .bind(error.as_ref().map(|error| error.1.as_str()))
            .bind(&delivery.id)
            .execute(pool)
            .await?;
        if error.is_some_and(|error| error.0 == StatusCode::BAD_GATEWAY) {
            sqlx::query("UPDATE webhook_deliveries SET next_attempt_at = max(next_attempt_at, ?) WHERE endpoint_id = ? AND status = 'pending' AND seq > (SELECT seq FROM webhook_deliveries WHERE id = ?)")
                .bind(next_attempt_at)
                .bind(&delivery.endpoint_id)
                .bind(&delivery.id)
                .execute(pool)
                .await?;
            break;
        }
    }
    Ok(())
}
