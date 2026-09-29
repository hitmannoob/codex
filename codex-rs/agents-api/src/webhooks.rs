//! Webhook endpoints: where session events are delivered, managed through the
//! API the pinned SDK exposes as `client.webhooks`. An endpoint's signing
//! secret lives in the encrypted secrets store and is returned only when the
//! endpoint is created or its secret rotated.
use crate::ApiError;
use crate::State;
use crate::contract::invalid;
use crate::contract::now;
use crate::webhook_delivery::PREVIOUS_SECRET_SECS;
use crate::webhook_delivery::secret_name;
use anyhow::Context;
use axum::Json;
use axum::Router;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State as Extract;
use axum::routing::get;
use axum::routing::post;
use serde_json::Map;
use serde_json::Value;
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

/// The Agents API events an endpoint can subscribe to, as the guide lists them.
pub(crate) const EVENT_TYPES: [&str; 5] = [
    "agent.session.created",
    "agent.session.action_required",
    "agent.session.in_progress",
    "agent.session.idle",
    "agent.session.failed",
];

pub(crate) fn router() -> Router<Arc<State>> {
    Router::new()
        .route("/v1/webhook_endpoints", get(list).post(create))
        .route(
            "/v1/webhook_endpoints/{id}",
            get(retrieve).post(update).delete(delete),
        )
        .route(
            "/v1/webhook_endpoints/{id}/rotate_secret",
            post(rotate_secret),
        )
        .route("/v1/webhook_endpoints/{id}/test", post(test))
        .route("/v1/webhook_event_types", get(event_types))
}

/// The request's fields, when it is an object with only `allowed` keys.
fn fields<'a>(body: &'a Value, allowed: &[&str]) -> Result<&'a Map<String, Value>, ApiError> {
    let fields = body
        .as_object()
        .ok_or_else(|| invalid("request body must be an object"))?;
    match fields.keys().find(|key| !allowed.contains(&key.as_str())) {
        Some(key) => Err(invalid(format!("unknown field {key}"))),
        None => Ok(fields),
    }
}

/// Validate `event_types`, dropping repeats and keeping their order.
fn event_type_list(value: Option<&Value>) -> Result<Vec<String>, ApiError> {
    let listed = value
        .and_then(Value::as_array)
        .filter(|listed| !listed.is_empty())
        .ok_or_else(|| invalid("event_types must list at least one event type"))?;
    let mut event_types: Vec<String> = Vec::new();
    for event_type in listed {
        let event_type = event_type
            .as_str()
            .filter(|event_type| EVENT_TYPES.contains(event_type))
            .ok_or_else(|| {
                invalid(format!(
                    "event_types accepts only {}",
                    EVENT_TYPES.join(", ")
                ))
            })?;
        if !event_types.iter().any(|seen| seen == event_type) {
            event_types.push(event_type.to_owned());
        }
    }
    Ok(event_types)
}

async fn url(state: &State, value: Option<&Value>) -> Result<String, ApiError> {
    let url = value
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("url must be an absolute https URL"))?;
    crate::webhook_delivery::destination(state, url).await?;
    Ok(url.to_owned())
}

/// A masked form of a signing secret that identifies it without revealing it.
fn hint(secret: &str) -> String {
    format!("whsec_...{}", &secret[secret.len().saturating_sub(4)..])
}

fn not_found() -> ApiError {
    crate::vaults::not_found("webhook endpoint")
}

pub(crate) async fn endpoint(state: &State, id: &str) -> Result<Option<Value>, ApiError> {
    let data: Option<String> =
        sqlx::query_scalar("SELECT data FROM webhook_endpoints WHERE id = ?")
            .bind(id)
            .fetch_optional(&state.store.0)
            .await
            .map_err(anyhow::Error::from)?;
    Ok(data
        .map(|data| serde_json::from_str(&data))
        .transpose()
        .map_err(anyhow::Error::from)?)
}

async fn save(state: &State, endpoint: &Value) -> Result<(), ApiError> {
    sqlx::query("UPDATE webhook_endpoints SET data = ? WHERE id = ?")
        .bind(endpoint.to_string())
        .bind(endpoint["id"].as_str())
        .execute(&state.store.0)
        .await
        .map_err(anyhow::Error::from)?;
    Ok(())
}

async fn create(
    Extract(state): Extract<Arc<State>>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let fields = fields(&body, &["name", "url", "event_types"])?;
    let name = crate::vaults::name(fields.get("name"), /*required*/ true)?;
    let url = url(&state, fields.get("url")).await?;
    let event_types = event_type_list(fields.get("event_types"))?;
    let id = format!("we_{}", Uuid::new_v4().simple());
    let secret = crate::webhook_delivery::new_secret();
    // Store the secret first: an endpoint must never lack its secret.
    state
        .secrets
        .set(
            secret_name(&id),
            json!({"current":secret,"previous":null,"previous_expires_at":null}),
        )
        .await?;
    let created_at = now();
    let mut endpoint = json!({"id":id,"object":"webhook_endpoint","created_at":created_at,
        "updated_at":created_at,"name":name,"url":url,"event_types":event_types,"signing_secret_hint":hint(&secret)});
    sqlx::query("INSERT INTO webhook_endpoints (id, data) VALUES (?, ?)")
        .bind(&id)
        .bind(endpoint.to_string())
        .execute(&state.store.0)
        .await
        .map_err(anyhow::Error::from)?;
    endpoint["signing_secret"] = json!(secret);
    Ok(Json(endpoint))
}

async fn retrieve(
    Extract(state): Extract<Arc<State>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    endpoint(&state, &id).await?.map(Json).ok_or_else(not_found)
}

/// Replace the supplied fields. An update that changes nothing keeps
/// `updated_at`, as the SDK documents.
async fn update(
    Extract(state): Extract<Arc<State>>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let fields = fields(&body, &["name", "url", "event_types"])?;
    let mut endpoint = endpoint(&state, &id).await?.ok_or_else(not_found)?;
    let before = endpoint.clone();
    if let Some(name) = fields.get("name") {
        endpoint["name"] = json!(crate::vaults::name(Some(name), /*required*/ true)?);
    }
    if let Some(value) = fields.get("url") {
        endpoint["url"] = json!(url(&state, Some(value)).await?);
    }
    if let Some(value) = fields.get("event_types") {
        endpoint["event_types"] = json!(event_type_list(Some(value))?);
    }
    if endpoint != before {
        endpoint["updated_at"] = json!(now());
        save(&state, &endpoint).await?;
    }
    Ok(Json(endpoint))
}

/// List endpoints newest first, `limit` (default 20, at most 100) at a time.
async fn list(
    Extract(state): Extract<Arc<State>>,
    Query(query): Query<Vec<(String, String)>>,
) -> Result<Json<Value>, ApiError> {
    let mut after = None;
    let mut limit = 20;
    for (key, value) in &query {
        match key.as_str() {
            "after" => after = Some(value.clone()),
            "limit" => {
                limit = value
                    .parse::<i64>()
                    .map_err(|_| invalid("limit must be an integer"))?
                    .clamp(1, 100);
            }
            _ => return Err(invalid(format!("unknown query parameter {key}"))),
        }
    }
    let pool = &state.store.0;
    let start: i64 = match after {
        Some(after) => sqlx::query_scalar("SELECT created_seq FROM webhook_endpoints WHERE id = ?")
            .bind(after)
            .fetch_optional(pool)
            .await
            .map_err(anyhow::Error::from)?
            .ok_or_else(|| invalid("invalid after cursor"))?,
        None => i64::MAX,
    };
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT data FROM webhook_endpoints WHERE created_seq < ? ORDER BY created_seq DESC LIMIT ?",
    )
    .bind(start)
    .bind(limit + 1)
    .fetch_all(pool)
    .await
    .map_err(anyhow::Error::from)?;
    let has_more = rows.len() as i64 > limit;
    let data = rows
        .iter()
        .take(limit as usize)
        .map(|row| serde_json::from_str::<Value>(row))
        .collect::<Result<Vec<_>, _>>()
        .map_err(anyhow::Error::from)?;
    Ok(Json(
        json!({"object":"list","first_id":data.first().map(|v| &v["id"]),
        "last_id":data.last().map(|v| &v["id"]),"data":data,"has_more":has_more}),
    ))
}

async fn delete(
    Extract(state): Extract<Arc<State>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    if endpoint(&state, &id).await?.is_none() {
        return Err(not_found());
    }
    // Remove the secret first, so a failure leaves the endpoint intact.
    state.secrets.delete(vec![secret_name(&id)]).await?;
    let mut tx = state.store.0.begin().await.map_err(anyhow::Error::from)?;
    for statement in [
        "DELETE FROM webhook_deliveries WHERE endpoint_id = ? AND status = 'pending'",
        "DELETE FROM webhook_endpoints WHERE id = ?",
    ] {
        sqlx::query(statement)
            .bind(&id)
            .execute(&mut *tx)
            .await
            .map_err(anyhow::Error::from)?;
    }
    tx.commit().await.map_err(anyhow::Error::from)?;
    Ok(Json(
        json!({"id":id,"object":"webhook_endpoint.deleted","deleted":true}),
    ))
}

/// Replace the signing secret. The previous secret keeps signing for 24 hours
/// when asked, so receivers can switch over without rejecting deliveries.
async fn rotate_secret(
    Extract(state): Extract<Arc<State>>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let fields = fields(&body, &["keep_old_secret_active_for_24_hours"])?;
    let keep = match fields.get("keep_old_secret_active_for_24_hours") {
        None | Some(Value::Null) => false,
        Some(Value::Bool(keep)) => *keep,
        Some(_) => {
            return Err(invalid(
                "keep_old_secret_active_for_24_hours must be a boolean",
            ));
        }
    };
    let mut endpoint = endpoint(&state, &id).await?.ok_or_else(not_found)?;
    let stored = state
        .secrets
        .get(secret_name(&id))
        .await?
        .context("webhook signing secret missing")?;
    let secret = crate::webhook_delivery::new_secret();
    let (previous, expires_at) = if keep {
        (
            stored["current"].clone(),
            json!(now() + PREVIOUS_SECRET_SECS),
        )
    } else {
        (Value::Null, Value::Null)
    };
    state
        .secrets
        .set(
            secret_name(&id),
            json!({"current":secret,"previous":previous,"previous_expires_at":expires_at}),
        )
        .await?;
    endpoint["signing_secret_hint"] = json!(hint(&secret));
    endpoint["updated_at"] = json!(now());
    save(&state, &endpoint).await?;
    endpoint["signing_secret"] = json!(secret);
    Ok(Json(endpoint))
}

/// Send one signed sample event now and report the receiver's status code.
/// Test deliveries are not retried.
async fn test(
    Extract(state): Extract<Arc<State>>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let fields = fields(&body, &["event_type"])?;
    let event_type = fields
        .get("event_type")
        .and_then(Value::as_str)
        .filter(|event_type| EVENT_TYPES.contains(event_type))
        .ok_or_else(|| {
            invalid(format!(
                "event_type must be one of {}",
                EVENT_TYPES.join(", ")
            ))
        })?;
    let endpoint = endpoint(&state, &id).await?.ok_or_else(not_found)?;
    let mut data = json!({"id":"sess_test"});
    if event_type == "agent.session.action_required" {
        data["required_action"] = json!({"type":"function_call"});
    }
    let body = crate::webhook_delivery::event(event_type, data).to_string();
    let secrets = crate::webhook_delivery::signing_secrets(&state, &id).await?;
    let status_code = crate::webhook_delivery::send(
        &state,
        endpoint["url"].as_str().unwrap_or_default(),
        &secrets,
        &format!("wh_{}", Uuid::new_v4().simple()),
        &body,
    )
    .await?;
    Ok(Json(
        json!({"object":"webhook_endpoint.test","webhook_endpoint_id":id,
        "event_type":event_type,"status_code":status_code,"success":true}),
    ))
}

async fn event_types() -> Json<Value> {
    Json(json!({"object":"list","data":EVENT_TYPES}))
}
