//! Vaults: named collections of credentials. Only non-secret metadata is
//! stored in the database and returned by the API.
use crate::ApiError;
use crate::State;
use crate::contract::invalid;
use axum::Json;
use axum::Router;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State as Extract;
use axum::http::StatusCode;
use axum::routing::get;
use serde_json::Value;
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

pub(crate) fn router() -> Router<Arc<State>> {
    Router::new()
        .route("/v1/vaults", get(list_vaults).post(create_vault))
        .route(
            "/v1/vaults/{vault_id}",
            get(retrieve_vault).delete(delete_vault),
        )
        .route(
            "/v1/vaults/{vault_id}/credentials",
            get(crate::credentials::list_credentials).post(crate::credentials::create_credential),
        )
        .route(
            "/v1/vaults/{vault_id}/credentials/{credential_id}",
            get(crate::credentials::retrieve_credential)
                .post(crate::credentials::rotate_credential)
                .delete(crate::credentials::delete_credential),
        )
}

pub(crate) fn not_found(what: &str) -> ApiError {
    ApiError(StatusCode::NOT_FOUND, format!("{what} not found"))
}

pub(crate) fn name(value: Option<&Value>, required: bool) -> Result<Option<String>, ApiError> {
    match value {
        None | Some(Value::Null) if !required => Ok(None),
        Some(Value::String(name)) if (1..=256).contains(&name.trim().len()) => {
            Ok(Some(name.trim().to_owned()))
        }
        _ => Err(invalid(
            "name must contain 1 to 256 UTF-8 bytes after trimming",
        )),
    }
}

/// Parse `after`, `limit` (clamped to 1..=100), `order`, and a `status` filter
/// given as `status=` or repeated `status[]=`. Vaults and credentials are
/// always active, so a filter without `active` matches nothing.
fn page(query: &[(String, String)]) -> Result<(Option<String>, i64, bool, bool), ApiError> {
    let mut after = None;
    let mut limit = 20;
    let mut ascending = false;
    let mut statuses = Vec::new();
    for (key, value) in query {
        match key.as_str() {
            "after" => after = Some(value.clone()),
            "limit" => {
                limit = value
                    .parse::<i64>()
                    .map_err(|_| invalid("limit must be an integer"))?
                    .clamp(1, 100);
            }
            "order" => {
                ascending = match value.as_str() {
                    "asc" => true,
                    "desc" => false,
                    _ => return Err(invalid("order must be asc or desc")),
                };
            }
            "status" | "status[]" => match value.as_str() {
                "active" | "archived" => statuses.push(value.clone()),
                _ => return Err(invalid("status must be active or archived")),
            },
            _ => return Err(invalid(format!("unknown query parameter {key}"))),
        }
    }
    let active = statuses.is_empty() || statuses.iter().any(|status| status == "active");
    Ok((after, limit, ascending, active))
}

/// List vaults, or one vault's credentials when `vault_id` is set, in
/// creation order.
pub(crate) async fn list(
    state: &State,
    vault_id: Option<&str>,
    query: &[(String, String)],
) -> Result<Json<Value>, ApiError> {
    let (after, limit, ascending, active) = page(query)?;
    let pool = &state.store.0;
    let cursor: Option<i64> = match &after {
        Some(after) => Some(
            match vault_id {
                None => {
                    sqlx::query_scalar("SELECT created_seq FROM vaults WHERE id = ?").bind(after)
                }
                Some(vault_id) => sqlx::query_scalar(
                    "SELECT created_seq FROM vault_credentials WHERE id = ? AND vault_id = ?",
                )
                .bind(after)
                .bind(vault_id),
            }
            .fetch_optional(pool)
            .await
            .map_err(anyhow::Error::from)?
            .ok_or_else(|| invalid("invalid after cursor"))?,
        ),
        None => None,
    };
    let start = cursor.unwrap_or(if ascending { 0 } else { i64::MAX });
    let rows: Vec<String> = match (active, vault_id) {
        (false, _) => Vec::new(),
        (true, None) => sqlx::query_scalar(if ascending {
            "SELECT data FROM vaults WHERE created_seq > ? ORDER BY created_seq ASC LIMIT ?"
        } else {
            "SELECT data FROM vaults WHERE created_seq < ? ORDER BY created_seq DESC LIMIT ?"
        })
        .bind(start)
        .bind(limit + 1)
        .fetch_all(pool)
        .await
        .map_err(anyhow::Error::from)?,
        (true, Some(vault_id)) => sqlx::query_scalar(if ascending {
            "SELECT data FROM vault_credentials WHERE vault_id = ? AND created_seq > ? ORDER BY created_seq ASC LIMIT ?"
        } else {
            "SELECT data FROM vault_credentials WHERE vault_id = ? AND created_seq < ? ORDER BY created_seq DESC LIMIT ?"
        })
        .bind(vault_id)
        .bind(start)
        .bind(limit + 1)
        .fetch_all(pool)
        .await
        .map_err(anyhow::Error::from)?,
    };
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

pub(crate) async fn vault(state: &State, vault_id: &str) -> Result<Option<Value>, ApiError> {
    let data: Option<String> = sqlx::query_scalar("SELECT data FROM vaults WHERE id = ?")
        .bind(vault_id)
        .fetch_optional(&state.store.0)
        .await
        .map_err(anyhow::Error::from)?;
    Ok(data
        .map(|data| serde_json::from_str(&data))
        .transpose()
        .map_err(anyhow::Error::from)?)
}

async fn create_vault(
    Extract(state): Extract<Arc<State>>,
    Json(mut body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let fields = body
        .as_object_mut()
        .ok_or_else(|| invalid("vault must be an object"))?;
    if let Some(key) = fields
        .keys()
        .find(|key| !matches!(key.as_str(), "name" | "metadata"))
    {
        return Err(invalid(format!("unknown vault field {key}")));
    }
    let name = name(fields.get("name"), /*required*/ false)?;
    let metadata = crate::configuration::metadata(fields.remove("metadata").unwrap_or_default())?;
    let vault = json!({"id":format!("vault_{}", Uuid::new_v4().simple()),"object":"vault",
        "created_at":crate::contract::now(),"metadata":metadata,"name":name});
    sqlx::query("INSERT INTO vaults (id, data) VALUES (?, ?)")
        .bind(vault["id"].as_str())
        .bind(vault.to_string())
        .execute(&state.store.0)
        .await
        .map_err(anyhow::Error::from)?;
    Ok(Json(vault))
}

async fn list_vaults(
    Extract(state): Extract<Arc<State>>,
    Query(query): Query<Vec<(String, String)>>,
) -> Result<Json<Value>, ApiError> {
    list(&state, /*vault_id*/ None, &query).await
}

async fn retrieve_vault(
    Extract(state): Extract<Arc<State>>,
    Path(vault_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    vault(&state, &vault_id)
        .await?
        .map(Json)
        .ok_or_else(|| not_found("vault"))
}

/// Delete a vault and all its credentials. Sessions that already snapshotted
/// a credential keep using it, as the guide documents.
async fn delete_vault(
    Extract(state): Extract<Arc<State>>,
    Path(vault_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let mut tx = state.store.0.begin().await.map_err(anyhow::Error::from)?;
    let credentials: Vec<String> =
        sqlx::query_scalar("DELETE FROM vault_credentials WHERE vault_id = ? RETURNING id")
            .bind(&vault_id)
            .fetch_all(&mut *tx)
            .await
            .map_err(anyhow::Error::from)?;
    let deleted = sqlx::query("DELETE FROM vaults WHERE id = ?")
        .bind(&vault_id)
        .execute(&mut *tx)
        .await
        .map_err(anyhow::Error::from)?
        .rows_affected();
    if deleted == 0 {
        return Err(not_found("vault"));
    }
    tx.commit().await.map_err(anyhow::Error::from)?;
    state
        .secrets
        .discard(
            credentials
                .iter()
                .map(|id| crate::secrets::credential_name(id))
                .collect(),
        )
        .await;
    Ok(Json(
        json!({"id":vault_id,"object":"vault.deleted","deleted":true}),
    ))
}
