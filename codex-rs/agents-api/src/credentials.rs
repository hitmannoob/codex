//! Vault credentials and the MCP secrets each session snapshots from them.
//! Credential metadata lives in the database; secret values live only in the
//! encrypted secrets store. A session snapshots its MCP secrets when it is
//! created. Rotating or deleting the vault credential afterwards
//! does not change a session that already holds a snapshot, as the vaults
//! guide documents; a new session picks up the new secret.
use crate::ApiError;
use crate::State;
use crate::contract::invalid;
use crate::resources::AgentConfig;
use axum::Json;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State as Extract;
use serde_json::Value;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

/// Validate a session's `vault_ids`: each must name an existing vault.
pub(crate) async fn vaults(
    state: &State,
    vault_ids: Option<Vec<String>>,
) -> Result<Vec<String>, ApiError> {
    let vault_ids = vault_ids.unwrap_or_default();
    if vault_ids.len() > 32 {
        return Err(invalid("vault_ids accepts at most 32 vaults"));
    }
    for vault_id in &vault_ids {
        let known: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM vaults WHERE id = ?)")
            .bind(vault_id)
            .fetch_one(&state.store.0)
            .await
            .map_err(anyhow::Error::from)?;
        if !known {
            return Err(invalid(format!("vault {vault_id} not found")));
        }
    }
    Ok(vault_ids)
}

/// Pick each MCP server's credential from the session's vaults: the one named
/// by `credential_id`, or else the only credential for the server's URL.
/// Returns (server label, credential ID) pairs.
pub(crate) async fn resolve(
    state: &State,
    config: &AgentConfig,
    vault_ids: &[String],
) -> Result<Vec<(String, String)>, ApiError> {
    let vaults = json!(vault_ids).to_string();
    let available: Vec<(String, String)> = sqlx::query_as(
        "SELECT id, json_extract(data, '$.auth.mcp_server_url') FROM vault_credentials WHERE vault_id IN (SELECT value FROM json_each(?)) ORDER BY created_seq",
    )
    .bind(&vaults)
    .fetch_all(&state.store.0)
    .await
    .map_err(anyhow::Error::from)?;
    let mut resolved = Vec::new();
    for (label, url, credential_id) in crate::mcp::credential_targets(config) {
        let chosen = match credential_id {
            Some(credential_id) => {
                if !available.iter().any(|(id, _)| id == credential_id) {
                    return Err(invalid(format!(
                        "credential {credential_id} for MCP server {label} is not in the session's vaults"
                    )));
                }
                Some(credential_id.to_owned())
            }
            None => {
                let mut matches = available.iter().filter(|(_, server)| server == url);
                match (matches.next(), matches.next()) {
                    (None, _) => None,
                    (Some((id, _)), None) => Some(id.clone()),
                    (Some(_), Some(_)) => {
                        return Err(invalid(format!(
                            "several credentials match MCP server {label}; set credential_id"
                        )));
                    }
                }
            }
        };
        if let Some(credential_id) = chosen {
            resolved.push((label.to_owned(), credential_id));
        }
    }
    Ok(resolved)
}

fn snapshot_name(session_id: &str, label: &str) -> String {
    let hex = |value: &str| {
        value
            .bytes()
            .map(|byte| format!("{byte:02X}"))
            .collect::<String>()
    };
    format!("SESSION_{}_{}", hex(session_id), hex(label))
}

/// Copy each resolved credential's secret into the session's own snapshot.
pub(crate) async fn snapshot(
    state: &State,
    session_id: &str,
    resolved: Vec<(String, String)>,
) -> Result<(), ApiError> {
    for (label, credential_id) in resolved {
        let secret = state
            .secrets
            .get(crate::secrets::credential_name(&credential_id))
            .await?
            .ok_or_else(|| invalid(format!("credential {credential_id} has no stored secret")))?;
        state
            .secrets
            .set(snapshot_name(session_id, &label), secret)
            .await?;
        sqlx::query("INSERT INTO session_credentials (session_id, server_label, credential_id) VALUES (?, ?, ?)")
            .bind(session_id)
            .bind(&label)
            .bind(&credential_id)
            .execute(&state.store.0)
            .await
            .map_err(anyhow::Error::from)?;
    }
    Ok(())
}

/// The bearer tokens a session snapshotted, by MCP server label.
pub(crate) async fn load(
    state: &State,
    session_id: &str,
) -> Result<BTreeMap<String, String>, ApiError> {
    let labels: Vec<String> =
        sqlx::query_scalar("SELECT server_label FROM session_credentials WHERE session_id = ?")
            .bind(session_id)
            .fetch_all(&state.store.0)
            .await
            .map_err(anyhow::Error::from)?;
    let mut tokens = BTreeMap::new();
    for label in labels {
        let secret = state
            .secrets
            .get(snapshot_name(session_id, &label))
            .await?
            .ok_or_else(|| anyhow::anyhow!("session credential snapshot missing"))?;
        let token = secret["token"]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| anyhow::anyhow!("session credential snapshot is malformed"))?;
        tokens.insert(label, token);
    }
    Ok(tokens)
}

/// Remove a deleted session's snapshots.
pub(crate) async fn forget(
    state: &State,
    session_id: &str,
    labels: Vec<String>,
) -> Result<(), ApiError> {
    if labels.is_empty() {
        return Ok(());
    }
    state
        .secrets
        .delete(
            labels
                .iter()
                .map(|label| snapshot_name(session_id, label))
                .collect(),
        )
        .await
}

pub(crate) async fn credential(
    state: &State,
    vault_id: &str,
    credential_id: &str,
) -> Result<Option<Value>, ApiError> {
    let data: Option<String> =
        sqlx::query_scalar("SELECT data FROM vault_credentials WHERE id = ? AND vault_id = ?")
            .bind(credential_id)
            .bind(vault_id)
            .fetch_optional(&state.store.0)
            .await
            .map_err(anyhow::Error::from)?;
    Ok(data
        .map(|data| serde_json::from_str(&data))
        .transpose()
        .map_err(anyhow::Error::from)?)
}

/// Split a credential's `auth` into its public description and its secret
/// values. Only MCP credentials are usable here: environment-variable
/// credentials apply to OpenAI-hosted environments, and OAuth refresh is not
/// performed by this service.
fn parse_auth(
    auth: &Value,
    http_hosts: &std::collections::HashSet<String>,
) -> Result<(Value, Value), ApiError> {
    let fields = auth
        .as_object()
        .ok_or_else(|| invalid("auth must be an object"))?;
    let secret = |field: &str| -> Result<String, ApiError> {
        match fields.get(field) {
            Some(Value::String(value))
                if !value.is_empty() && !value.contains(['\r', '\n', '\0']) =>
            {
                Ok(value.clone())
            }
            _ => Err(invalid(format!(
                "auth.{field} must be a nonempty single-line string"
            ))),
        }
    };
    let server_url = || -> Result<String, ApiError> {
        let url = fields
            .get("mcp_server_url")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("auth.mcp_server_url is required"))?;
        let uri: axum::http::Uri = url
            .parse()
            .map_err(|_| invalid("auth.mcp_server_url must be an https URL"))?;
        // Plain http is allowed only for hosts the operator allows for MCP.
        let host = uri.host().unwrap_or_default().to_ascii_lowercase();
        let secure = uri.scheme_str() == Some("https")
            || (uri.scheme_str() == Some("http") && http_hosts.contains(&host));
        if !secure || uri.host().is_none() || url.len() > 2048 {
            return Err(invalid("auth.mcp_server_url must be an https URL"));
        }
        Ok(url.to_owned())
    };
    let allowed = |keys: &[&str]| {
        fields
            .keys()
            .find(|key| !keys.contains(&key.as_str()))
            .map_or(Ok(()), |key| {
                Err(invalid(format!("unknown auth field {key}")))
            })
    };
    match fields.get("type").and_then(Value::as_str) {
        Some("static_bearer") => {
            allowed(&["type", "token", "mcp_server_url"])?;
            Ok((
                json!({"type":"static_bearer","mcp_server_url":server_url()?}),
                json!({"token":secret("token")?}),
            ))
        }
        Some("mcp_oauth") => {
            allowed(&[
                "type",
                "access_token",
                "mcp_server_url",
                "expires_at",
                "refresh",
            ])?;
            if fields
                .get("refresh")
                .is_some_and(|refresh| !refresh.is_null())
            {
                return Err(invalid("OAuth refresh is not implemented"));
            }
            let expires_at = match fields.get("expires_at") {
                None | Some(Value::Null) => Value::Null,
                Some(Value::String(at)) if !at.is_empty() && at.len() <= 64 => json!(at),
                Some(_) => return Err(invalid("auth.expires_at must be an RFC 3339 timestamp")),
            };
            Ok((
                json!({"type":"mcp_oauth","mcp_server_url":server_url()?,"expires_at":expires_at,"refresh":null}),
                json!({"token":secret("access_token")?}),
            ))
        }
        Some("environment_variable") => Err(invalid(
            "environment_variable credentials apply only to OpenAI-hosted environments, which are not implemented",
        )),
        _ => Err(invalid(
            "auth.type must be static_bearer, mcp_oauth, or environment_variable",
        )),
    }
}

pub(crate) async fn create_credential(
    Extract(state): Extract<Arc<State>>,
    Path(vault_id): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let fields = body
        .as_object()
        .ok_or_else(|| invalid("credential must be an object"))?;
    if let Some(key) = fields
        .keys()
        .find(|key| !matches!(key.as_str(), "name" | "auth"))
    {
        return Err(invalid(format!("unknown credential field {key}")));
    }
    let name = crate::vaults::name(fields.get("name"), /*required*/ true)?;
    let hosts = crate::lock(&state.mcp_hosts).clone();
    let (auth, secret) = parse_auth(fields.get("auth").unwrap_or(&Value::Null), &hosts)?;
    if crate::vaults::vault(&state, &vault_id).await?.is_none() {
        return Err(crate::vaults::not_found("vault"));
    }
    let now = crate::contract::now();
    let credential = json!({"id":format!("cred_{}", Uuid::new_v4().simple()),"object":"vault.credential",
        "vault_id":vault_id,"name":name,"auth":auth,"created_at":now,"updated_at":now});
    let id = credential["id"].as_str().unwrap_or_default().to_owned();
    // Store the secret first: a credential row must never lack its secret.
    state
        .secrets
        .set(crate::secrets::credential_name(&id), secret)
        .await?;
    let inserted = sqlx::query("INSERT INTO vault_credentials (id, vault_id, data) SELECT ?, ?, ? WHERE EXISTS (SELECT 1 FROM vaults WHERE id = ?)")
        .bind(&id).bind(&vault_id).bind(credential.to_string()).bind(&vault_id)
        .execute(&state.store.0).await.map_err(anyhow::Error::from)?
        .rows_affected();
    if inserted == 0 {
        state
            .secrets
            .delete(vec![crate::secrets::credential_name(&id)])
            .await?;
        return Err(crate::vaults::not_found("vault"));
    }
    Ok(Json(credential))
}

pub(crate) async fn list_credentials(
    Extract(state): Extract<Arc<State>>,
    Path(vault_id): Path<String>,
    Query(query): Query<Vec<(String, String)>>,
) -> Result<Json<Value>, ApiError> {
    if crate::vaults::vault(&state, &vault_id).await?.is_none() {
        return Err(crate::vaults::not_found("vault"));
    }
    crate::vaults::list(&state, Some(&vault_id), &query).await
}

pub(crate) async fn retrieve_credential(
    Extract(state): Extract<Arc<State>>,
    Path((vault_id, credential_id)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    credential(&state, &vault_id, &credential_id)
        .await?
        .map(Json)
        .ok_or_else(|| crate::vaults::not_found("credential"))
}

/// Replace a credential's secret without changing its identity. Sessions that
/// already snapshotted the old secret keep it until they are recreated.
pub(crate) async fn rotate_credential(
    Extract(state): Extract<Arc<State>>,
    Path((vault_id, credential_id)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let fields = body
        .as_object()
        .ok_or_else(|| invalid("credential update must be an object"))?;
    if let Some(key) = fields.keys().find(|key| key.as_str() != "auth") {
        return Err(invalid(format!("unknown credential field {key}")));
    }
    let rotation = fields
        .get("auth")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("auth is required"))?;
    let mut credential = credential(&state, &vault_id, &credential_id)
        .await?
        .ok_or_else(|| crate::vaults::not_found("credential"))?;
    let kind = credential["auth"]["type"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    if rotation.get("type").and_then(Value::as_str) != Some(kind.as_str()) {
        return Err(invalid("auth.type must match the credential's type"));
    }
    if rotation
        .get("refresh")
        .is_some_and(|refresh| !refresh.is_null())
    {
        return Err(invalid("OAuth refresh is not implemented"));
    }
    // Re-validate through the creation path using the stored public values.
    let mut replacement = credential["auth"].clone();
    for (key, value) in rotation {
        replacement[key] = value.clone();
    }
    if kind == "mcp_oauth" {
        replacement
            .as_object_mut()
            .map(|auth| auth.remove("refresh"));
        // A new access token clears an expiry that was not restated.
        if rotation.contains_key("access_token") && !rotation.contains_key("expires_at") {
            replacement["expires_at"] = Value::Null;
        }
        if !rotation.contains_key("access_token") {
            let stored = state
                .secrets
                .get(crate::secrets::credential_name(&credential_id))
                .await?
                .ok_or_else(|| anyhow::anyhow!("credential secret missing"))?;
            replacement["access_token"] = stored["token"].clone();
        }
    }
    let hosts = crate::lock(&state.mcp_hosts).clone();
    let (auth, secret) = parse_auth(&replacement, &hosts)?;
    state
        .secrets
        .set(crate::secrets::credential_name(&credential_id), secret)
        .await?;
    credential["auth"] = auth;
    credential["updated_at"] = json!(crate::contract::now());
    sqlx::query("UPDATE vault_credentials SET data = ? WHERE id = ?")
        .bind(credential.to_string())
        .bind(&credential_id)
        .execute(&state.store.0)
        .await
        .map_err(anyhow::Error::from)?;
    Ok(Json(credential))
}

pub(crate) async fn delete_credential(
    Extract(state): Extract<Arc<State>>,
    Path((vault_id, credential_id)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    let deleted = sqlx::query("DELETE FROM vault_credentials WHERE id = ? AND vault_id = ?")
        .bind(&credential_id)
        .bind(&vault_id)
        .execute(&state.store.0)
        .await
        .map_err(anyhow::Error::from)?
        .rows_affected();
    if deleted == 0 {
        return Err(crate::vaults::not_found("credential"));
    }
    state
        .secrets
        .delete(vec![crate::secrets::credential_name(&credential_id)])
        .await?;
    Ok(Json(
        json!({"id":credential_id,"object":"vault.credential.deleted","deleted":true}),
    ))
}
