//! Signed webhook delivery. Requests follow the Standard Webhooks scheme the
//! pinned SDK verifies: a `v1,` HMAC-SHA256 signature over
//! `{webhook-id}.{webhook-timestamp}.{body}` for each active signing secret.
//! Deliveries go only to addresses the egress policy allows, and connect to
//! the addresses that were checked.
use crate::ApiError;
use crate::State;
use crate::contract::invalid;
use crate::contract::now;
use anyhow::Context;
use axum::http::StatusCode;
use axum::http::Uri;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hmac::Hmac;
use hmac::Mac;
use serde_json::Value;
use serde_json::json;
use sha2::Sha256;
use std::net::SocketAddr;
use std::time::Duration;
use uuid::Uuid;

/// How long a receiver has to answer one delivery attempt.
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(/*secs*/ 10);
/// How long a replaced signing secret keeps signing when its rotation asks.
pub(crate) const PREVIOUS_SECRET_SECS: u64 = 24 * 60 * 60;
const USER_AGENT: &str = "codex-agents-api webhooks";

/// A fresh `whsec_` signing secret: 32 random bytes, base64 encoded.
pub(crate) fn new_secret() -> String {
    format!("whsec_{}", STANDARD.encode(rand::random::<[u8; 32]>()))
}

/// The secrets-store name holding an endpoint's signing secrets.
pub(crate) fn secret_name(endpoint_id: &str) -> String {
    format!("WEBHOOK_{}", endpoint_id.to_ascii_uppercase())
}

/// An event body as the webhook guide shows it.
pub(crate) fn event(event_type: &str, data: Value) -> Value {
    json!({"id":format!("evt_{}", Uuid::new_v4().simple()),"object":"event",
        "created_at":now(),"type":event_type,"data":data})
}

/// The secrets that sign a delivery now: the current one, and a replaced one
/// still inside the grace period its rotation asked for.
pub(crate) async fn signing_secrets(
    state: &State,
    endpoint_id: &str,
) -> Result<Vec<String>, ApiError> {
    let stored = state
        .secrets
        .get(secret_name(endpoint_id))
        .await?
        .context("webhook signing secret missing")?;
    let mut secrets = vec![
        stored["current"]
            .as_str()
            .context("webhook signing secret malformed")?
            .to_owned(),
    ];
    if let Some(previous) = stored["previous"].as_str()
        && stored["previous_expires_at"]
            .as_u64()
            .is_some_and(|expires_at| expires_at > now())
    {
        secrets.push(previous.to_owned());
    }
    Ok(secrets)
}

/// The `webhook-signature` header: one `v1,` signature per secret.
fn signature(secrets: &[String], id: &str, timestamp: u64, body: &str) -> anyhow::Result<String> {
    let signed = format!("{id}.{timestamp}.{body}");
    let signatures = secrets
        .iter()
        .map(|secret| {
            let key = STANDARD.decode(secret.trim_start_matches("whsec_"))?;
            let mut mac = Hmac::<Sha256>::new_from_slice(&key)
                .map_err(|_| anyhow::anyhow!("invalid webhook signing key"))?;
            mac.update(signed.as_bytes());
            Ok(format!(
                "v1,{}",
                STANDARD.encode(mac.finalize().into_bytes())
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(signatures.join(" "))
}

/// Check a webhook URL and resolve the addresses a delivery may use. URLs must
/// be https and resolve only to public addresses, unless the operator allows
/// the host, which may then also use plain http.
pub(crate) async fn destination(
    state: &State,
    url: &str,
) -> Result<(String, Vec<SocketAddr>), ApiError> {
    let uri: Uri = url
        .parse()
        .map_err(|_| invalid("url must be an absolute https URL"))?;
    let host = uri
        .host()
        .unwrap_or_default()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_ascii_lowercase();
    let allowed = crate::lock(&state.webhook_hosts).contains(&host);
    let https = uri.scheme_str() == Some("https");
    if host.is_empty()
        || url.len() > 2048
        || !(https || (allowed && uri.scheme_str() == Some("http")))
    {
        return Err(invalid("url must be an absolute https URL"));
    }
    let port = uri.port_u16().unwrap_or(if https { 443 } else { 80 });
    let addresses: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), port))
        .await
        .map_err(|_| invalid(format!("webhook host {host} did not resolve")))?
        .collect();
    if addresses.is_empty()
        || (!allowed
            && !addresses
                .iter()
                .all(|address| crate::mcp::public(&address.ip())))
    {
        return Err(invalid(
            "url resolves to a non-public address; the operator must allow its host",
        ));
    }
    Ok((host, addresses))
}

/// Make one delivery attempt and return the receiver's status code. Redirects
/// are not followed; like any status other than 2xx, they fail the attempt.
pub(crate) async fn send(
    state: &State,
    url: &str,
    secrets: &[String],
    id: &str,
    body: &str,
) -> Result<u16, ApiError> {
    let (host, addresses) = destination(state, url).await?;
    let timestamp = now();
    // Connect only to the addresses just checked, so a DNS change cannot move
    // the delivery elsewhere.
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(ATTEMPT_TIMEOUT)
        .resolve_to_addrs(&host, &addresses)
        .build()
        .map_err(anyhow::Error::from)?;
    let response = client
        .post(url)
        .header("content-type", "application/json")
        .header("user-agent", USER_AGENT)
        .header("webhook-id", id)
        .header("webhook-timestamp", timestamp.to_string())
        .header(
            "webhook-signature",
            signature(secrets, id, timestamp, body)?,
        )
        .body(body.to_owned())
        .send()
        .await
        .map_err(|error| {
            let reason = if error.is_timeout() {
                "the receiver did not answer in time"
            } else if error.is_connect() {
                "could not connect to the receiver"
            } else {
                "the request failed"
            };
            ApiError(
                StatusCode::BAD_GATEWAY,
                format!("webhook delivery failed: {reason}"),
            )
        })?;
    Ok(response.status().as_u16())
}
