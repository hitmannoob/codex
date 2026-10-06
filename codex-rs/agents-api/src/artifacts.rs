//! Session artifacts (`/v1/agents/sessions/{id}/artifacts`). The service
//! publishes none: artifacts are the `/workspace/outputs` files of
//! OpenAI-hosted environments, and the guide states that files from
//! self-hosted environments are not published through the Artifacts API. A
//! self-hosted caller reads its own workspace, or uses environment files. The
//! routes still check the session and parameters, so clients see the
//! documented shapes: an empty list, and 404 for any artifact.
use crate::ApiError;
use crate::State;
use crate::contract::invalid;
use axum::Json;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State as Extract;
use axum::http::StatusCode;
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;
use std::sync::Arc;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ListParams {
    after: Option<String>,
    environment_id: Option<String>,
    limit: Option<u32>,
    order: Option<String>,
}

pub(crate) async fn list(
    Extract(state): Extract<Arc<State>>,
    Path(session_id): Path<String>,
    Query(params): Query<ListParams>,
) -> Result<Json<Value>, ApiError> {
    crate::records::session(&state, &session_id).await?;
    if !(1..=100).contains(&params.limit.unwrap_or(/*default*/ 20))
        || !matches!(params.order.as_deref(), None | Some("asc" | "desc"))
        || params.environment_id.as_deref() == Some("")
    {
        return Err(invalid("invalid pagination parameters"));
    }
    // There is no artifact for a cursor to name.
    if params.after.is_some() {
        return Err(invalid("invalid after cursor"));
    }
    Ok(Json(
        json!({"object": "list", "first_id": null, "last_id": null, "data": [], "has_more": false}),
    ))
}

/// Retrieve, download, and delete: the session must exist, and no artifact
/// does.
pub(crate) async fn missing(
    Extract(state): Extract<Arc<State>>,
    Path((session_id, _artifact_id)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    crate::records::session(&state, &session_id).await?;
    Err(ApiError(StatusCode::NOT_FOUND, "artifact not found".into()))
}
