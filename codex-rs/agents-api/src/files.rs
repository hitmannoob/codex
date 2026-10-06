//! A minimal Files API (`/v1/files`) for uploads that environment files name
//! by `file_id`. Contents live under `DATA_DIRECTORY/files`, metadata in the
//! database. A file expires only when its upload asks for it; an expired file
//! reads as deleted and is removed on the next upload or start. Uploads stream
//! to disk with a SHA-256 digest that is checked before contents are served.
use crate::ApiError;
use crate::State;
use crate::contract::invalid;
use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State as Extract;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::http::header;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use axum::routing::post;
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;
use sha2::Digest;
use sha2::Sha256;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use uuid::Uuid;

/// The largest upload, and so the largest `file_id` environment file.
pub(crate) const MAX_FILE_BYTES: usize = 50 * 1024 * 1024;
const PURPOSES: [&str; 6] = [
    "assistants",
    "batch",
    "fine-tune",
    "vision",
    "user_data",
    "evals",
];

/// Where file contents are stored.
pub(crate) struct Files(pub(crate) PathBuf);

pub(crate) fn router() -> Router<Arc<State>> {
    Router::new()
        .route("/v1/files", post(create).get(list))
        .route("/v1/files/{id}", get(retrieve).delete(delete))
        .route("/v1/files/{id}/content", get(content))
}

type Row = (String, String, String, i64, i64, Option<i64>);
const COLUMNS: &str = "id, filename, purpose, bytes, created_at, expires_at";
/// Rows that have not expired.
const LIVE: &str = "(expires_at IS NULL OR expires_at > ?)";

fn public((id, filename, purpose, bytes, created_at, expires_at): Row) -> Value {
    json!({"id": id, "object": "file", "bytes": bytes, "created_at": created_at, "filename": filename,
        "purpose": purpose, "status": "processed", "expires_at": expires_at, "status_details": null})
}

fn not_found() -> ApiError {
    ApiError(StatusCode::NOT_FOUND, "file not found".into())
}

async fn row(state: &State, id: &str) -> Result<Row, ApiError> {
    let row: Option<Row> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {COLUMNS} FROM files WHERE id = ? AND {LIVE}"
    )))
    .bind(id)
    .bind(crate::contract::now() as i64)
    .fetch_optional(&state.store.0)
    .await
    .map_err(anyhow::Error::from)?;
    row.ok_or_else(not_found)
}

/// A live file's stored digest, if it has one (uploads before digests did not).
async fn digest(state: &State, id: &str) -> Result<Option<String>, ApiError> {
    let digest: Option<Option<String>> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT sha256 FROM files WHERE id = ? AND {LIVE}"
    )))
    .bind(id)
    .bind(crate::contract::now() as i64)
    .fetch_optional(&state.store.0)
    .await
    .map_err(anyhow::Error::from)?;
    digest.ok_or_else(not_found)
}

fn corrupted() -> ApiError {
    ApiError(
        StatusCode::INTERNAL_SERVER_ERROR,
        "file contents failed their integrity check".into(),
    )
}

/// A live file's contents, checked against its digest, for an environment
/// file's `file_id` source. Uploads are bounded, so this holds at most
/// [`MAX_FILE_BYTES`].
pub(crate) async fn read(state: &State, id: &str) -> Result<Vec<u8>, ApiError> {
    let expected = digest(state, id).await?;
    let contents = tokio::fs::read(state.files.0.join(id))
        .await
        .map_err(|_| not_found())?;
    if expected.is_some_and(|expected| format!("{:x}", Sha256::digest(&contents)) != expected) {
        return Err(corrupted());
    }
    Ok(contents)
}

/// Check a stored file against its digest without holding it in memory.
async fn verify(path: &std::path::Path, expected: &str) -> Result<(), ApiError> {
    let mut file = tokio::fs::File::open(path).await.map_err(|_| not_found())?;
    let mut hasher = Sha256::new();
    let mut chunk = vec![0; 64 * 1024];
    loop {
        let read = file.read(&mut chunk).await.map_err(anyhow::Error::from)?;
        if read == 0 {
            break;
        }
        hasher.update(&chunk[..read]);
    }
    if format!("{:x}", hasher.finalize()) != expected {
        return Err(corrupted());
    }
    Ok(())
}

/// Remove expired files.
async fn expire(state: &State) -> anyhow::Result<()> {
    let expired: Vec<String> = sqlx::query_scalar(
        "DELETE FROM files WHERE expires_at IS NOT NULL AND expires_at <= ? RETURNING id",
    )
    .bind(crate::contract::now() as i64)
    .fetch_all(&state.store.0)
    .await?;
    for id in expired {
        let _ = tokio::fs::remove_file(state.files.0.join(id)).await;
    }
    Ok(())
}

/// At startup, remove expired files and contents left by an interrupted
/// upload or delete.
pub(crate) async fn sweep(state: &State) -> anyhow::Result<()> {
    expire(state).await?;
    let Ok(mut entries) = tokio::fs::read_dir(&state.files.0).await else {
        return Ok(());
    };
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name().to_string_lossy().into_owned();
        let known: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM files WHERE id = ?)")
            .bind(&name)
            .fetch_one(&state.store.0)
            .await?;
        if !known {
            let _ = tokio::fs::remove_file(entry.path()).await;
        }
    }
    Ok(())
}

async fn create(
    Extract(state): Extract<Arc<State>>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, ApiError> {
    let boundary = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(crate::upload::boundary)
        .ok_or_else(|| invalid("uploads must be multipart/form-data with a boundary"))?;
    expire(&state).await?;
    let id = format!("file-{}", Uuid::new_v4().simple());
    tokio::fs::create_dir_all(&state.files.0)
        .await
        .map_err(anyhow::Error::from)?;
    // Received under a temporary name, so an interrupted upload never leaves
    // partial contents under a file's ID; the startup sweep removes it.
    let partial = state.files.0.join(format!("{id}.partial"));
    let received = async {
        let upload =
            crate::upload::receive(body, &boundary, &partial, MAX_FILE_BYTES as u64).await?;
        let purpose = upload
            .fields
            .get("purpose")
            .cloned()
            .ok_or_else(|| invalid("purpose is required"))?;
        if !PURPOSES.contains(&purpose.as_str()) {
            return Err(invalid(format!(
                "purpose must be one of {}",
                PURPOSES.join(", ")
            )));
        }
        if upload.filename.is_empty() || upload.filename.len() > 512 {
            return Err(invalid(
                "the file part needs a filename of at most 512 bytes",
            ));
        }
        let now = crate::contract::now() as i64;
        let expires_at = match (
            upload.fields.get("expires_after[anchor]"),
            upload.fields.get("expires_after[seconds]"),
        ) {
            (None, None) => None,
            (Some(anchor), Some(seconds)) if anchor == "created_at" => {
                let seconds: i64 = seconds
                    .parse()
                    .ok()
                    .filter(|seconds| (3600..=2_592_000).contains(seconds))
                    .ok_or_else(|| {
                        invalid("expires_after.seconds must be between 3600 and 2592000")
                    })?;
                Some(now + seconds)
            }
            _ => {
                return Err(invalid("expires_after needs anchor created_at and seconds"));
            }
        };
        tokio::fs::rename(&partial, state.files.0.join(&id))
            .await
            .map_err(anyhow::Error::from)?;
        Ok((upload, purpose, now, expires_at))
    }
    .await;
    let (upload, purpose, now, expires_at) = match received {
        Ok(received) => received,
        Err(error) => {
            let _ = tokio::fs::remove_file(&partial).await;
            return Err(error);
        }
    };
    let row: Row = (
        id,
        upload.filename,
        purpose,
        upload.size as i64,
        now,
        expires_at,
    );
    sqlx::query("INSERT INTO files (id, filename, purpose, bytes, created_at, expires_at, sha256) VALUES (?, ?, ?, ?, ?, ?, ?)")
        .bind(&row.0).bind(&row.1).bind(&row.2).bind(row.3).bind(row.4).bind(row.5).bind(&upload.sha256)
        .execute(&state.store.0).await.map_err(anyhow::Error::from)?;
    Ok(Json(public(row)))
}

async fn retrieve(
    Extract(state): Extract<Arc<State>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(public(row(&state, &id).await?)))
}

async fn content(
    Extract(state): Extract<Arc<State>>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let expected = digest(&state, &id).await?;
    let path = state.files.0.join(&id);
    if let Some(expected) = expected {
        verify(&path, &expected).await?;
    }
    let file = tokio::fs::File::open(&path)
        .await
        .map_err(|_| not_found())?;
    let length = file.metadata().await.map_err(anyhow::Error::from)?.len();
    Ok((
        [
            (header::CONTENT_TYPE, "application/octet-stream".to_owned()),
            (header::CONTENT_LENGTH, length.to_string()),
        ],
        Body::from_stream(tokio_util::io::ReaderStream::new(file)),
    )
        .into_response())
}

async fn delete(
    Extract(state): Extract<Arc<State>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let deleted = sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM files WHERE id = ? AND {LIVE}"
    )))
    .bind(&id)
    .bind(crate::contract::now() as i64)
    .execute(&state.store.0)
    .await
    .map_err(anyhow::Error::from)?
    .rows_affected();
    if deleted == 0 {
        return Err(not_found());
    }
    // A failure here leaves contents for the startup sweep.
    let _ = tokio::fs::remove_file(state.files.0.join(&id)).await;
    Ok(Json(json!({"id": id, "object": "file", "deleted": true})))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListParams {
    after: Option<String>,
    limit: Option<i64>,
    order: Option<String>,
    purpose: Option<String>,
}

async fn list(
    Extract(state): Extract<Arc<State>>,
    Query(params): Query<ListParams>,
) -> Result<Json<Value>, ApiError> {
    let limit = params.limit.unwrap_or(10_000);
    if !(1..=10_000).contains(&limit) {
        return Err(invalid("limit must be between 1 and 10000"));
    }
    let (direction, comparison) = match params.order.as_deref() {
        None | Some("desc") => ("DESC", "<"),
        Some("asc") => ("ASC", ">"),
        Some(_) => return Err(invalid("order must be asc or desc")),
    };
    let after: Option<i64> = match &params.after {
        Some(after) => Some(
            sqlx::query_scalar("SELECT seq FROM files WHERE id = ?")
                .bind(after)
                .fetch_optional(&state.store.0)
                .await
                .map_err(anyhow::Error::from)?
                .ok_or_else(|| invalid("after must name an existing file"))?,
        ),
        None => None,
    };
    // Fetch one extra row to learn whether more follow.
    let rows: Vec<Row> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {COLUMNS} FROM files WHERE {LIVE} AND (?2 IS NULL OR purpose = ?2) AND (?3 IS NULL OR seq {comparison} ?3) ORDER BY seq {direction} LIMIT ?4"
    )))
    .bind(crate::contract::now() as i64)
    .bind(&params.purpose)
    .bind(after)
    .bind(limit + 1)
    .fetch_all(&state.store.0)
    .await
    .map_err(anyhow::Error::from)?;
    let has_more = rows.len() as i64 > limit;
    let data: Vec<Value> = rows.into_iter().take(limit as usize).map(public).collect();
    Ok(Json(json!({"object": "list", "data": data,
        "first_id": data.first().map(|file| &file["id"]), "last_id": data.last().map(|file| &file["id"]),
        "has_more": has_more})))
}
