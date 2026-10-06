//! A minimal Files API (`/v1/files`) for uploads that environment files name
//! by `file_id`. Contents live under `DATA_DIRECTORY/files`, metadata in the
//! database. A file expires only when its upload asks for it; an expired file
//! reads as deleted and is removed on the next upload or start.
use crate::ApiError;
use crate::State;
use crate::contract::invalid;
use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::DefaultBodyLimit;
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
use std::path::PathBuf;
use std::sync::Arc;
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
        .route(
            "/v1/files",
            post(create)
                .get(list)
                .layer(DefaultBodyLimit::max(MAX_FILE_BYTES + 1024 * 1024)),
        )
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

/// A live file's contents, for an environment file's `file_id` source.
pub(crate) async fn read(state: &State, id: &str) -> Result<Vec<u8>, ApiError> {
    row(state, id).await?;
    tokio::fs::read(state.files.0.join(id))
        .await
        .map_err(|_| not_found())
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

/// One `multipart/form-data` part.
struct Part<'a> {
    name: String,
    filename: Option<String>,
    data: &'a [u8],
}

fn find(haystack: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    haystack
        .get(from..)?
        .windows(needle.len())
        .position(|window| window == needle)
        .map(|position| position + from)
}

/// The parts of a `multipart/form-data` body, or `None` when it is malformed.
fn parts<'a>(content_type: &str, body: &'a [u8]) -> Option<Vec<Part<'a>>> {
    let boundary = content_type
        .split(';')
        .map(str::trim)
        .find_map(|parameter| parameter.strip_prefix("boundary="))?
        .trim_matches('"');
    let delimiter = format!("--{boundary}").into_bytes();
    let separator = format!("\r\n--{boundary}").into_bytes();
    let mut cursor = find(body, &delimiter, 0)? + delimiter.len();
    let mut parts = Vec::new();
    loop {
        if body.get(cursor..cursor + 2)? == b"--" {
            return Some(parts);
        }
        cursor += 2; // The CRLF after a delimiter.
        let headers_end = find(body, b"\r\n\r\n", cursor)?;
        let headers = std::str::from_utf8(&body[cursor..headers_end]).ok()?;
        let disposition = headers.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.trim()
                .eq_ignore_ascii_case("content-disposition")
                .then_some(value)
        })?;
        let parameter = |key: &str| {
            disposition.split(';').map(str::trim).find_map(|parameter| {
                parameter
                    .strip_prefix(key)
                    .and_then(|rest| rest.strip_prefix('='))
                    .map(|value| value.trim_matches('"').to_owned())
            })
        };
        let data_start = headers_end + 4;
        let data_end = find(body, &separator, data_start)?;
        parts.push(Part {
            name: parameter("name")?,
            filename: parameter("filename"),
            data: &body[data_start..data_end],
        });
        cursor = data_end + separator.len();
    }
}

async fn create(
    Extract(state): Extract<Arc<State>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .filter(|value| value.starts_with("multipart/form-data"))
        .ok_or_else(|| invalid("uploads must be multipart/form-data"))?;
    let parts = parts(content_type, &body).ok_or_else(|| invalid("malformed multipart body"))?;
    let field = |name: &str| {
        parts
            .iter()
            .find(|part| part.name == name)
            .map(|part| String::from_utf8_lossy(part.data).into_owned())
    };
    let file = parts
        .iter()
        .find(|part| part.name == "file")
        .ok_or_else(|| invalid("file is required"))?;
    let purpose = field("purpose").ok_or_else(|| invalid("purpose is required"))?;
    if !PURPOSES.contains(&purpose.as_str()) {
        return Err(invalid(format!(
            "purpose must be one of {}",
            PURPOSES.join(", ")
        )));
    }
    if file.data.len() > MAX_FILE_BYTES {
        return Err(invalid(format!(
            "files are limited to {MAX_FILE_BYTES} bytes"
        )));
    }
    let filename = file
        .filename
        .clone()
        .filter(|name| !name.is_empty() && name.len() <= 512)
        .ok_or_else(|| invalid("the file part needs a filename of at most 512 bytes"))?;
    let now = crate::contract::now() as i64;
    let expires_at = match (
        field("expires_after[anchor]"),
        field("expires_after[seconds]"),
    ) {
        (None, None) => None,
        (Some(anchor), Some(seconds)) if anchor == "created_at" => {
            let seconds: i64 = seconds
                .parse()
                .ok()
                .filter(|seconds| (3600..=2_592_000).contains(seconds))
                .ok_or_else(|| invalid("expires_after.seconds must be between 3600 and 2592000"))?;
            Some(now + seconds)
        }
        _ => {
            return Err(invalid("expires_after needs anchor created_at and seconds"));
        }
    };
    expire(&state).await?;
    let id = format!("file-{}", Uuid::new_v4().simple());
    tokio::fs::create_dir_all(&state.files.0)
        .await
        .map_err(anyhow::Error::from)?;
    // Write under a temporary name so a crash never leaves partial contents
    // under a file's ID.
    let partial = state.files.0.join(format!("{id}.partial"));
    tokio::fs::write(&partial, file.data)
        .await
        .map_err(anyhow::Error::from)?;
    tokio::fs::rename(&partial, state.files.0.join(&id))
        .await
        .map_err(anyhow::Error::from)?;
    let row: Row = (
        id,
        filename,
        purpose,
        file.data.len() as i64,
        now,
        expires_at,
    );
    sqlx::query("INSERT INTO files (id, filename, purpose, bytes, created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?)")
        .bind(&row.0).bind(&row.1).bind(&row.2).bind(row.3).bind(row.4).bind(row.5)
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
    let data = read(&state, &id).await?;
    Ok(([(header::CONTENT_TYPE, "application/octet-stream")], data).into_response())
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
