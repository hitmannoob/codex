//! Live files in a self-hosted environment's workspace, read and written on
//! the caller's executor. The service reaches the executor through its own
//! registry, as the worker does, so files never pass through the worker.
//!
//! Paths are the executor's own: POSIX and Windows absolute paths are parsed
//! by their spelling, whatever OS this service runs on, and must stay inside
//! the environment's `workspace_directory` after dot segments are resolved.
use crate::ApiError;
use crate::State;
use crate::contract::invalid;
use crate::registry::ExecutorState;
use axum::Json;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State as Extract;
use axum::http::StatusCode;
use base64::Engine;
use codex_exec_server::CreateDirectoryOptions;
use codex_exec_server::ExecutorFileSystem;
use codex_exec_server::GetMetadataOptions;
use codex_exec_server::WalkEntryKind;
use codex_exec_server::WalkOptions;
use codex_exec_server::WriteFileOptions;
use codex_utils_path_uri::LegacyAppPathString;
use codex_utils_path_uri::PathUri;
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;
use std::sync::Arc;

/// The largest inline file, decoded.
pub(crate) const MAX_INLINE_BYTES: usize = 5 * 1024 * 1024;
/// Bounds on one listing's walk of the executor's workspace.
const WALK: WalkOptions = WalkOptions {
    max_depth: 64,
    max_directories: 10_000,
    max_entries: 50_000,
    follow_directory_symlinks: false,
    prune_hidden_directories: false,
};

/// A connected environment's workspace and filesystem.
struct Workspace {
    root: PathUri,
    files: Arc<dyn ExecutorFileSystem>,
}

impl Workspace {
    async fn open(state: &State, environment_id: &str) -> Result<Self, ApiError> {
        let data: Option<String> = sqlx::query_scalar("SELECT data FROM environments WHERE id = ?")
            .bind(environment_id)
            .fetch_optional(&state.store.0)
            .await
            .map_err(anyhow::Error::from)?;
        let data: Value = serde_json::from_str(
            &data.ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "environment not found".into()))?,
        )
        .map_err(anyhow::Error::from)?;
        let root = path_uri(data["workspace_directory"].as_str().unwrap_or_default())
            .ok_or_else(|| anyhow::anyhow!("stored workspace directory is not absolute"))?;
        if state.registry.state(environment_id) != Some(ExecutorState::Connected) {
            return Err(ApiError(
                StatusCode::CONFLICT,
                "environment is not connected; start its executor and retry".into(),
            ));
        }
        let url = state.registry.harness_url().ok_or_else(|| {
            ApiError(
                StatusCode::NOT_IMPLEMENTED,
                "self-hosted environments are disabled; the operator must configure an environment key".into(),
            )
        })?;
        let environment = match state.executors.get_environment(environment_id) {
            Some(environment) => environment,
            None => {
                state
                    .executors
                    .upsert_noise_environment(
                        environment_id.to_owned(),
                        url.to_owned(),
                        environment_id.to_owned(),
                        state.registry.harness_token().to_owned(),
                        // The service's own connection needs no skills.
                        codex_config::ScopedSkillsConfig::default(),
                    )
                    .map_err(anyhow::Error::from)?;
                state
                    .executors
                    .get_environment(environment_id)
                    .ok_or_else(|| anyhow::anyhow!("environment was not added"))?
            }
        };
        Ok(Self {
            root,
            files: environment.get_filesystem(),
        })
    }

    /// A path inside the workspace, or the workspace itself when `allow_root`.
    fn path(&self, path: &str, allow_root: bool) -> Result<PathUri, ApiError> {
        let resolved = path_uri(path)
            .filter(|resolved| resolved.starts_with(&self.root))
            .filter(|resolved| allow_root || *resolved != self.root)
            .ok_or_else(|| {
                invalid(format!(
                    "path must be an absolute path inside the workspace directory {}",
                    self.root.inferred_native_path_string()
                ))
            })?;
        Ok(resolved)
    }
}

/// An absolute path on the executor, with dot segments resolved.
fn path_uri(path: &str) -> Option<PathUri> {
    if path.contains('\0') {
        return None;
    }
    LegacyAppPathString::from_string(path).to_inferred_path_uri()
}

fn unavailable(error: std::io::Error) -> ApiError {
    ApiError(
        StatusCode::BAD_GATEWAY,
        format!("environment file operation failed: {error}"),
    )
}

fn file(environment_id: &str, path: &PathUri, size_bytes: u64) -> Value {
    json!({"object": "agent.environment.file", "environment_id": environment_id,
        "path": path.inferred_native_path_string(), "size_bytes": size_bytes})
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Create {
    Inline { path: String, data: String },
    FileId { path: String, file_id: String },
}

pub(crate) async fn create(
    Extract(state): Extract<Arc<State>>,
    Path(environment_id): Path<String>,
    Json(params): Json<Create>,
) -> Result<Json<Value>, ApiError> {
    let (path, contents) = match params {
        Create::Inline { path, data } => {
            let contents = base64::engine::general_purpose::STANDARD
                .decode(data.as_bytes())
                .map_err(|_| invalid("data must be standard base64"))?;
            if contents.len() > MAX_INLINE_BYTES {
                return Err(invalid(format!(
                    "inline files are limited to {MAX_INLINE_BYTES} bytes"
                )));
            }
            (path, contents)
        }
        // Uploads are already bounded by the Files API limit.
        Create::FileId { path, file_id } => match crate::files::read(&state, &file_id).await {
            Ok(contents) => (path, contents),
            Err(error) if error.0 == StatusCode::NOT_FOUND => {
                return Err(invalid(format!("file {file_id} not found")));
            }
            Err(error) => return Err(error),
        },
    };
    let workspace = Workspace::open(&state, &environment_id).await?;
    let path = workspace.path(&path, /*allow_root*/ false)?;
    if let Some(parent) = path.parent() {
        let options = CreateDirectoryOptions {
            recursive: true,
            follow_symlinks: false,
        };
        workspace
            .files
            .create_directory(&parent, options, /*sandbox*/ None)
            .await
            .map_err(unavailable)?;
    }
    // Never follow a link out of the workspace.
    let options = WriteFileOptions {
        follow_symlinks: false,
    };
    let size = contents.len() as u64;
    workspace
        .files
        .write_file(&path, contents, options, /*sandbox*/ None)
        .await
        .map_err(unavailable)?;
    Ok(Json(file(&environment_id, &path, size)))
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ListParams {
    limit: Option<usize>,
    order: Option<String>,
    page: Option<String>,
    path: Option<String>,
}

pub(crate) async fn list(
    Extract(state): Extract<Arc<State>>,
    Path(environment_id): Path<String>,
    Query(params): Query<ListParams>,
) -> Result<Json<Value>, ApiError> {
    let limit = params.limit.unwrap_or(20);
    if !(1..=100).contains(&limit) {
        return Err(invalid("limit must be between 1 and 100"));
    }
    let descending = match params.order.as_deref() {
        None | Some("desc") => true,
        Some("asc") => false,
        Some(_) => return Err(invalid("order must be asc or desc")),
    };
    // The page token is the last path of the previous page.
    let after = params
        .page
        .map(|page| {
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(page.as_bytes())
                .ok()
                .and_then(|bytes| String::from_utf8(bytes).ok())
                .and_then(|path| path_uri(&path))
                .ok_or_else(|| invalid("invalid page token"))
        })
        .transpose()?;
    let workspace = Workspace::open(&state, &environment_id).await?;
    let directory = match &params.path {
        Some(path) => workspace.path(path, /*allow_root*/ true)?,
        None => workspace.root.clone(),
    };
    let walked = workspace
        .files
        .walk(&directory, WALK, /*sandbox*/ None)
        .await
        .map_err(unavailable)?;
    if walked.truncated {
        return Err(invalid(
            "the directory holds too many entries to list; pass a narrower path",
        ));
    }
    // Case-sensitive path components, so a directory sorts before its siblings'
    // longer names.
    let key = |path: &PathUri| -> Vec<String> {
        path.encoded_path()
            .split('/')
            .filter(|segment| !segment.is_empty())
            .map(str::to_owned)
            .collect()
    };
    let mut paths: Vec<(Vec<String>, PathUri)> = walked
        .entries
        .into_iter()
        .filter(|entry| entry.kind == WalkEntryKind::File)
        .map(|entry| (key(&entry.path), entry.path))
        .collect();
    paths.sort_by(|left, right| left.0.cmp(&right.0));
    if descending {
        paths.reverse();
    }
    if let Some(after) = after {
        let after = key(&after);
        paths.retain(|(path, _)| {
            if descending {
                *path < after
            } else {
                *path > after
            }
        });
    }
    let has_more = paths.len() > limit;
    paths.truncate(limit);
    let mut data = Vec::with_capacity(paths.len());
    for (_, path) in &paths {
        let metadata = workspace
            .files
            .get_metadata(path, GetMetadataOptions::default(), /*sandbox*/ None)
            .await
            .map_err(unavailable)?;
        data.push(file(&environment_id, path, metadata.size));
    }
    let next = paths.last().filter(|_| has_more).map(|(_, path)| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(path.inferred_native_path_string().as_bytes())
    });
    Ok(Json(
        json!({"object": "list", "data": data, "has_more": has_more, "next": next}),
    ))
}
