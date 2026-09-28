//! Durable public records are committed before their live notifications are sent.
use crate::ApiError;
use crate::State;
use crate::resources::RequiredAction;
use crate::resources::ToolResult;
use axum::Json;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State as Extract;
use axum::http::StatusCode;
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

pub(crate) async fn disconnected(pool: &sqlx::SqlitePool) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query("UPDATE public_sessions SET data = json_set(data, '$.status', 'failed', '$.error', 'backend connection lost') WHERE json_extract(data, '$.status') IN ('in_progress', 'requires_action')").execute(&mut *tx).await?;
    sqlx::query("UPDATE public_records SET data = json_set(data, '$.status', 'failed', '$.completed_at', ?, '$.error', json(?)) WHERE kind = 'turn' AND json_extract(data, '$.status') IN ('queued', 'in_progress', 'waiting')")
        .bind(crate::contract::now() as i64).bind(json!({"code":crate::reconcile::CONNECTION_LOST_CODE,"message":"backend connection lost"}).to_string()).execute(&mut *tx).await?;
    sqlx::query("UPDATE public_records SET data = json_set(data, '$.status', 'incomplete') WHERE kind = 'item' AND json_extract(data, '$.status') = 'in_progress'").execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn create_session(pool: &sqlx::SqlitePool, data: &Value) -> anyhow::Result<()> {
    sqlx::query("INSERT INTO public_sessions (id,data) VALUES (?,?)")
        .bind(data["id"].as_str())
        .bind(data.to_string())
        .execute(pool)
        .await?;
    Ok(())
}

/// Change only the fields status writers own, so a concurrent session update's
/// metadata and settings survive. Returns the lifecycle event to broadcast after
/// commit, or `None` when the session has been deleted.
pub(crate) async fn transition(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    id: &str,
    status: &str,
    error: Option<&str>,
) -> anyhow::Result<Option<Value>> {
    let data: Option<String> = sqlx::query_scalar("UPDATE public_sessions SET data = json_set(data, '$.status', ?, '$.error', ?, '$.last_active_at', ?) WHERE id = ? RETURNING data")
        .bind(status).bind(error).bind(crate::contract::now() as i64).bind(id).fetch_optional(&mut **tx).await?;
    let Some(data) = data else {
        return Ok(None);
    };
    let session = decorate(&mut **tx, id, serde_json::from_str(&data)?).await?;
    Ok(Some(
        json!({"type": format!("agent.session.{status}"), "session": session}),
    ))
}

/// Fill a stored public session document with its current required actions.
pub(crate) async fn decorate<'e, E: sqlx::Executor<'e, Database = sqlx::Sqlite>>(
    executor: E,
    id: &str,
    mut data: Value,
) -> anyhow::Result<Value> {
    let actions: Vec<String> = sqlx::query_scalar("SELECT action FROM tool_calls WHERE session_id = ? AND status = 'pending' ORDER BY turn_id, call_id")
        .bind(id).fetch_all(executor).await?;
    data["required_actions"] = actions
        .iter()
        .map(|action| {
            let action: RequiredAction = serde_json::from_str(action)?;
            Ok(json!({"type":"function_call","turn_id":action.turn_id,"call_id":action.call_id,"name":action.name,"arguments":action.arguments}))
        })
        .collect::<anyhow::Result<_>>()?;
    Ok(data)
}

pub(crate) async fn session(state: &State, id: &str) -> Result<Value, ApiError> {
    let data: Option<String> = sqlx::query_scalar("SELECT data FROM public_sessions WHERE id = ?")
        .bind(id)
        .fetch_optional(&state.store.0)
        .await
        .map_err(anyhow::Error::from)?;
    let data = serde_json::from_str(
        &data.ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "session not found".into()))?,
    )
    .map_err(anyhow::Error::from)?;
    Ok(decorate(&state.store.0, id, data).await?)
}

pub(crate) fn emit(state: &State, mut event: Value) {
    event["event_id"] = json!(Uuid::new_v4().to_string());
    let _ = state.public_events.send(event);
}

pub(crate) async fn session_status(
    state: &State,
    id: &str,
    status: &str,
    error: Option<&str>,
) -> Result<(), ApiError> {
    let mut tx = state
        .store
        .0
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(anyhow::Error::from)?;
    let event = transition(&mut tx, id, status, error).await?;
    tx.commit().await.map_err(anyhow::Error::from)?;
    if let Some(event) = event {
        emit(state, event);
    }
    Ok(())
}

/// Persist a public record. `subagent` tags records a subagent produced; an
/// update keeps the record's original tag.
pub(crate) async fn save<'e, E: sqlx::Executor<'e, Database = sqlx::Sqlite>>(
    executor: E,
    session_id: &str,
    kind: &str,
    data: &Value,
    turn_id: &str,
    subagent: Option<&str>,
) -> anyhow::Result<()> {
    sqlx::query("INSERT INTO public_records(session_id,kind,id,turn_id,data,subagent_id) VALUES(?,?,?,?,?,?) ON CONFLICT(session_id,kind,id) DO UPDATE SET data = excluded.data")
        .bind(session_id).bind(kind).bind(data["id"].as_str()).bind(turn_id).bind(data.to_string()).bind(subagent).execute(executor).await?;
    Ok(())
}

pub(crate) async fn active_turn(state: &State, id: &str) -> Result<Option<String>, ApiError> {
    sqlx::query_scalar("SELECT id FROM public_records WHERE session_id = ? AND kind = 'turn' AND subagent_id IS NULL AND json_extract(data, '$.status') IN ('queued','in_progress','waiting') ORDER BY seq DESC LIMIT 1")
        .bind(id).fetch_optional(&state.store.0).await.map_err(anyhow::Error::from).map_err(Into::into)
}

pub(crate) async fn notification(state: &State, raw: &Value) -> Result<(), ApiError> {
    let params = &raw["params"];
    let Some(thread_id) = params["threadId"].as_str() else {
        return Ok(());
    };
    let method = raw["method"].as_str().unwrap_or_default();
    if !matches!(
        method,
        "turn/started"
            | "turn/completed"
            | "session.requires_action"
            | "item/started"
            | "item/completed"
            | "thread/tokenUsage/updated"
    ) {
        return Ok(());
    }
    // Resolve the session inside the write transaction: a concurrent deletion
    // either commits first (nothing is written) or waits for this commit.
    // Reserve the writer up front; upgrading a deferred read transaction can
    // fail immediately if another session writes.
    let mut tx = state
        .store
        .0
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(anyhow::Error::from)?;
    let Some(owner) = crate::subagents::owner(&mut tx, thread_id).await? else {
        return Ok(());
    };
    let id = owner.session_id.clone();
    let subagent = owner.subagent_id.as_deref();
    // Related records commit together; events are broadcast only afterwards.
    let mut events = Vec::new();
    match method {
        "turn/started" | "turn/completed" => {
            let source = &params["turn"];
            let status = match source["status"].as_str() {
                Some("completed") => "completed",
                Some("interrupted") => "cancelled",
                Some("failed") => "failed",
                _ => "in_progress",
            };
            let turn_id = source["id"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("turn missing ID"))?;
            let turn = json!({"id":turn_id,"object":"agent.session.turn","session_id":id,"agent_id":owner.agent_id,"created_at":source["startedAt"].as_u64().unwrap_or_else(crate::contract::now),
                "started_at":source["startedAt"],"completed_at":source["completedAt"],"status":status,"usage":crate::usage::usage(&mut tx, &id, Some(turn_id)).await?,"subagent_id":subagent,
                "error":if status == "failed" {crate::turns::turn_error(&source["error"])} else {Value::Null}});
            if status != "in_progress" {
                events.extend(close_items(&mut tx, &id, turn_id).await?);
                state.streams.end_turn(&id, turn_id);
            }
            save(&mut *tx, &id, "turn", &turn, turn_id, subagent).await?;
            // Only the session's own turns change its status; subagent turns
            // appear on its stream tagged with their `subagent_id`.
            let root = subagent.is_none();
            if method == "turn/started" {
                if root {
                    events.extend(transition(&mut tx, &id, "in_progress", /*error*/ None).await?);
                }
                events.push(json!({"type":"agent.session.turn.created","session_id":id,"turn_id":turn_id,"turn":turn}));
            }
            events.push(json!({"type":format!("agent.session.turn.{status}"),"session_id":id,"turn_id":turn_id,"turn":turn}));
            if status != "in_progress" && root {
                events.extend(transition(&mut tx, &id, "idle", /*error*/ None).await?);
            }
        }
        "thread/tokenUsage/updated" => {
            let Some(turn_id) = params["turnId"].as_str() else {
                return Ok(());
            };
            crate::usage::record(&mut tx, &id, thread_id, turn_id, &params["tokenUsage"]).await?;
        }
        // Subagents have no function tools, so only root turns wait on actions.
        "session.requires_action" if subagent.is_none() => {
            let turn_id = params["action"]["turnId"].as_str();
            sqlx::query("UPDATE public_records SET data = json_set(data, '$.status', 'waiting') WHERE session_id = ? AND kind = 'turn' AND id = ?").bind(&id).bind(turn_id).execute(&mut *tx).await.map_err(anyhow::Error::from)?;
            events.extend(transition(&mut tx, &id, "requires_action", /*error*/ None).await?);
        }
        "item/started" | "item/completed" => {
            let item = &params["item"];
            let turn_id = params["turnId"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("item missing turn ID"))?;
            let done = method == "item/completed";
            let codex_id = item["id"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("item missing ID"))?;
            let status = if done { "completed" } else { "in_progress" };
            let item_id = format!("item_{turn_id}_{codex_id}");
            let common = json!({"id":item_id,"turn_id":turn_id,"status":status});
            let mut public = match item["type"].as_str() {
                Some("userMessage") => {
                    json!({"type":"message","role":"user","phase":null,"content":item["content"].as_array().into_iter().flatten().filter_map(|c| match c["type"].as_str() {
                        Some("text") => Some(json!({"type":"input_text","text":c["text"]})),
                        Some("image") => Some(json!({"type":"input_image","image_url":c["url"]})),
                        _ => None,
                    }).collect::<Vec<_>>()})
                }
                Some("agentMessage") => {
                    json!({"type":"message","role":"assistant","phase":item["phase"],"content":[{"type":"output_text","text":item["text"]}]})
                }
                Some("reasoning") => {
                    json!({"type":"reasoning","summary":item["summary"].as_array().into_iter().flatten().map(|text| json!({"type":"summary_text","text":text})).collect::<Vec<_>>()})
                }
                Some("dynamicToolCall") => {
                    json!({"type":"function_call","call_id":item["id"],"name":item["tool"],"arguments":item["arguments"]})
                }
                Some("subAgentActivity") => {
                    let Some((subagent_id, created)) =
                        crate::subagents::register(&mut tx, &owner, item).await?
                    else {
                        return Ok(());
                    };
                    events.extend(created);
                    let Some(public) = crate::subagents::activity_item(&owner, &subagent_id, item)
                    else {
                        tx.commit().await.map_err(anyhow::Error::from)?;
                        for event in events {
                            emit(state, event);
                        }
                        return Ok(());
                    };
                    public
                }
                Some("mcpToolCall") => {
                    json!({"type":"mcp_call","server_label":item["server"],"name":item["tool"],"arguments":item["arguments"],
                        "output":item["result"]["content"],"error":item["error"]["message"]})
                }
                _ => return Ok(()),
            };
            public
                .as_object_mut()
                .ok_or_else(|| anyhow::anyhow!("invalid item"))?
                .extend(
                    common
                        .as_object()
                        .into_iter()
                        .flatten()
                        .map(|(k, v)| (k.clone(), v.clone())),
                );
            if public["role"] == "user" {
                public["status"] = json!("completed");
            }
            if done
                && matches!(
                    item["type"].as_str(),
                    Some("dynamicToolCall" | "mcpToolCall")
                )
                && item["status"] == "failed"
            {
                public["status"] = json!("failed");
            }
            // The item and, for a resolved tool call, its output record share one
            // transaction so `output_index` counts them consistently.
            let published = publish_item(&mut tx, &id, turn_id, &public, done, subagent).await?;
            events.extend(published.added);
            if let Some(output_index) = published.output_index {
                events.extend(if done {
                    state
                        .streams
                        .finished(thread_id, codex_id, &id, &public, output_index)
                } else {
                    state
                        .streams
                        .started(thread_id, codex_id, &id, &public, output_index)
                });
            }
            events.extend(published.done);
            if done && item["type"] == "dynamicToolCall" {
                let text = item["contentItems"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|c| c["text"].as_str())
                    .collect::<Vec<_>>()
                    .join("\n");
                let failed = item["success"] == false || item["status"] == "failed";
                // Save the result as submitted (a string or content parts); fall
                // back to Codex's text when no matching submission was stored.
                let stored: Option<String> = sqlx::query_scalar("SELECT result FROM tool_calls WHERE session_id = ? AND turn_id = ? AND call_id = ?")
                    .bind(&id).bind(turn_id).bind(codex_id).fetch_optional(&mut *tx).await.map_err(anyhow::Error::from)?.flatten();
                let stored: Option<ToolResult> = stored
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()
                    .map_err(anyhow::Error::from)?;
                let (result, error) = match (stored, failed) {
                    (Some(stored), false) if stored.success => (stored.output, Value::Null),
                    (Some(stored), true) if !stored.success => (Value::Null, stored.output),
                    (_, false) => (json!(text), Value::Null),
                    (_, true) => (Value::Null, json!(text)),
                };
                let output = json!({"id":format!("output_{item_id}"),"type":"function_call_output","turn_id":turn_id,"call_id":item["id"],"status":if failed {"failed"} else {"completed"},"output":result,"error":error});
                let published =
                    publish_item(&mut tx, &id, turn_id, &output, /*done*/ true, subagent).await?;
                events.extend(published.added);
            }
        }
        _ => return Ok(()),
    }
    tx.commit().await.map_err(anyhow::Error::from)?;
    for event in events {
        emit(state, event);
    }
    Ok(())
}

/// Lifecycle events for one persisted item, broadcast only after commit.
struct Published {
    added: Option<Value>,
    done: Option<Value>,
    /// The item's position in the turn output; `None` for input items.
    output_index: Option<i64>,
}

/// Persist one public item within the caller's transaction and return its
/// events, rather than broadcasting mid-write.
async fn publish_item(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    id: &str,
    turn_id: &str,
    item: &Value,
    done: bool,
    subagent: Option<&str>,
) -> anyhow::Result<Published> {
    let item_id = item["id"].as_str().unwrap_or_default();
    let previous: Option<i64> = sqlx::query_scalar(
        "SELECT seq FROM public_records WHERE session_id = ? AND kind = 'item' AND id = ?",
    )
    .bind(id)
    .bind(item_id)
    .fetch_optional(&mut **tx)
    .await?;
    save(&mut **tx, id, "item", item, turn_id, subagent).await?;
    let input = item["role"] == "user" || item["type"] == "function_call_output";
    let output_index = if input {
        None
    } else {
        Some(output_index(tx, id, turn_id, item_id).await?)
    };
    let event = |kind: &str| json!({"type":kind,"session_id":id,"turn_id":turn_id,"item":item,"output_index":output_index});
    Ok(Published {
        added: previous
            .is_none()
            .then(|| event("agent.session.turn.item.added")),
        done: (done && !input).then(|| event("agent.session.turn.item.done")),
        output_index,
    })
}

/// An output item's position among its turn's output items, in arrival order.
async fn output_index(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    id: &str,
    turn_id: &str,
    item_id: &str,
) -> anyhow::Result<i64> {
    Ok(sqlx::query_scalar("SELECT count(*) - 1 FROM public_records WHERE session_id = ? AND kind = 'item' AND turn_id = ? AND coalesce(json_extract(data, '$.role'), '') != 'user' AND json_extract(data, '$.type') != 'function_call_output' AND seq <= (SELECT seq FROM public_records WHERE session_id = ? AND kind = 'item' AND id = ?)")
        .bind(id).bind(turn_id).bind(id).bind(item_id).fetch_one(&mut **tx).await?)
}

/// Mark items a terminal turn left unfinished `incomplete`, returning their
/// `item.done` events so every added item is closed exactly once.
async fn close_items(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    id: &str,
    turn_id: &str,
) -> anyhow::Result<Vec<Value>> {
    let open: Vec<String> = sqlx::query_scalar("SELECT data FROM public_records WHERE session_id = ? AND kind = 'item' AND turn_id = ? AND json_extract(data, '$.status') = 'in_progress' ORDER BY seq")
        .bind(id).bind(turn_id).fetch_all(&mut **tx).await?;
    let mut events = Vec::new();
    for item in open {
        let mut item: Value = serde_json::from_str(&item)?;
        item["status"] = json!("incomplete");
        save(
            &mut **tx, id, "item", &item, turn_id, /*subagent*/ None,
        )
        .await?;
        let output_index =
            output_index(tx, id, turn_id, item["id"].as_str().unwrap_or_default()).await?;
        events.push(json!({"type":"agent.session.turn.item.done","session_id":id,"turn_id":turn_id,"item":item,"output_index":output_index}));
    }
    Ok(events)
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Page {
    after: Option<String>,
    limit: Option<u32>,
    order: Option<String>,
    pub(crate) turn_id: Option<String>,
}

/// List a session's records of one kind: its own when `subagent` is `None`,
/// otherwise that subagent's.
pub(crate) async fn list(
    state: &State,
    id: &str,
    kind: &str,
    page: Page,
    subagent: Option<&str>,
) -> Result<Json<Value>, ApiError> {
    session(state, id).await?;
    let limit = page.limit.unwrap_or(/*default*/ 20);
    let order = page.order.as_deref().unwrap_or("desc");
    if !(1..=100).contains(&limit)
        || !matches!(order, "asc" | "desc")
        || kind == "turn" && page.turn_id.is_some()
    {
        return Err(crate::contract::invalid("invalid pagination parameters"));
    }
    let cursor: Option<i64> = if let Some(after) = page.after {
        Some(sqlx::query_scalar("SELECT seq FROM public_records WHERE session_id = ? AND kind = ? AND id = ? AND subagent_id IS ? AND (? IS NULL OR turn_id = ?)").bind(id).bind(kind).bind(after).bind(subagent).bind(&page.turn_id).bind(&page.turn_id).fetch_optional(&state.store.0).await.map_err(anyhow::Error::from)?.ok_or_else(|| crate::contract::invalid("invalid after cursor"))?)
    } else {
        None
    };
    let query = if order == "asc" {
        "SELECT data FROM public_records WHERE session_id = ? AND kind = ? AND subagent_id IS ? AND (? IS NULL OR seq > ?) AND (? IS NULL OR turn_id = ?) ORDER BY seq ASC LIMIT ?"
    } else {
        "SELECT data FROM public_records WHERE session_id = ? AND kind = ? AND subagent_id IS ? AND (? IS NULL OR seq < ?) AND (? IS NULL OR turn_id = ?) ORDER BY seq DESC LIMIT ?"
    };
    let rows: Vec<String> = sqlx::query_scalar(query)
        .bind(id)
        .bind(kind)
        .bind(subagent)
        .bind(cursor)
        .bind(cursor)
        .bind(&page.turn_id)
        .bind(&page.turn_id)
        .bind(limit + 1)
        .fetch_all(&state.store.0)
        .await
        .map_err(anyhow::Error::from)?;
    let has_more = rows.len() > limit as usize;
    let data = rows
        .into_iter()
        .take(limit as usize)
        .map(|s| serde_json::from_str::<Value>(&s))
        .collect::<Result<Vec<_>, _>>()
        .map_err(anyhow::Error::from)?;
    Ok(Json(
        json!({"object":"list","first_id":data.first().map(|v| &v["id"]),"last_id":data.last().map(|v| &v["id"]),"data":data,"has_more":has_more}),
    ))
}

pub(crate) async fn items(
    Extract(state): Extract<Arc<State>>,
    Path(id): Path<String>,
    Query(page): Query<Page>,
) -> Result<Json<Value>, ApiError> {
    list(&state, &id, "item", page, /*subagent*/ None).await
}
pub(crate) async fn turns(
    Extract(state): Extract<Arc<State>>,
    Path(id): Path<String>,
    Query(page): Query<Page>,
) -> Result<Json<Value>, ApiError> {
    list(&state, &id, "turn", page, /*subagent*/ None).await
}
pub(crate) async fn turn(
    Extract(state): Extract<Arc<State>>,
    Path((id, turn_id)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    turn_record(&state, &id, /*subagent*/ None, &turn_id).await
}

/// One of a session's turns, or one of a subagent's when `subagent` is set.
pub(crate) async fn turn_record(
    state: &State,
    id: &str,
    subagent: Option<&str>,
    turn_id: &str,
) -> Result<Json<Value>, ApiError> {
    let data: Option<String> = sqlx::query_scalar(
        "SELECT data FROM public_records WHERE session_id = ? AND kind = 'turn' AND subagent_id IS ? AND id = ?",
    )
    .bind(id)
    .bind(subagent)
    .bind(turn_id)
    .fetch_optional(&state.store.0)
    .await
    .map_err(anyhow::Error::from)?;
    Ok(Json(
        serde_json::from_str(
            &data.ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "turn not found".into()))?,
        )
        .map_err(anyhow::Error::from)?,
    ))
}
