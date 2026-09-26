//! Durable public records are committed before their live notifications are sent.
use crate::ApiError;
use crate::State;
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

/// Persist a public session document. Executor-generic so a single write can go
/// straight to the pool, while a multi-write handler can pass its transaction so
/// related records commit together before any event is broadcast.
pub(crate) async fn save_session<'e, E: sqlx::Executor<'e, Database = sqlx::Sqlite>>(
    executor: E,
    id: &str,
    data: &Value,
) -> anyhow::Result<()> {
    sqlx::query("INSERT INTO public_sessions (id,data) VALUES (?,?) ON CONFLICT(id) DO UPDATE SET data = excluded.data")
        .bind(id).bind(data.to_string()).execute(executor).await?;
    Ok(())
}

/// Apply a session status change in memory, returning the updated document to
/// persist and the lifecycle event to broadcast only after the commit.
fn transition(mut session: Value, status: &str) -> (Value, Value) {
    session["status"] = json!(status);
    session["error"] = Value::Null;
    session["last_active_at"] = json!(crate::contract::now());
    let event = json!({"type": format!("agent.session.{status}"), "session": session});
    (session, event)
}

pub(crate) async fn session(state: &State, id: &str) -> Result<Value, ApiError> {
    let data: Option<String> = sqlx::query_scalar("SELECT data FROM public_sessions WHERE id = ?")
        .bind(id)
        .fetch_optional(&state.store.0)
        .await
        .map_err(anyhow::Error::from)?;
    let mut data: Value = serde_json::from_str(
        &data.ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "session not found".into()))?,
    )
    .map_err(anyhow::Error::from)?;
    let saved = state
        .store
        .session(id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing session"))?;
    data["required_actions"] = json!(saved.required_actions.into_iter().map(|a| json!({"type":"function_call","turn_id":a.turn_id,"call_id":a.call_id,"name":a.name,"arguments":a.arguments})).collect::<Vec<_>>());
    Ok(data)
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
    let mut data = session(state, id).await?;
    data["status"] = json!(status);
    data["error"] = json!(error);
    data["last_active_at"] = json!(crate::contract::now());
    save_session(&state.store.0, id, &data).await?;
    emit(
        state,
        json!({"type":format!("agent.session.{status}"),"session":data}),
    );
    Ok(())
}

async fn save<'e, E: sqlx::Executor<'e, Database = sqlx::Sqlite>>(
    executor: E,
    session_id: &str,
    kind: &str,
    data: &Value,
    turn_id: &str,
) -> anyhow::Result<()> {
    sqlx::query("INSERT INTO public_records(session_id,kind,id,turn_id,data) VALUES(?,?,?,?,?) ON CONFLICT(session_id,kind,id) DO UPDATE SET data = excluded.data")
        .bind(session_id).bind(kind).bind(data["id"].as_str()).bind(turn_id).bind(data.to_string()).execute(executor).await?;
    Ok(())
}

pub(crate) async fn active_turn(state: &State, id: &str) -> Result<Option<String>, ApiError> {
    sqlx::query_scalar("SELECT id FROM public_records WHERE session_id = ? AND kind = 'turn' AND json_extract(data, '$.status') IN ('queued','in_progress','waiting') ORDER BY seq DESC LIMIT 1")
        .bind(id).fetch_optional(&state.store.0).await.map_err(anyhow::Error::from).map_err(Into::into)
}

pub(crate) async fn notification(state: &State, raw: &Value) -> Result<(), ApiError> {
    let params = &raw["params"];
    let Some(thread_id) = params["threadId"].as_str() else {
        return Ok(());
    };
    let id: Option<String> = sqlx::query_scalar(
        "SELECT s.id FROM sessions s JOIN public_sessions p ON p.id = s.id WHERE s.thread_id = ?",
    )
    .bind(thread_id)
    .fetch_optional(&state.store.0)
    .await
    .map_err(anyhow::Error::from)?;
    let Some(id) = id else {
        return Ok(());
    };
    match raw["method"].as_str().unwrap_or_default() {
        "turn/started" | "turn/completed" => {
            let source = &params["turn"];
            let status = match source["status"].as_str() {
                Some("completed") => "completed",
                Some("interrupted") => "cancelled",
                Some("failed") => "failed",
                _ => "in_progress",
            };
            let current = session(state, &id).await?;
            let turn_id = source["id"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("turn missing ID"))?;
            let turn = json!({"id":turn_id,"object":"agent.session.turn","session_id":id,"agent_id":current["agent"]["id"],"created_at":source["startedAt"].as_u64().unwrap_or_else(crate::contract::now),
                "started_at":source["startedAt"],"completed_at":source["completedAt"],"status":status,"usage":null,"subagent_id":null,
                "error":if status == "failed" {json!({"code":"internal_error","message":source["error"]["message"].as_str().unwrap_or("turn failed")})} else {Value::Null}});
            // Persist the turn record and any session-status change atomically,
            // then broadcast the events only after the commit succeeds.
            let mut events = Vec::new();
            let mut tx = state.store.0.begin().await.map_err(anyhow::Error::from)?;
            save(&mut *tx, &id, "turn", &turn, turn_id).await?;
            if raw["method"] == "turn/started" {
                let (data, event) = transition(current.clone(), "in_progress");
                save_session(&mut *tx, &id, &data).await?;
                events.push(event);
                events.push(json!({"type":"agent.session.turn.created","session_id":id,"turn_id":turn_id,"turn":turn}));
            }
            events.push(json!({"type":format!("agent.session.turn.{status}"),"session_id":id,"turn_id":turn_id,"turn":turn}));
            if status != "in_progress" {
                let (data, event) = transition(current.clone(), "idle");
                save_session(&mut *tx, &id, &data).await?;
                events.push(event);
            }
            tx.commit().await.map_err(anyhow::Error::from)?;
            for event in events {
                emit(state, event);
            }
        }
        "session.requires_action" => {
            let turn_id = params["action"]["turnId"].as_str();
            let (data, event) = transition(session(state, &id).await?, "requires_action");
            let mut tx = state.store.0.begin().await.map_err(anyhow::Error::from)?;
            sqlx::query("UPDATE public_records SET data = json_set(data, '$.status', 'waiting') WHERE session_id = ? AND kind = 'turn' AND id = ?").bind(&id).bind(turn_id).execute(&mut *tx).await.map_err(anyhow::Error::from)?;
            save_session(&mut *tx, &id, &data).await?;
            tx.commit().await.map_err(anyhow::Error::from)?;
            emit(state, event);
        }
        "item/started" | "item/completed" => {
            let item = &params["item"];
            let turn_id = params["turnId"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("item missing turn ID"))?;
            let done = raw["method"] == "item/completed";
            let status = if done { "completed" } else { "in_progress" };
            let item_id = format!(
                "item_{turn_id}_{}",
                item["id"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("item missing ID"))?
            );
            let common = json!({"id":item_id,"turn_id":turn_id,"status":status});
            let mut public = match item["type"].as_str() {
                Some("userMessage") => {
                    json!({"type":"message","role":"user","phase":null,"content":item["content"].as_array().into_iter().flatten().filter_map(|c| c["text"].as_str().map(|text| json!({"type":"input_text","text":text}))).collect::<Vec<_>>()})
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
            if done && item["type"] == "dynamicToolCall" && item["status"] == "failed" {
                public["status"] = json!("failed");
            }
            // The item and, for a resolved tool call, its output record share one
            // transaction so `output_index` counts them consistently; events are
            // broadcast only after the commit.
            let mut tx = state.store.0.begin().await.map_err(anyhow::Error::from)?;
            let mut events = publish_item(&mut tx, &id, turn_id, &public, done).await?;
            if done && item["type"] == "dynamicToolCall" {
                let output = item["contentItems"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|c| c["text"].as_str())
                    .collect::<Vec<_>>()
                    .join("\n");
                let failed = item["success"] == false || item["status"] == "failed";
                let output = json!({"id":format!("output_{item_id}"),"type":"function_call_output","turn_id":turn_id,"call_id":item["id"],"status":if failed {"failed"} else {"completed"},"output":if failed {Value::Null} else {json!(output)},"error":if failed {json!(output)} else {Value::Null}});
                events.extend(publish_item(&mut tx, &id, turn_id, &output, /*done*/ true).await?);
            }
            tx.commit().await.map_err(anyhow::Error::from)?;
            for event in events {
                emit(state, event);
            }
        }
        _ => {}
    }
    Ok(())
}

/// Persist one public item within the caller's transaction and return the events
/// to broadcast after commit (in order), rather than broadcasting mid-write.
async fn publish_item(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    id: &str,
    turn_id: &str,
    item: &Value,
    done: bool,
) -> anyhow::Result<Vec<Value>> {
    let previous: Option<i64> = sqlx::query_scalar(
        "SELECT seq FROM public_records WHERE session_id = ? AND kind = 'item' AND id = ?",
    )
    .bind(id)
    .bind(item["id"].as_str())
    .fetch_optional(&mut **tx)
    .await?;
    save(&mut **tx, id, "item", item, turn_id).await?;
    let output_index: i64 = sqlx::query_scalar("SELECT count(*) - 1 FROM public_records WHERE session_id = ? AND kind = 'item' AND turn_id = ? AND coalesce(json_extract(data, '$.role'), '') != 'user' AND json_extract(data, '$.type') != 'function_call_output' AND seq <= (SELECT seq FROM public_records WHERE session_id = ? AND kind = 'item' AND id = ?)")
        .bind(id).bind(turn_id).bind(id).bind(item["id"].as_str()).fetch_one(&mut **tx).await?;
    let input = item["role"] == "user" || item["type"] == "function_call_output";
    let mut events = Vec::new();
    let mut event = json!({"session_id":id,"turn_id":turn_id,"item":item,"output_index":if input {Value::Null} else {json!(output_index)}});
    if previous.is_none() {
        event["type"] = json!("agent.session.turn.item.added");
        events.push(event.clone());
    }
    if done && item["role"] == "assistant" {
        events.push(json!({"type":"agent.session.turn.output_text.done","session_id":id,"turn_id":turn_id,"item_id":item["id"],"output_index":output_index,"content_index":0,"text":item["content"][0]["text"]}));
    }
    if done && !input {
        event["type"] = json!("agent.session.turn.item.done");
        events.push(event);
    }
    Ok(events)
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Page {
    after: Option<String>,
    limit: Option<u32>,
    order: Option<String>,
    turn_id: Option<String>,
}

async fn list(state: &State, id: &str, kind: &str, page: Page) -> Result<Json<Value>, ApiError> {
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
        Some(sqlx::query_scalar("SELECT seq FROM public_records WHERE session_id = ? AND kind = ? AND id = ? AND (? IS NULL OR turn_id = ?)").bind(id).bind(kind).bind(after).bind(&page.turn_id).bind(&page.turn_id).fetch_optional(&state.store.0).await.map_err(anyhow::Error::from)?.ok_or_else(|| crate::contract::invalid("invalid after cursor"))?)
    } else {
        None
    };
    let query = if order == "asc" {
        "SELECT data FROM public_records WHERE session_id = ? AND kind = ? AND (? IS NULL OR seq > ?) AND (? IS NULL OR turn_id = ?) ORDER BY seq ASC LIMIT ?"
    } else {
        "SELECT data FROM public_records WHERE session_id = ? AND kind = ? AND (? IS NULL OR seq < ?) AND (? IS NULL OR turn_id = ?) ORDER BY seq DESC LIMIT ?"
    };
    let rows: Vec<String> = sqlx::query_scalar(query)
        .bind(id)
        .bind(kind)
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
    list(&state, &id, "item", page).await
}
pub(crate) async fn turns(
    Extract(state): Extract<Arc<State>>,
    Path(id): Path<String>,
    Query(page): Query<Page>,
) -> Result<Json<Value>, ApiError> {
    list(&state, &id, "turn", page).await
}
pub(crate) async fn turn(
    Extract(state): Extract<Arc<State>>,
    Path((id, turn_id)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    let data: Option<String> = sqlx::query_scalar(
        "SELECT data FROM public_records WHERE session_id = ? AND kind = 'turn' AND id = ?",
    )
    .bind(id)
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
