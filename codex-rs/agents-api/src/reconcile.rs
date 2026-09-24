//! Correct work that a connection loss resolved provisionally against the
//! app-server's authoritative rollout history, once a backend reconnects. This
//! covers turns failed only by the disconnect and function calls whose delivery
//! became uncertain; both are decided from authoritative turn status, which
//! `thread/turns/list` serves from persisted rollout history (so no resume is
//! needed on a freshly restarted worker).
use crate::State;
use serde_json::Value;
use serde_json::json;
use std::collections::BTreeSet;
use std::collections::HashMap;

/// Error code that [`crate::records::disconnected`] stamps on turns failed only
/// because the backend connection dropped. Reconciliation revisits exactly
/// these turns; a turn the model genuinely failed carries a different code, so
/// it is never re-examined here.
pub(crate) const CONNECTION_LOST_CODE: &str = "connection_failed";

/// Reconcile every session left with provisional state by a disconnect. A no-op
/// on the common fresh-start case where nothing was interrupted. Best-effort: a
/// session whose thread cannot be listed keeps its provisional state.
pub(crate) async fn run(state: &State) -> anyhow::Result<()> {
    // Sessions whose turns were failed only by the connection loss, plus those
    // holding function calls whose delivery the loss left uncertain.
    let mut sessions: BTreeSet<String> = sqlx::query_scalar(
        "SELECT DISTINCT session_id FROM public_records WHERE kind = 'turn' AND json_extract(data, '$.error.code') = ?",
    )
    .bind(CONNECTION_LOST_CODE)
    .fetch_all(&state.store.0)
    .await?
    .into_iter()
    .collect();
    let calls: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT session_id FROM tool_calls WHERE status IN ('unavailable', 'submitting')",
    )
    .fetch_all(&state.store.0)
    .await?;
    sessions.extend(calls);
    for session_id in sessions {
        if let Err(error) = reconcile_session(state, &session_id).await {
            eprintln!("agents-api: reconciling session {session_id} failed: {error:#}");
        }
    }
    Ok(())
}

async fn reconcile_session(state: &State, session_id: &str) -> anyhow::Result<()> {
    let thread_id: Option<String> =
        sqlx::query_scalar("SELECT thread_id FROM sessions WHERE id = ?")
            .bind(session_id)
            .fetch_optional(&state.store.0)
            .await?
            .flatten();
    let Some(thread_id) = thread_id else {
        return Ok(());
    };
    let response = state
        .rpc(
            "thread/turns/list",
            json!({"threadId": thread_id, "limit": 100, "itemsView": "summary"}),
        )
        .await
        .map_err(|error| anyhow::anyhow!(error.1))?;
    let authoritative: HashMap<&str, &Value> = response["data"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|turn| turn["id"].as_str().map(|id| (id, turn)))
        .collect();

    // Correct provisionally-failed turns: a turn the rollout records completed
    // is recovered; one still unfinished keeps its provisional failure, because
    // an interrupted turn's completion cannot be established.
    let provisional: Vec<String> = sqlx::query_scalar(
        "SELECT id FROM public_records WHERE session_id = ? AND kind = 'turn' AND json_extract(data, '$.error.code') = ?",
    )
    .bind(session_id)
    .bind(CONNECTION_LOST_CODE)
    .fetch_all(&state.store.0)
    .await?;
    let mut recovered_turn = false;
    for turn_id in provisional {
        let Some(turn) = authoritative.get(turn_id.as_str()) else {
            continue; // Never persisted authoritatively; keep it failed.
        };
        let (status, error) = match turn["status"].as_str() {
            Some("completed") => ("completed", Value::Null),
            Some("interrupted") => ("cancelled", Value::Null),
            Some("failed") => (
                "failed",
                json!({"code":"internal_error","message":turn["error"]["message"].as_str().unwrap_or("turn failed")}),
            ),
            // inProgress or unknown: completion cannot be established.
            _ => continue,
        };
        let Some(data): Option<String> = sqlx::query_scalar(
            "SELECT data FROM public_records WHERE session_id = ? AND kind = 'turn' AND id = ?",
        )
        .bind(session_id)
        .bind(&turn_id)
        .fetch_optional(&state.store.0)
        .await?
        else {
            continue;
        };
        let mut record: Value = serde_json::from_str(&data)?;
        record["status"] = json!(status);
        record["error"] = error;
        if let Some(completed_at) = turn["completed_at"].as_i64() {
            record["completed_at"] = json!(completed_at);
        }
        sqlx::query(
            "UPDATE public_records SET data = ? WHERE session_id = ? AND kind = 'turn' AND id = ?",
        )
        .bind(record.to_string())
        .bind(session_id)
        .bind(&turn_id)
        .execute(&state.store.0)
        .await?;
        recovered_turn = true;
    }

    // Recover function calls whose delivery acknowledgment was lost. Only a
    // completed turn is conclusive: a turn cannot complete unless every call it
    // contained was resolved, so the result delivered even though the ack never
    // returned, and the stored receipt is restored. Any other status —
    // interrupted, failed, still open, or absent from history — leaves the call
    // unresolved for the caller to reconcile, never claiming a delivery or
    // recreating a waiter from a state that cannot be established.
    let uncertain: Vec<(String, String)> = sqlx::query_as(
        "SELECT turn_id, call_id FROM tool_calls WHERE session_id = ? AND status IN ('unavailable', 'submitting')",
    )
    .bind(session_id)
    .fetch_all(&state.store.0)
    .await?;
    for (turn_id, call_id) in uncertain {
        if authoritative
            .get(turn_id.as_str())
            .is_none_or(|turn| turn["status"] != "completed")
        {
            continue;
        }
        sqlx::query(
            "UPDATE tool_calls SET status = 'submitted' WHERE session_id = ? AND turn_id = ? AND call_id = ?",
        )
        .bind(session_id)
        .bind(&turn_id)
        .bind(&call_id)
        .execute(&state.store.0)
        .await?;
    }

    if recovered_turn {
        // A resolved terminal turn leaves the session ready for input again,
        // mirroring the live path that idles a session after any terminal turn.
        crate::records::session_status(state, session_id, "idle", /*error*/ None)
            .await
            .map_err(|error| anyhow::anyhow!(error.1))?;
    }
    Ok(())
}
