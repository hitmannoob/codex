//! Best-effort token usage. Codex reports a cumulative total per thread; each
//! update attributes only its increase to the running turn, so repeated and
//! replayed totals are never counted twice.
use serde_json::Value;
use serde_json::json;

/// Input, cached input, output, reasoning output, and total tokens.
type Tokens = [i64; 5];

fn tokens(breakdown: &Value) -> Tokens {
    [
        "inputTokens",
        "cachedInputTokens",
        "outputTokens",
        "reasoningOutputTokens",
        "totalTokens",
    ]
    .map(|field| breakdown[field].as_i64().unwrap_or(0))
}

/// Attribute a usage update's increase over the session's stored total to its
/// turn. An update for a turn that is no longer running is a replay (Codex
/// resends the latest total when a thread is resumed), so it only moves the
/// stored total. Without a usable stored total, only the latest response counts.
pub(crate) async fn record(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    id: &str,
    turn_id: &str,
    reported: &Value,
) -> anyhow::Result<()> {
    let total = tokens(&reported["total"]);
    let previous: Option<(i64, i64, i64, i64, i64)> = sqlx::query_as("SELECT input_tokens, cached_tokens, output_tokens, reasoning_tokens, total_tokens FROM usage_totals WHERE session_id = ?")
        .bind(id).fetch_optional(&mut **tx).await?;
    sqlx::query("INSERT INTO usage_totals (session_id, input_tokens, cached_tokens, output_tokens, reasoning_tokens, total_tokens) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(session_id) DO UPDATE SET input_tokens = excluded.input_tokens, cached_tokens = excluded.cached_tokens, output_tokens = excluded.output_tokens, reasoning_tokens = excluded.reasoning_tokens, total_tokens = excluded.total_tokens")
        .bind(id).bind(total[0]).bind(total[1]).bind(total[2]).bind(total[3]).bind(total[4]).execute(&mut **tx).await?;
    let running: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM public_records WHERE session_id = ? AND kind = 'turn' AND id = ? AND json_extract(data, '$.status') IN ('queued', 'in_progress', 'waiting'))")
        .bind(id).bind(turn_id).fetch_one(&mut **tx).await?;
    if !running {
        return Ok(());
    }
    let increase = match previous {
        Some((input, cached, output, reasoning, all))
            if total
                .iter()
                .zip([input, cached, output, reasoning, all])
                .all(|(now, before)| *now >= before) =>
        {
            [
                total[0] - input,
                total[1] - cached,
                total[2] - output,
                total[3] - reasoning,
                total[4] - all,
            ]
        }
        _ => tokens(&reported["last"]),
    };
    if increase.iter().all(|count| *count == 0) {
        return Ok(());
    }
    sqlx::query("INSERT INTO turn_usage (session_id, turn_id, input_tokens, cached_tokens, output_tokens, reasoning_tokens, total_tokens) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(session_id, turn_id) DO UPDATE SET input_tokens = input_tokens + excluded.input_tokens, cached_tokens = cached_tokens + excluded.cached_tokens, output_tokens = output_tokens + excluded.output_tokens, reasoning_tokens = reasoning_tokens + excluded.reasoning_tokens, total_tokens = total_tokens + excluded.total_tokens")
        .bind(id).bind(turn_id).bind(increase[0]).bind(increase[1]).bind(increase[2]).bind(increase[3]).bind(increase[4]).execute(&mut **tx).await?;
    let turn_usage = usage(tx, id, Some(turn_id)).await?;
    sqlx::query("UPDATE public_records SET data = json_set(data, '$.usage', json(?)) WHERE session_id = ? AND kind = 'turn' AND id = ?")
        .bind(turn_usage.to_string()).bind(id).bind(turn_id).execute(&mut **tx).await?;
    let session_usage = usage(tx, id, /*turn_id*/ None).await?;
    sqlx::query(
        "UPDATE public_sessions SET data = json_set(data, '$.usage', json(?)) WHERE id = ?",
    )
    .bind(session_usage.to_string())
    .bind(id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Sum attributed usage for a session, or one of its turns. `null` when nothing
/// was recorded: missing usage is unknown, not zero.
pub(crate) async fn usage(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    id: &str,
    turn_id: Option<&str>,
) -> anyhow::Result<Value> {
    let (turns, input, cached, output, reasoning, total): (i64, i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT count(*), coalesce(sum(input_tokens), 0), coalesce(sum(cached_tokens), 0), coalesce(sum(output_tokens), 0), coalesce(sum(reasoning_tokens), 0), coalesce(sum(total_tokens), 0) FROM turn_usage WHERE session_id = ? AND (? IS NULL OR turn_id = ?)",
    )
    .bind(id)
    .bind(turn_id)
    .bind(turn_id)
    .fetch_one(&mut **tx)
    .await?;
    Ok(if turns == 0 {
        Value::Null
    } else {
        json!({"input_tokens":input,"input_tokens_details":{"cached_tokens":cached},
            "output_tokens":output,"output_tokens_details":{"reasoning_tokens":reasoning},"total_tokens":total})
    })
}
