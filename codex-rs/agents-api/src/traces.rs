//! Session trace export. Each finished root turn is one OTLP trace, shaped as
//! the tracing guide describes: an agent span for the root agent and for each
//! subagent that ran during the turn, with generation spans for model
//! responses and tool spans for calls beneath the agent that made them. Traces
//! are built from saved records on request, so span IDs are stable across
//! exports. Attribute names follow the OpenTelemetry GenAI conventions where
//! the guide leaves them open.
use crate::ApiError;
use crate::State;
use crate::contract::invalid;
use crate::otlp::attribute;
use crate::otlp::hex;
use crate::otlp::span;
use crate::otlp::span_id;
use axum::Json;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State as Extract;
use serde_json::Value;
use serde_json::json;
use sha2::Digest;
use sha2::Sha256;
use std::sync::Arc;
use uuid::Uuid;

const SERVICE: &str = "codex-agents-api";

/// A saved turn or item with the times the service observed it.
struct Record {
    seq: i64,
    id: String,
    turn_id: String,
    subagent_id: Option<String>,
    data: Value,
    started_ms: Option<i64>,
    completed_ms: Option<i64>,
}

type Row = (
    i64,
    String,
    String,
    Option<String>,
    String,
    Option<i64>,
    Option<i64>,
);

impl Record {
    fn from_row(row: Row) -> anyhow::Result<Self> {
        let (seq, id, turn_id, subagent_id, data, started_ms, completed_ms) = row;
        Ok(Self {
            seq,
            id,
            turn_id,
            subagent_id,
            data: serde_json::from_str(&data)?,
            started_ms,
            completed_ms,
        })
    }

    /// When a turn started and ended. Turns saved before timing was recorded
    /// fall back to their second-resolution timestamps.
    fn span(&self) -> (i64, i64) {
        let seconds = |field: &str| self.data[field].as_i64().map(|at| at * 1000);
        let start = self
            .started_ms
            .or_else(|| seconds("started_at"))
            .or_else(|| seconds("created_at"))
            .unwrap_or_default();
        let end = self
            .completed_ms
            .or_else(|| seconds("completed_at"))
            .unwrap_or(start);
        (start, end.max(start))
    }

    fn kind(&self) -> &str {
        self.data["type"].as_str().unwrap_or_default()
    }

    /// Inputs to the model: user messages and function results.
    fn is_input(&self) -> bool {
        self.data["role"] == "user" || self.kind() == "function_call_output"
    }

    /// Calls the model made, as opposed to its messages and reasoning.
    fn is_tool(&self) -> bool {
        !self.is_input() && !matches!(self.kind(), "message" | "reasoning")
    }
}

/// A model response's reported usage.
struct Generation {
    turn_id: String,
    input_tokens: i64,
    output_tokens: i64,
    total_tokens: i64,
}

pub(crate) async fn list(
    Extract(state): Extract<Arc<State>>,
    Path(id): Path<String>,
    Query(query): Query<Vec<(String, String)>>,
) -> Result<Json<Value>, ApiError> {
    let mut after = None;
    let mut limit = 20;
    let mut ascending = false;
    for (key, value) in &query {
        match key.as_str() {
            "after" => after = Some(value.clone()),
            "limit" => {
                limit = value
                    .parse::<i64>()
                    .map_err(|_| invalid("limit must be an integer"))?
                    .clamp(1, 100);
            }
            "order" => {
                ascending = match value.as_str() {
                    "asc" => true,
                    "desc" => false,
                    _ => return Err(invalid("order must be asc or desc")),
                };
            }
            _ => return Err(invalid(format!("unknown query parameter {key}"))),
        }
    }
    let session = crate::records::session(&state, &id).await?;
    let pool = &state.store.0;
    let start = match after {
        Some(after) => sqlx::query_scalar("SELECT seq FROM public_records WHERE session_id = ? AND kind = 'turn' AND subagent_id IS NULL AND id = ?")
            .bind(&id)
            .bind(after.strip_prefix("trace_").unwrap_or_default())
            .fetch_optional(pool)
            .await
            .map_err(anyhow::Error::from)?
            .ok_or_else(|| invalid("invalid after cursor"))?,
        None if ascending => 0,
        None => i64::MAX,
    };
    // Traces are built once a turn ends.
    let rows: Vec<Row> = sqlx::query_as(if ascending {
        "SELECT seq, id, turn_id, subagent_id, data, started_ms, completed_ms FROM public_records WHERE session_id = ? AND kind = 'turn' AND subagent_id IS NULL AND json_extract(data, '$.status') IN ('completed', 'failed', 'cancelled') AND seq > ? ORDER BY seq ASC LIMIT ?"
    } else {
        "SELECT seq, id, turn_id, subagent_id, data, started_ms, completed_ms FROM public_records WHERE session_id = ? AND kind = 'turn' AND subagent_id IS NULL AND json_extract(data, '$.status') IN ('completed', 'failed', 'cancelled') AND seq < ? ORDER BY seq DESC LIMIT ?"
    })
    .bind(&id)
    .bind(start)
    .bind(limit + 1)
    .fetch_all(pool)
    .await
    .map_err(anyhow::Error::from)?;
    let has_more = rows.len() as i64 > limit;
    let mut data = Vec::new();
    for row in rows.into_iter().take(limit as usize) {
        data.push(trace(&state, &session, Record::from_row(row)?).await?);
    }
    Ok(Json(
        json!({"object":"list","first_id":data.first().map(|v| &v["id"]),
        "last_id":data.last().map(|v| &v["id"]),"data":data,"has_more":has_more}),
    ))
}

/// One root turn's trace, with the subagent turns that started while it was
/// the session's latest turn.
async fn trace(state: &State, session: &Value, root: Record) -> anyhow::Result<Value> {
    let pool = &state.store.0;
    let session_id = session["id"].as_str().unwrap_or_default();
    let next: Option<i64> = sqlx::query_scalar("SELECT min(seq) FROM public_records WHERE session_id = ? AND kind = 'turn' AND subagent_id IS NULL AND seq > ?")
        .bind(session_id)
        .bind(root.seq)
        .fetch_one(pool)
        .await?;
    let subagent_turns = sqlx::query_as::<_, Row>("SELECT seq, id, turn_id, subagent_id, data, started_ms, completed_ms FROM public_records WHERE session_id = ? AND kind = 'turn' AND subagent_id IS NOT NULL AND seq > ? AND seq < ? ORDER BY seq")
        .bind(session_id)
        .bind(root.seq)
        .bind(next.unwrap_or(i64::MAX))
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(Record::from_row)
        .collect::<anyhow::Result<Vec<_>>>()?;
    let turn_ids = json!(
        std::iter::once(&root.id)
            .chain(subagent_turns.iter().map(|turn| &turn.id))
            .collect::<Vec<_>>()
    )
    .to_string();
    let items = sqlx::query_as::<_, Row>("SELECT seq, id, turn_id, subagent_id, data, started_ms, completed_ms FROM public_records WHERE session_id = ? AND kind = 'item' AND turn_id IN (SELECT value FROM json_each(?)) ORDER BY seq")
        .bind(session_id)
        .bind(&turn_ids)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(Record::from_row)
        .collect::<anyhow::Result<Vec<_>>>()?;
    let generations = sqlx::query_as::<_, (String, i64, i64, i64)>("SELECT turn_id, input_tokens, output_tokens, total_tokens FROM generations WHERE session_id = ? AND turn_id IN (SELECT value FROM json_each(?)) ORDER BY seq")
        .bind(session_id)
        .bind(&turn_ids)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|(turn_id, input_tokens, output_tokens, total_tokens)| Generation {
            turn_id,
            input_tokens,
            output_tokens,
            total_tokens,
        })
        .collect::<Vec<_>>();
    let trace_id = Uuid::parse_str(&root.id)
        .map(|id| id.simple().to_string())
        .unwrap_or_else(|_| hex(&Sha256::digest(root.id.as_bytes())[..16]));
    let agent = &session["agent"];
    let root_agent = Agent {
        key: "root".into(),
        id: agent["id"].as_str().unwrap_or_default().to_owned(),
        name: agent["name"].as_str().map(str::to_owned),
        kind: "root",
        instructions: agent["instructions"].as_str().map(str::to_owned),
    };
    let root_span = span_id(&trace_id, &root_agent.key);
    let mut spans = agent_spans(
        &trace_id,
        /*parent*/ None,
        &root_agent,
        &[&root],
        &items,
        &generations,
    );
    let mut subagents: Vec<(&str, Vec<&Record>)> = Vec::new();
    for turn in &subagent_turns {
        let subagent = turn.subagent_id.as_deref().unwrap_or_default();
        match subagents.iter_mut().find(|(id, _)| *id == subagent) {
            Some((_, turns)) => turns.push(turn),
            None => subagents.push((subagent, vec![turn])),
        }
    }
    for (subagent, turns) in subagents {
        let name: Option<String> = sqlx::query_scalar(
            "SELECT json_extract(data, '$.name') FROM subagents WHERE session_id = ? AND id = ?",
        )
        .bind(session_id)
        .bind(subagent)
        .fetch_optional(pool)
        .await?
        .flatten();
        let agent = Agent {
            key: format!("subagent:{subagent}"),
            id: subagent.to_owned(),
            name,
            kind: "subagent",
            instructions: None,
        };
        spans.extend(agent_spans(
            &trace_id,
            Some(&root_span),
            &agent,
            &turns,
            &items,
            &generations,
        ));
    }
    let resource = [
        attribute("service.name", &json!(SERVICE)),
        attribute("openai.agents.session_id", &json!(session_id)),
        attribute("openai.agents.turn_id", &json!(root.id)),
    ];
    Ok(
        json!({"id":format!("trace_{}", root.id),"object":"agent.session.trace","session_id":session_id,
        "turn_id":root.id,"created_at":root.span().1 / 1000,"otlp":{"resourceSpans":[{"resource":{"attributes":resource},
        "scopeSpans":[{"scope":{"name":SERVICE},"spans":spans}]}]}}),
    )
}

struct Agent {
    /// Distinguishes the agent's span within its trace.
    key: String,
    id: String,
    name: Option<String>,
    kind: &'static str,
    instructions: Option<String>,
}

/// An agent's span, then the generation and tool spans of its turns beneath it.
fn agent_spans(
    trace_id: &str,
    parent: Option<&str>,
    agent: &Agent,
    turns: &[&Record],
    items: &[Record],
    generations: &[Generation],
) -> Vec<Value> {
    let own = span_id(trace_id, &agent.key);
    let start = turns
        .iter()
        .map(|turn| turn.span().0)
        .min()
        .unwrap_or_default();
    let end = turns
        .iter()
        .map(|turn| turn.span().1)
        .max()
        .unwrap_or(start);
    let last = turns.last().map(|turn| &turn.data).unwrap_or(&Value::Null);
    let status = last["status"].as_str().unwrap_or_default();
    let usage = |field: &str| {
        let counts: Vec<i64> = turns
            .iter()
            .filter_map(|turn| turn.data["usage"][field].as_i64())
            .collect();
        (!counts.is_empty()).then(|| json!(counts.iter().sum::<i64>()))
    };
    let mut attributes = vec![
        attribute("gen_ai.operation.name", &json!("invoke_agent")),
        attribute("gen_ai.agent.id", &json!(agent.id)),
        attribute("openai.agents.agent_type", &json!(agent.kind)),
        attribute("openai.agents.status", &json!(status)),
    ];
    for (key, value) in [
        (
            "gen_ai.agent.name",
            agent.name.as_ref().map(|name| json!(name)),
        ),
        (
            "gen_ai.system_instructions",
            agent.instructions.as_ref().map(|text| json!(text)),
        ),
        ("gen_ai.usage.input_tokens", usage("input_tokens")),
        ("gen_ai.usage.output_tokens", usage("output_tokens")),
        ("openai.agents.usage.total_tokens", usage("total_tokens")),
    ] {
        if let Some(value) = value {
            attributes.push(attribute(key, &value));
        }
    }
    let outcome = match status {
        "completed" => (1, None),
        "failed" => (2, last["error"]["message"].as_str().map(str::to_owned)),
        _ => (0, None),
    };
    let label = agent.name.as_deref().unwrap_or(agent.kind);
    let mut spans = vec![span(
        trace_id,
        &own,
        parent,
        &format!("invoke_agent {label}"),
        (start, end),
        outcome,
        attributes,
    )];
    for turn in turns {
        let (turn_start, turn_end) = turn.span();
        let owned: Vec<&Record> = items
            .iter()
            .filter(|item| item.turn_id == turn.id)
            .collect();
        let times = |item: &Record| {
            let begin = item.started_ms.or(item.completed_ms).unwrap_or(turn_start);
            (begin, item.completed_ms.unwrap_or(turn_end).max(begin))
        };
        // Split the turn's items into model responses, as (inputs, outputs).
        // A response's outputs run until the model has to wait: for an input
        // such as a function result, or for a tool call it made to return,
        // which a message or reasoning after that call shows.
        let mut responses: Vec<(Vec<&Record>, Vec<&Record>)> = Vec::new();
        let mut current: (Vec<&Record>, Vec<&Record>) = (Vec::new(), Vec::new());
        for item in &owned {
            let waited = item.is_input()
                || (!item.is_tool() && current.1.iter().any(|output| output.is_tool()));
            if waited && !current.1.is_empty() {
                responses.push(std::mem::take(&mut current));
            }
            if item.is_input() {
                current.0.push(item);
            } else {
                current.1.push(item);
            }
        }
        if !current.1.is_empty() {
            responses.push(current);
        }
        // Usage arrives once per response, in order; it is attached only when
        // every response has its report.
        let usage: Vec<&Generation> = generations
            .iter()
            .filter(|generation| generation.turn_id == turn.id)
            .collect();
        let mut previous = turn_start;
        for (index, (inputs, outputs)) in responses.iter().enumerate() {
            // A response starts once its latest input is ready, and ends when
            // it produced its last output: a tool call's issue, or the end of
            // a message.
            let returned = index
                .checked_sub(1)
                .map(|prior| {
                    responses[prior]
                        .1
                        .iter()
                        .filter(|item| item.is_tool())
                        .map(|item| times(item).1)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let begin = inputs
                .iter()
                .map(|item| times(item).1)
                .chain(returned)
                .fold(previous, i64::max);
            let end = outputs
                .iter()
                .map(|item| {
                    if item.is_tool() {
                        times(item).0
                    } else {
                        times(item).1
                    }
                })
                .fold(begin, i64::max);
            let mut attributes = vec![
                attribute("gen_ai.operation.name", &json!("chat")),
                attribute(
                    "gen_ai.input.messages",
                    &json!(inputs.iter().map(|item| &item.data).collect::<Vec<_>>()),
                ),
                attribute(
                    "gen_ai.output.messages",
                    &json!(outputs.iter().map(|item| &item.data).collect::<Vec<_>>()),
                ),
            ];
            if usage.len() == responses.len() {
                let reported = usage[index];
                attributes.extend([
                    attribute("gen_ai.usage.input_tokens", &json!(reported.input_tokens)),
                    attribute("gen_ai.usage.output_tokens", &json!(reported.output_tokens)),
                    attribute(
                        "openai.agents.usage.total_tokens",
                        &json!(reported.total_tokens),
                    ),
                ]);
            }
            spans.push(span(
                trace_id,
                &span_id(
                    trace_id,
                    &format!("{}:generation:{}:{index}", agent.key, turn.id),
                ),
                Some(&own),
                "chat",
                (begin, end),
                (1, None),
                attributes,
            ));
            previous = end;
        }
        for item in owned.iter().filter(|item| item.is_tool()) {
            let call = &item.data;
            let call_id = call["call_id"].as_str().or_else(|| call["id"].as_str());
            let result = owned
                .iter()
                .find(|output| {
                    output.kind() == "function_call_output"
                        && output.data["call_id"] == call["call_id"]
                })
                .map(|output| &output.data);
            let name = match item.kind() {
                "function_call" => call["name"].as_str().unwrap_or_default().to_owned(),
                "mcp_call" => format!(
                    "{}.{}",
                    call["server_label"].as_str().unwrap_or_default(),
                    call["name"].as_str().unwrap_or_default()
                ),
                kind => kind.to_owned(),
            };
            let error = result
                .filter(|result| result["status"] == "failed")
                .map(|result| &result["error"])
                .or_else(|| Some(&call["error"]).filter(|error| !error.is_null()));
            let status = call["status"].as_str().unwrap_or_default();
            let outcome = match (error, status) {
                (Some(error), _) => (
                    2,
                    Some(
                        error
                            .as_str()
                            .map_or_else(|| error.to_string(), str::to_owned),
                    ),
                ),
                (None, "completed") => (1, None),
                (None, "failed") => (2, None),
                (None, _) => (0, None),
            };
            let mut attributes = vec![
                attribute("gen_ai.operation.name", &json!("execute_tool")),
                attribute("gen_ai.tool.name", &json!(name)),
                attribute("gen_ai.tool.type", &json!(item.kind())),
                attribute("openai.agents.status", &json!(status)),
                attribute("openai.agents.tool.call", call),
            ];
            if let Some(call_id) = call_id {
                attributes.push(attribute("gen_ai.tool.call.id", &json!(call_id)));
            }
            if let Some(result) = result {
                attributes.push(attribute("openai.agents.tool.result", result));
            }
            spans.push(span(
                trace_id,
                &span_id(trace_id, &format!("{}:tool:{}", agent.key, item.id)),
                Some(&own),
                &format!("execute_tool {name}"),
                times(item),
                outcome,
                attributes,
            ));
        }
    }
    spans
}
