//! Transient stream events: output-text, reasoning-summary, and command-output
//! deltas, the content-part events that frame them, and `error` events. None of these are
//! persisted: a finished item's saved record carries its complete text, and a
//! failed turn's record carries its error.
use crate::State;
use serde_json::Value;
use serde_json::json;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Message,
    Reasoning,
    Command,
}

/// What deltas need about an item while it streams.
struct Stream {
    kind: Kind,
    session_id: String,
    turn_id: String,
    item_id: String,
    output_index: i64,
    /// Content (message) or summary (reasoning) indexes already announced.
    parts: BTreeSet<i64>,
}

impl Stream {
    fn new(session_id: &str, item: &Value, output_index: i64) -> Option<Self> {
        let kind = if item["role"] == "assistant" {
            Kind::Message
        } else if item["type"] == "reasoning" {
            Kind::Reasoning
        } else if item["type"] == "command_execution" {
            Kind::Command
        } else {
            return None;
        };
        Some(Self {
            kind,
            session_id: session_id.to_owned(),
            turn_id: item["turn_id"].as_str()?.to_owned(),
            item_id: item["id"].as_str()?.to_owned(),
            output_index,
            parts: BTreeSet::new(),
        })
    }

    fn event(&self, kind: &str, fields: Value) -> Value {
        let mut event = json!({"type":format!("agent.session.turn.{kind}"),"session_id":self.session_id,
            "turn_id":self.turn_id,"item_id":self.item_id,"output_index":self.output_index});
        if let (Some(event), Value::Object(fields)) = (event.as_object_mut(), fields) {
            event.extend(fields);
        }
        event
    }

    /// Announce a content or summary part once, before any of its text.
    fn part_added(&mut self, index: i64) -> Option<Value> {
        if !self.parts.insert(index) {
            return None;
        }
        Some(match self.kind {
            Kind::Message => self.event(
                "content_part.added",
                json!({"content_index":index,"part":{"type":"output_text","text":""}}),
            ),
            Kind::Reasoning => self.event(
                "reasoning_summary_part.added",
                json!({"summary_index":index,"part":{"type":"summary_text","text":""}}),
            ),
            // Command output has no parts.
            Kind::Command => return None,
        })
    }
}

/// Streaming items keyed by (thread ID, Codex item ID). An entry lives from the
/// item's start until it finishes, its turn ends, or the connection is lost.
#[derive(Default)]
pub(crate) struct Streams(Mutex<HashMap<(String, String), Stream>>);

impl Streams {
    /// Begin streaming a newly added item; returns the events that open it.
    pub(crate) fn started(
        &self,
        thread_id: &str,
        codex_id: &str,
        session_id: &str,
        item: &Value,
        output_index: i64,
    ) -> Vec<Value> {
        let Some(mut stream) = Stream::new(session_id, item, output_index) else {
            return Vec::new();
        };
        // A message has one output-text part, announced with the item.
        let events = match stream.kind {
            Kind::Message => stream.part_added(/*index*/ 0).into_iter().collect(),
            Kind::Reasoning | Kind::Command => Vec::new(),
        };
        crate::lock(&self.0).insert((thread_id.to_owned(), codex_id.to_owned()), stream);
        events
    }

    /// Finish an item; returns the events that close its parts, which precede
    /// the item's own `item.done`.
    pub(crate) fn finished(
        &self,
        thread_id: &str,
        codex_id: &str,
        session_id: &str,
        item: &Value,
        output_index: i64,
    ) -> Vec<Value> {
        let removed = crate::lock(&self.0).remove(&(thread_id.to_owned(), codex_id.to_owned()));
        let Some(mut stream) = removed.or_else(|| Stream::new(session_id, item, output_index))
        else {
            return Vec::new();
        };
        let mut events = Vec::new();
        match stream.kind {
            Kind::Message => {
                let text = &item["content"][0]["text"];
                events.extend(stream.part_added(/*index*/ 0));
                events
                    .push(stream.event("output_text.done", json!({"content_index":0,"text":text})));
                events.push(stream.event(
                    "content_part.done",
                    json!({"content_index":0,"part":{"type":"output_text","text":text}}),
                ));
            }
            Kind::Reasoning => {
                for (index, part) in item["summary"].as_array().into_iter().flatten().enumerate() {
                    let index = index as i64;
                    events.extend(stream.part_added(index));
                    events.push(stream.event(
                        "reasoning_summary_text.done",
                        json!({"summary_index":index,"text":part["text"]}),
                    ));
                    events.push(stream.event(
                        "reasoning_summary_part.done",
                        json!({"summary_index":index,"part":part}),
                    ));
                }
            }
            // The saved item carries the complete output.
            Kind::Command => {}
        }
        events
    }

    /// Forget every item of a turn that reached a terminal state.
    pub(crate) fn end_turn(&self, session_id: &str, turn_id: &str) {
        crate::lock(&self.0)
            .retain(|_, stream| stream.session_id != session_id || stream.turn_id != turn_id);
    }

    /// Forget everything; the connection that produced these items is gone.
    pub(crate) fn clear(&self) {
        crate::lock(&self.0).clear();
    }

    /// Translate one delta notification into its public events.
    fn delta(&self, method: &str, params: &Value) -> Vec<Value> {
        let key = (
            params["threadId"].as_str().unwrap_or_default().to_owned(),
            params["itemId"].as_str().unwrap_or_default().to_owned(),
        );
        let mut streams = crate::lock(&self.0);
        // Deltas for an item this connection never saw start are dropped.
        let Some(stream) = streams.get_mut(&key) else {
            return Vec::new();
        };
        if stream.kind == Kind::Command {
            return vec![
                json!({"type":"agent.output.command_execution_output.delta","session_id":stream.session_id,
                "turn_id":stream.turn_id,"item_id":stream.item_id,"output_index":stream.output_index,"delta":params["delta"]}),
            ];
        }
        let index = params["summaryIndex"].as_i64().unwrap_or(0);
        let mut events: Vec<Value> = stream.part_added(index).into_iter().collect();
        match (method, stream.kind) {
            ("item/agentMessage/delta", Kind::Message) => events.push(stream.event(
                "output_text.delta",
                json!({"content_index":index,"delta":params["delta"]}),
            )),
            ("item/reasoning/summaryTextDelta", Kind::Reasoning) => events.push(stream.event(
                "reasoning_summary_text.delta",
                json!({"summary_index":index,"delta":params["delta"]}),
            )),
            // A part announcement carries no text of its own.
            _ => {}
        }
        events
    }
}

/// Emit transient events for delta and error notifications. Nothing here is
/// persisted, so a failure loses only live detail that saved records restore.
pub(crate) async fn notification(state: &State, raw: &Value) -> anyhow::Result<()> {
    let params = &raw["params"];
    let events = match raw["method"].as_str().unwrap_or_default() {
        method @ ("item/agentMessage/delta"
        | "item/reasoning/summaryTextDelta"
        | "item/reasoning/summaryPartAdded"
        | "item/commandExecution/outputDelta") => state.streams.delta(method, params),
        // Retried errors do not end the turn; a final one precedes `turn.failed`.
        "error" if params["willRetry"] == false => {
            let session: Option<String> = sqlx::query_scalar(
                "SELECT p.id FROM sessions s JOIN public_sessions p ON p.id = s.id WHERE s.thread_id = ?",
            )
            .bind(params["threadId"].as_str())
            .fetch_optional(&state.store.0)
            .await?;
            let error = crate::turns::turn_error(&params["error"]);
            session
                .map(|session| {
                    json!({"type":"error","session_id":session,"error":{
                        "type":"error","code":error["code"],"message":error["message"],"param":null}})
                })
                .into_iter()
                .collect()
        }
        _ => Vec::new(),
    };
    for event in events {
        crate::records::emit(state, event);
    }
    Ok(())
}
