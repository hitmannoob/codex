//! OTLP JSON encoding for exported traces: spans, attributes, and IDs, as
//! the OpenTelemetry protocol's JSON mapping writes them.
use serde_json::Value;
use serde_json::json;
use sha2::Digest;
use sha2::Sha256;

/// The longest string attribute exported; longer values are cut.
const MAX_ATTRIBUTE_BYTES: usize = 32 * 1024;

/// An OTLP JSON span. Times are milliseconds; `outcome` is the status code
/// (0 unset, 1 ok, 2 error) and message.
pub(crate) fn span(
    trace_id: &str,
    span_id: &str,
    parent: Option<&str>,
    name: &str,
    (start, end): (i64, i64),
    (code, message): (i64, Option<String>),
    attributes: Vec<Value>,
) -> Value {
    let mut status = json!({"code":code});
    if let Some(message) = message {
        status["message"] = json!(message);
    }
    json!({"traceId":trace_id,"spanId":span_id,"parentSpanId":parent.unwrap_or_default(),"name":name,
        "kind":1,"startTimeUnixNano":(start * 1_000_000).to_string(),
        "endTimeUnixNano":(end.max(start) * 1_000_000).to_string(),"attributes":attributes,"status":status})
}

/// An OTLP attribute. Integers and booleans keep their types; anything else
/// becomes a string, JSON-encoded unless it is one, cut to
/// [`MAX_ATTRIBUTE_BYTES`].
pub(crate) fn attribute(key: &str, value: &Value) -> Value {
    let value = match value {
        Value::Bool(flag) => json!({"boolValue":flag}),
        Value::Number(number) if number.is_i64() => json!({"intValue":number.to_string()}),
        Value::String(text) => json!({"stringValue":truncate(text)}),
        other => json!({"stringValue":truncate(&other.to_string())}),
    };
    json!({"key":key,"value":value})
}

fn truncate(text: &str) -> String {
    if text.len() <= MAX_ATTRIBUTE_BYTES {
        return text.to_owned();
    }
    let mut end = MAX_ATTRIBUTE_BYTES;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…[truncated]", &text[..end])
}

/// A span ID derived from the trace and a key naming the span within it.
pub(crate) fn span_id(trace_id: &str, key: &str) -> String {
    hex(&Sha256::digest(format!("{trace_id}:{key}").as_bytes())[..8])
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
