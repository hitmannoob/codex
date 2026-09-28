use super::*;
use pretty_assertions::assert_eq;

async fn body(response: Response) -> String {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("stream body");
    String::from_utf8(bytes.to_vec()).expect("utf-8 stream")
}

#[tokio::test]
async fn lagged_stream_closes_instead_of_skipping_events() {
    let (sender, receiver) = tokio::sync::broadcast::channel(/*capacity*/ 2);
    for delta in ["a", "b", "c"] {
        sender
            .send(json!({"type":"agent.session.turn.output_text.delta","session_id":"s","delta":delta}))
            .expect("subscribed");
    }
    // The subscriber missed an event, so it gets nothing rather than a gap.
    assert_eq!(body(sse(receiver, "s".into())).await, "");
}

#[tokio::test]
async fn stream_carries_only_its_session_and_ends_on_deletion() {
    let (sender, receiver) = tokio::sync::broadcast::channel(/*capacity*/ 8);
    let own = json!({"type":"agent.session.idle","session":{"id":"s"}});
    for event in [
        json!({"type":"agent.session.idle","session":{"id":"other"}}),
        own.clone(),
        json!({"type":crate::sessions::DELETED,"session_id":"other"}),
        json!({"type":crate::sessions::DELETED,"session_id":"s"}),
        json!({"type":"agent.session.idle","session":{"id":"s"}}),
    ] {
        sender.send(event).expect("subscribed");
    }
    assert_eq!(
        body(sse(receiver, "s".into())).await,
        format!("event: agent.session.idle\ndata: {own}\n\n")
    );
}
