use super::*;
use pretty_assertions::assert_eq;
use std::time::Duration;

#[tokio::test]
async fn one_session_waits_while_independent_sessions_proceed() {
    let gates = Arc::new(Gates::default());
    let first = gates.lock("a").await;
    // An independent session is admitted while "a" is held.
    let other = tokio::time::timeout(Duration::from_secs(/*secs*/ 1), gates.lock("b"))
        .await
        .expect("independent session admitted");
    let waiting = tokio::spawn({
        let gates = Arc::clone(&gates);
        async move { drop(gates.lock("a").await) }
    });
    tokio::time::sleep(Duration::from_millis(/*millis*/ 50)).await;
    assert!(!waiting.is_finished(), "same session admitted concurrently");
    drop(first);
    waiting.await.expect("waiter completes");
    drop(other);
    // Released gates are pruned on the next admission.
    drop(gates.lock("c").await);
    assert_eq!(crate::lock(&gates.0).len(), 1);
}
