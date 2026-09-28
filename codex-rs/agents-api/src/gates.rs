//! Per-session input admission. Work for one session (thread bootstrap, turn
//! submission, deletion checks) never interleaves, while independent sessions
//! proceed concurrently.
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use tokio::sync::OwnedMutexGuard;

/// Async mutexes keyed by session ID. An entry lives only while a caller holds
/// or awaits its mutex, so the map stays bounded by in-flight sessions.
#[derive(Default)]
pub(crate) struct Gates(Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>);

impl Gates {
    pub(crate) async fn lock(&self, id: &str) -> OwnedMutexGuard<()> {
        let gate = {
            let mut gates = crate::lock(&self.0);
            gates.retain(|_, gate| gate.strong_count() > 0);
            if let Some(gate) = gates.get(id).and_then(Weak::upgrade) {
                gate
            } else {
                let gate = Arc::new(tokio::sync::Mutex::new(()));
                gates.insert(id.to_owned(), Arc::downgrade(&gate));
                gate
            }
        };
        gate.lock_owned().await
    }
}

#[cfg(test)]
#[path = "gates_tests.rs"]
mod tests;
