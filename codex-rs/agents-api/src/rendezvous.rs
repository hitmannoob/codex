//! Rendezvous: routes relay frames between an environment's executor socket and
//! the harness sockets that reach it. Frames are `RelayMessageFrame` protobufs
//! whose payloads are Noise ciphertext; the relay reads only the `stream_id`
//! (field 2) and whether a frame resets its stream (field 8), and never sees
//! plaintext. Each stream belongs to the harness socket that first used it.
use crate::State;
use crate::registry::ExecutorState;
use crate::registry::error;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State as Extract;
use axum::extract::WebSocketUpgrade;
use axum::extract::ws::Message;
use axum::extract::ws::WebSocket;
use axum::http::StatusCode;
use axum::response::Response;
use futures::SinkExt;
use futures::StreamExt;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::mpsc;

/// The exec-server's largest relay frame.
const MAX_FRAME_BYTES: usize = 256 * 1024;
/// Frames buffered per socket before a slow peer is disconnected.
const SOCKET_BUFFER: usize = 256;
/// Harness sockets per environment.
const MAX_HARNESSES: usize = 256;

#[derive(Deserialize)]
pub(crate) struct ExecutorQuery {
    registration: String,
    token: String,
}

#[derive(Deserialize)]
pub(crate) struct HarnessQuery {
    authorization: String,
}

/// The executor's socket. A stale registration or token is refused with 401,
/// which makes the executor register again.
pub(crate) async fn executor(
    Extract(state): Extract<Arc<State>>,
    Path(environment_id): Path<String>,
    Query(query): Query<ExecutorQuery>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let current = state.registry.with_slot(&environment_id, |slot| {
        slot.registration.as_ref().is_some_and(|registration| {
            registration.id == query.registration && registration.executor_token == query.token
        })
    }) == Some(true);
    if !current {
        return error(
            StatusCode::UNAUTHORIZED,
            "registration_rejected",
            "register again",
        );
    }
    upgrade
        .max_message_size(MAX_FRAME_BYTES)
        .on_upgrade(move |socket| run_executor(state, environment_id, socket))
}

async fn run_executor(state: Arc<State>, environment_id: String, socket: WebSocket) {
    let (sender, mut outbound) = mpsc::channel::<Vec<u8>>(SOCKET_BUFFER);
    let generation = state.registry.next_connection();
    // A newer connection replaces an older one; dropping the old sender
    // closes it. A deleted environment has no slot, and the socket closes.
    if state
        .registry
        .with_slot(&environment_id, |slot| {
            slot.executor = Some((generation, sender));
            slot.set_state(ExecutorState::Connected);
        })
        .is_none()
    {
        return;
    }
    tracing::info!(environment_id, "executor connected");
    let (mut sink, mut stream) = socket.split();
    loop {
        tokio::select! {
            frame = outbound.recv() => match frame {
                Some(frame) => if sink.send(Message::Binary(frame.into())).await.is_err() { break },
                None => break,
            },
            message = stream.next() => match message {
                Some(Ok(Message::Binary(frame))) => to_harness(&state, &environment_id, frame.to_vec()),
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                Some(Ok(_)) => {}
            },
        }
    }
    // Streams through this executor are over; tell each harness.
    let still_current = state.registry.with_slot(&environment_id, |slot| {
        let current = slot
            .executor
            .as_ref()
            .is_some_and(|(current, _)| *current == generation);
        if current {
            slot.executor = None;
            for (stream_id, owner) in slot.streams.drain() {
                if let Some(harness) = slot.harnesses.get(&owner) {
                    let _ = harness.try_send(reset(&stream_id, "environment_disconnected"));
                }
            }
            if slot.registration.is_some() {
                slot.set_state(ExecutorState::Registered);
            }
        }
        current
    }) == Some(true);
    if still_current {
        tracing::warn!(environment_id, "executor disconnected");
    }
}

/// Route an executor frame to the harness that owns its stream.
fn to_harness(state: &State, environment_id: &str, frame: Vec<u8>) {
    let Some((stream_id, resets)) = stream_of(&frame) else {
        return;
    };
    state.registry.with_slot(environment_id, |slot| {
        let Some(owner) = slot.streams.get(&stream_id).copied() else {
            return;
        };
        if resets {
            slot.streams.remove(&stream_id);
        }
        if let Some(harness) = slot.harnesses.get(&owner)
            && harness.try_send(frame).is_err()
        {
            // A harness that cannot keep up is dropped; it reconnects.
            slot.harnesses.remove(&owner);
        }
    });
}

/// A harness socket, authorized by the token `connect` issued.
pub(crate) async fn harness(
    Extract(state): Extract<Arc<State>>,
    Path(environment_id): Path<String>,
    Query(query): Query<HarnessQuery>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let admitted = state.registry.with_slot(&environment_id, |slot| {
        slot.authorizes(&query.authorization) && slot.harnesses.len() < MAX_HARNESSES
    }) == Some(true);
    if !admitted {
        return error(
            StatusCode::UNAUTHORIZED,
            "harness_rejected",
            "connect again",
        );
    }
    upgrade
        .max_message_size(MAX_FRAME_BYTES)
        .on_upgrade(move |socket| run_harness(state, environment_id, socket))
}

async fn run_harness(state: Arc<State>, environment_id: String, socket: WebSocket) {
    let (sender, mut outbound) = mpsc::channel::<Vec<u8>>(SOCKET_BUFFER);
    let connection = state.registry.next_connection();
    if state
        .registry
        .with_slot(&environment_id, |slot| {
            slot.harnesses.insert(connection, sender)
        })
        .is_none()
    {
        return;
    }
    let (mut sink, mut stream) = socket.split();
    loop {
        tokio::select! {
            frame = outbound.recv() => match frame {
                Some(frame) => if sink.send(Message::Binary(frame.into())).await.is_err() { break },
                None => break,
            },
            message = stream.next() => match message {
                Some(Ok(Message::Binary(frame))) => to_executor(&state, &environment_id, connection, frame.to_vec()),
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                Some(Ok(_)) => {}
            },
        }
    }
    // The executor would otherwise keep this harness's streams open.
    state.registry.with_slot(&environment_id, |slot| {
        slot.harnesses.remove(&connection);
        let owned: Vec<String> = slot
            .streams
            .iter()
            .filter(|(_, owner)| **owner == connection)
            .map(|(stream_id, _)| stream_id.clone())
            .collect();
        for stream_id in owned {
            slot.streams.remove(&stream_id);
            if let Some((_, executor)) = &slot.executor {
                let _ = executor.try_send(reset(&stream_id, "harness_disconnected"));
            }
        }
    });
}

/// Route a harness frame to the executor, claiming its stream for this
/// harness. A stream another harness owns, or an offline executor, gets a
/// reset back.
fn to_executor(state: &State, environment_id: &str, connection: u64, frame: Vec<u8>) {
    let Some((stream_id, resets)) = stream_of(&frame) else {
        return;
    };
    state.registry.with_slot(environment_id, |slot| {
        let refusal = match slot.streams.get(&stream_id) {
            Some(owner) if *owner != connection => Some("stream_in_use"),
            _ if slot.executor.is_none() => Some("environment_offline"),
            _ => None,
        };
        if let Some(reason) = refusal {
            if let Some(harness) = slot.harnesses.get(&connection) {
                let _ = harness.try_send(reset(&stream_id, reason));
            }
            return;
        }
        if resets {
            slot.streams.remove(&stream_id);
        } else {
            slot.streams.insert(stream_id, connection);
        }
        if let Some((_, executor)) = &slot.executor {
            let _ = executor.try_send(frame);
        }
    });
}

/// A frame's `stream_id` (field 2) and whether its body is a reset (field 8).
fn stream_of(frame: &[u8]) -> Option<(String, bool)> {
    let mut stream_id = None;
    let mut resets = false;
    let mut rest = frame;
    while !rest.is_empty() {
        let key = varint(&mut rest)?;
        let field = key >> 3;
        match key & 7 {
            0 => {
                varint(&mut rest)?;
            }
            1 => rest = rest.get(8..)?,
            2 => {
                let length = usize::try_from(varint(&mut rest)?).ok()?;
                let value = rest.get(..length)?;
                rest = &rest[length..];
                match field {
                    2 => stream_id = Some(String::from_utf8(value.to_vec()).ok()?),
                    8 => resets = true,
                    _ => {}
                }
            }
            5 => rest = rest.get(4..)?,
            _ => return None,
        }
    }
    stream_id
        .filter(|stream_id| !stream_id.is_empty())
        .map(|stream_id| (stream_id, resets))
}

fn varint(bytes: &mut &[u8]) -> Option<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let (&byte, rest) = bytes.split_first()?;
        *bytes = rest;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
    }
    None
}

fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn put_bytes(out: &mut Vec<u8>, field: u64, bytes: &[u8]) {
    put_varint(out, (field << 3) | 2);
    put_varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

/// A `RelayMessageFrame{version: 1, stream_id, reset: {reason}}`.
fn reset(stream_id: &str, reason: &str) -> Vec<u8> {
    let mut body = Vec::new();
    put_bytes(&mut body, 1, reason.as_bytes());
    let mut frame = vec![0x08, 0x01];
    put_bytes(&mut frame, 2, stream_id.as_bytes());
    put_bytes(&mut frame, 8, &body);
    frame
}

#[cfg(test)]
#[path = "rendezvous_tests.rs"]
mod tests;
