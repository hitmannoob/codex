"""Acceptance against openai==3.17.0; invoked by the ignored Rust SDK test."""

import json
import sys
import time

import openai

assert openai.__version__ == "3.17.0", openai.__version__
client = openai.OpenAI(
    base_url=sys.argv[1],
    api_key="test-token-for-the-local-agents-api",
    max_retries=0,
    timeout=20,
    _strict_response_validation=True,
)
sessions = client.beta.agents.sessions


def finish(stream):
    seen = []
    for event in stream:
        assert event.event_id
        seen.append(event.type)
        if event.type == "agent.session.idle":
            assert "agent.session.turn.completed" in seen, seen
            return seen
    raise AssertionError("stream ended before completion")


def message(text):
    return {
        "type": "agent.session.input.message",
        "input": [{"role": "user", "content": [{"type": "input_text", "text": text}]}],
    }


if len(sys.argv) == 3:
    session_id = sys.argv[2]
    assert sessions.retrieve(session_id).status == "idle"
    assert len(list(sessions.turns.list(session_id))) == 3
    with sessions.events.stream(session_id) as stream:
        sessions.events.create(
            session_id, events=[message("Continue after service restart.")]
        )
        finish(stream)
    print(json.dumps({"session_id": session_id, "restarted": True}))
    sys.exit(0)

agent = client.beta.agents.create(
    model="mock-model",
    instructions="Use the lookup tool.",
    tools=[
        {
            "type": "function",
            "name": "lookup",
            "description": "Look up a code",
            "parameters": {"type": "object", "properties": {}},
        }
    ],
)
assert client.beta.agents.retrieve(agent.id) == agent
with sessions.create(
    agent_id=agent.id,
    environment={"type": "none"},
    input="Remember orange-731",
    stream=True,
) as stream:
    for event in stream:
        assert event.event_id
        if event.type == "agent.session.requires_action":
            session_id = event.session.id
            break
    else:
        raise AssertionError("no required action")

# The application can disconnect while a function is waiting, then recover its IDs.
print("SDK: pending function", file=sys.stderr, flush=True)
pending = sessions.retrieve(session_id)
assert pending.status == "requires_action"
action = pending.required_actions[0]
assert action.type == "function_call" and action.name == "lookup"
result = {
    "type": "agent.session.input.tool_result",
    "turn_id": action.turn_id,
    "call_id": action.call_id,
    "success": True,
    "output": "sdk-result-731",
}
with sessions.events.stream(session_id) as stream:
    sessions.events.create(session_id, events=[result])
    sessions.events.create(
        session_id, events=[result]
    )  # Duplicate identical result is safe.
    seen = finish(stream)
assert "agent.session.turn.item.done" in seen
print("SDK: read history", file=sys.stderr, flush=True)
items = list(sessions.items.list(session_id, limit=1, order="asc"))
assert len({item.id for item in items}) == len(items)
assert any(
    item.type == "function_call_output" and item.output == "sdk-result-731"
    for item in items
), items
assert any(item.type == "message" and item.role == "assistant" for item in items), items
turns = list(sessions.turns.list(session_id, limit=1))
assert len(turns) == 1 and turns[0].status == "completed"
assert sessions.turns.retrieve(turns[0].id, session_id=session_id) == turns[0]

with sessions.events.stream(session_id) as stream:
    sessions.events.create(session_id, events=[message("Look up another code.")])
    for event in stream:
        if event.type == "agent.session.requires_action":
            action = event.session.required_actions[0]
            sessions.events.create(
                session_id,
                events=[
                    {
                        "type": "agent.session.input.tool_result",
                        "turn_id": action.turn_id,
                        "call_id": action.call_id,
                        "success": False,
                        "error": "lookup unavailable",
                    }
                ],
            )
            break
    finish(stream)
assert any(
    item.type == "function_call_output" and item.error == "lookup unavailable"
    for item in sessions.items.list(session_id)
)
assert (
    len(
        [
            item
            for item in sessions.items.list(session_id)
            if item.type == "message" and item.role == "assistant"
        ]
    )
    == 2
)

# Saved-agent overrides belong to the new session, not the reusable agent.
print("SDK: snapshot override", file=sys.stderr, flush=True)
inline = sessions.create(
    agent_id=agent.id,
    agent={"instructions": None, "tools": None},
    environment={"type": "none"},
    input="Independent conversation",
)
assert inline.agent.instructions == "" and inline.agent.tools == []
assert client.beta.agents.retrieve(agent.id) == agent
deadline = time.monotonic() + 10
while sessions.retrieve(inline.id).status != "idle":
    assert time.monotonic() < deadline
    time.sleep(0.02)
assert len(list(sessions.turns.list(inline.id))) == 1

# Cancel a stalled model request, and preserve the outcome in history.
print("SDK: cancel turn", file=sys.stderr, flush=True)
with sessions.events.stream(session_id) as stream:
    sessions.events.create(session_id, events=[message("sdk-cancel-input")])
    for event in stream:
        if event.type == "agent.session.turn.in_progress":
            sessions.events.create(
                session_id, events=[message("Keep this turn concise.")]
            )
            sessions.events.create(
                session_id, events=[{"type": "agent.session.input.cancel"}]
            )
        if event.type == "agent.session.turn.cancelled":
            cancelled = event.turn
        if event.type == "agent.session.idle":
            break
assert (
    sessions.turns.retrieve(cancelled.id, session_id=session_id).status == "cancelled"
)
assert len(list(sessions.turns.list(session_id))) == 3

try:
    sessions.events.create(
        session_id,
        events=[message("must not execute")],
        idempotency_key="unsupported-key",
    )
    raise AssertionError("idempotency key silently ignored")
except openai.BadRequestError:
    pass

try:
    sessions.events.create(
        session_id,
        events=[{"type": "agent.session.input.message", "input": [{"content": []}]}],
    )
    raise AssertionError("invalid message accepted")
except openai.BadRequestError as error:
    assert error.body["type"] == "invalid_request_error"

for bad in [
    {
        "environment": {"type": "none"},
        "agent": {"model": "mock-model"},
        "input": "x",
        "vault_ids": ["secret"],
    },
    {
        "environment": {"type": "none"},
        "agent": {"model": "mock-model", "text": {"verbosity": "low"}},
        "input": "x",
    },
]:
    try:
        sessions.create(**bad)
        raise AssertionError("unsupported option accepted")
    except openai.BadRequestError as error:
        assert error.body["type"] == "invalid_request_error"

print(
    json.dumps(
        {"session_id": session_id, "items": len(items), "sdk": openai.__version__}
    )
)
