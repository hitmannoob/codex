"""Input batch, image, and Idempotency-Key coverage, called from the pinned SDK lifecycle test."""

import sys

import openai
from openai.lib.streaming.agents._tools import is_pending_call_race

from sdk_helpers import message
from sdk_sessions import idle
from sdk_sessions import rejected

# A 1x1 transparent PNG.
PIXEL = (
    "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk"
    "YAAAAAYAAjCB0C8AAAAASUVORK5CYII="
)


def finish(stream):
    seen = []
    for event in stream:
        seen.append(event.type)
        if event.type in ("agent.session.turn.failed", "agent.session.failed"):
            raise AssertionError(event)
        if (
            event.type == "agent.session.idle"
            and "agent.session.turn.completed" in seen
        ):
            return seen
    raise AssertionError("stream ended before completion")


def check_input_semantics(client, agent_id):
    sessions = client.beta.agents.sessions
    worker = sessions.create(
        agent_id=agent_id, environment={"type": "none"}, input="Start the input checks"
    )
    idle(sessions, worker.id)
    print("SDK: stream helper", file=sys.stderr, flush=True)

    # The SDK helper keys its input and tool-result submissions and retries a
    # result rejected by the pending-call registration race.
    calls = []

    def lookup(arguments):
        calls.append(arguments)
        return "handler-result"

    with sessions.stream(
        worker.id, input="Look up with a handler", tool_handlers={"lookup": lookup}
    ) as stream:
        seen = [event.type for event in stream]
    assert calls == [{}] and seen[-1] == "agent.session.idle", seen
    assert any(
        item.type == "function_call_output" and item.output == "handler-result"
        for item in sessions.items.list(worker.id)
    )
    unknown = {
        "type": "agent.session.input.tool_result",
        "turn_id": "missing-turn",
        "call_id": "missing-call",
        "success": True,
        "output": "late",
    }
    try:
        sessions.events.create(worker.id, events=[unknown])
        raise AssertionError("unknown call accepted")
    except openai.BadRequestError as error:
        assert is_pending_call_race(error, "missing-call"), error.body

    print("SDK: keyed image message", file=sys.stderr, flush=True)
    # An identical keyed retry replays acceptance; the text and image message
    # starts only one turn and is saved with both content parts.
    turns = len(list(sessions.turns.list(worker.id)))
    keyed = [
        {
            "type": "agent.session.input.message",
            "input": [
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": "Describe this image"},
                        {"type": "input_image", "image_url": PIXEL},
                    ],
                }
            ],
        }
    ]
    with sessions.events.stream(worker.id) as stream:
        sessions.events.create(worker.id, events=keyed, idempotency_key="keyed-input")
        finish(stream)
    sessions.events.create(worker.id, events=keyed, idempotency_key="keyed-input")
    assert len(list(sessions.turns.list(worker.id))) == turns + 1
    rejected(
        openai.BadRequestError,
        sessions.events.create,
        worker.id,
        events=[message("different")],
        idempotency_key="keyed-input",
    )
    user = [
        item
        for item in sessions.items.list(worker.id, order="asc")
        if item.type == "message" and item.role == "user"
    ][-1]
    assert [part.model_dump() for part in user.content] == [
        {"type": "input_text", "text": "Describe this image"},
        {"type": "input_image", "image_url": PIXEL},
    ]

    print("SDK: invalid batches", file=sys.stderr, flush=True)
    # Invalid batches are rejected whole, before anything runs.
    cancel = {"type": "agent.session.input.cancel"}
    result = {
        "type": "agent.session.input.tool_result",
        "turn_id": "turn",
        "call_id": "call",
        "success": True,
        "output": "x",
    }

    def image(url):
        return {
            "type": "agent.session.input.message",
            "input": [
                {"role": "user", "content": [{"type": "input_image", "image_url": url}]}
            ],
        }

    for events in [
        [],
        [message("first"), message("second")],
        [message("first"), cancel],
        [cancel, cancel],
        [result] * 33,
        [result, result],
        [image("file:///etc/hosts")],
        [image("https://example.com/remote.png")],
    ]:
        rejected(
            openai.BadRequestError, sessions.events.create, worker.id, events=events
        )
    rejected(
        openai.BadRequestError,
        sessions.events.create,
        worker.id,
        events=[message("too long a key")],
        idempotency_key="k" * 256,
    )
    assert len(list(sessions.turns.list(worker.id))) == turns + 1
