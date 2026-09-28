"""Streamed event coverage, called from the pinned SDK lifecycle test."""

from openai._models import validate_type
from openai.types.beta.agent_session_event import AgentSessionEvent

MESSAGE_EVENTS = [
    "agent.session.turn.item.added",
    "agent.session.turn.content_part.added",
    "agent.session.turn.output_text.delta",
    "agent.session.turn.output_text.delta",
    "agent.session.turn.output_text.done",
    "agent.session.turn.content_part.done",
    "agent.session.turn.item.done",
]
REASONING_EVENTS = [
    "agent.session.turn.item.added",
    "agent.session.turn.reasoning_summary_part.added",
    "agent.session.turn.reasoning_summary_text.delta",
    "agent.session.turn.reasoning_summary_text.delta",
    "agent.session.turn.reasoning_summary_text.done",
    "agent.session.turn.reasoning_summary_part.done",
    "agent.session.turn.item.done",
]


def check_event_stream(client):
    """Stream one turn whose provider response streams reasoning and text."""
    sessions = client.beta.agents.sessions
    events = []
    with sessions.create(
        agent={"model": "mock-model", "reasoning": {"summary": "concise"}},
        environment={"type": "none"},
        input="Stream reasoning and text",
        stream=True,
    ) as stream:
        for event in stream:
            # Every payload must satisfy the pinned SDK's event union.
            validate_type(type_=AgentSessionEvent, value=event.to_dict())
            events.append(event)
            if event.type == "agent.session.idle":
                break
    session_id = events[0].session.id
    assert len({event.event_id for event in events}) == len(events)
    assert [event.type for event in events[-2:]] == [
        "agent.session.turn.completed",
        "agent.session.idle",
    ]

    by_item = {}
    for event in events:
        item = getattr(event, "item", None)
        item_id = getattr(event, "item_id", None) or (item.id if item else None)
        if item_id:
            by_item.setdefault(item_id, []).append(event)
    saved = list(sessions.items.list(session_id, order="asc"))
    message = next(
        item for item in saved if item.type == "message" and item.role == "assistant"
    )
    reasoning = next(item for item in saved if item.type == "reasoning")

    # Each streamed item opens, streams, and closes in order at one position,
    # and its deltas rebuild exactly the text that was saved.
    assert [event.type for event in by_item[message.id]] == MESSAGE_EVENTS
    assert [event.type for event in by_item[reasoning.id]] == REASONING_EVENTS
    text = "".join(
        event.delta for event in by_item[message.id] if event.type.endswith(".delta")
    )
    summary = "".join(
        event.delta for event in by_item[reasoning.id] if event.type.endswith(".delta")
    )
    assert text == message.content[0].text == "Hello"
    assert summary == reasoning.summary[0].text == "Thinking"
    # The event and list models are distinct classes; compare their payloads.
    assert by_item[message.id][-1].item.to_dict() == message.to_dict()
    assert by_item[reasoning.id][-1].item.to_dict() == reasoning.to_dict()
    message_index = {event.output_index for event in by_item[message.id]}
    reasoning_index = {event.output_index for event in by_item[reasoning.id]}
    assert reasoning_index == {0} and message_index == {1}, (
        reasoning_index,
        message_index,
    )

    # The provider reported usage once; the turn and session carry it exactly.
    expected = {
        "input_tokens": 10,
        "input_tokens_details": {"cached_tokens": 4},
        "output_tokens": 6,
        "output_tokens_details": {"reasoning_tokens": 2},
        "total_tokens": 16,
    }
    completed = next(
        event for event in events if event.type == "agent.session.turn.completed"
    )
    assert completed.turn.usage.to_dict() == expected
    assert events[-1].session.usage.to_dict() == expected
    assert sessions.retrieve(session_id).usage.to_dict() == expected
