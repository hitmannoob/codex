"""Subagent coverage, called from the pinned SDK lifecycle test."""

from openai._models import validate_type
from openai.types.beta.agent_session_event import AgentSessionEvent


def check_subagents(client):
    """Stream a turn whose root agent spawns one subagent, then read it back."""
    sessions = client.beta.agents.sessions
    events = []
    with sessions.create(
        agent={"model": "mock-model", "multi_agent": {"enabled": True}},
        environment={"type": "none"},
        input="sdk-delegate",
        stream=True,
    ) as stream:
        for event in stream:
            validate_type(type_=AgentSessionEvent, value=event.to_dict())
            events.append(event)
            idle = any(e.type == "agent.session.idle" for e in events)
            child_done = any(
                e.type == "agent.session.turn.completed" and e.turn.subagent_id
                for e in events
            )
            if idle and child_done:
                break
    session_id = events[0].session.id

    subagents = list(sessions.subagents.list(session_id))
    assert len(subagents) == 1, subagents
    subagent = subagents[0]
    assert subagent.status == "active" and subagent.name == "researcher"
    assert sessions.subagents.retrieve(subagent.id, session_id=session_id) == subagent
    created = [e for e in events if e.type == "agent.session.subagent.created"]
    assert [e.subagent for e in created] == [subagent]

    turns = list(sessions.subagents.turns.list(subagent.id, session_id=session_id))
    assert len(turns) == 1 and turns[0].subagent_id == subagent.id, turns
    assert (
        sessions.subagents.turns.retrieve(
            turns[0].id, session_id=session_id, subagent_id=subagent.id
        )
        == turns[0]
    )
    items = list(sessions.subagents.items.list(subagent.id, session_id=session_id))
    turn_items = list(
        sessions.subagents.turns.items.list(
            turns[0].id, session_id=session_id, subagent_id=subagent.id
        )
    )
    assert [item.id for item in turn_items] == [item.id for item in items]
    assert any(item.type == "message" and item.role == "assistant" for item in items)

    root = list(sessions.items.list(session_id))
    spawns = [item for item in root if item.type == "create_subagent_call"]
    assert [item.agent_id for item in spawns] == [subagent.id]
    assert [turn.subagent_id for turn in sessions.turns.list(session_id)] == [None]
