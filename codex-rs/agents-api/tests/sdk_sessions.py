"""Session list/update/delete coverage, called from the pinned SDK lifecycle test."""

import time

import openai

from sdk_helpers import message


def rejected(error, operation, *args, **kwargs):
    try:
        operation(*args, **kwargs)
    except error:
        return
    raise AssertionError(f"accepted {args} {kwargs}")


def idle(sessions, session_id):
    deadline = time.monotonic() + 10
    while (session := sessions.retrieve(session_id)).status != "idle":
        assert time.monotonic() < deadline
        time.sleep(0.02)
    return session


def check_session_management(client, agent_id, created):
    """`created` lists the IDs of sessions made from `agent_id`, oldest first."""
    sessions = client.beta.agents.sessions
    other = sessions.create(
        agent={"model": "mock-model", "reasoning": {"summary": "concise"}},
        environment={"type": "none"},
        input="Another agent",
        metadata={"owner": "other"},
    )
    other = idle(sessions, other.id)

    assert [session.id for session in sessions.list()] == [other.id, *reversed(created)]
    assert sessions.list(limit=1).data == [other]
    page = sessions.list(agent_id=agent_id, limit=1, order="asc")
    assert [session.id for session in page.data] == created[:1] and page.has_next_page()
    assert [session.id for session in page.get_next_page().data] == created[1:2]
    for query in [
        {"limit": 0},
        {"limit": 101},
        {"order": "wrong"},
        {"after": "missing"},
        {"agent_id": agent_id, "after": other.id},
    ]:
        rejected(openai.BadRequestError, sessions.list, extra_query=query)

    updated = sessions.update(
        other.id,
        metadata={"owner": "updated"},
        agent={
            "model": "updated-model",
            "reasoning": {"effort": "low"},
            "service_tier": "flex",
        },
    )
    expected = other.model_dump()
    expected["metadata"] = {"owner": "updated"}
    expected["agent"].update(model="updated-model", service_tier="flex")
    expected["agent"]["reasoning"] = {"effort": "low", "summary": "concise"}
    assert updated.model_dump() == expected
    assert sessions.retrieve(other.id) == updated
    assert sessions.update(other.id, agent={"reasoning": {}}) == updated
    reset = sessions.update(
        other.id,
        metadata=None,
        agent={"reasoning": {"effort": None}, "service_tier": None},
    )
    expected["metadata"] = {}
    expected["agent"].update(service_tier="auto")
    expected["agent"]["reasoning"] = {"effort": None, "summary": "concise"}
    assert reset.model_dump() == expected
    for patch in [
        {"agent": {"instructions": "fixed at creation"}},
        {"agent": {"reasoning": {"summary": "auto"}}},
        {"agent": {"reasoning": None}},
        {"agent": {"model": None}},
        {"agent": {"service_tier": "wrong"}},
        {"metadata": {"key": 7}},
        {"metadata": {str(i): "value" for i in range(17)}},
        {"status": "idle"},
    ]:
        rejected(openai.BadRequestError, sessions.update, other.id, extra_body=patch)
        assert sessions.retrieve(other.id) == reset

    assert sessions.delete(other.id).model_dump() == {
        "id": other.id,
        "object": "agent.session.deleted",
        "deleted": True,
    }
    for operation in [sessions.retrieve, sessions.delete, sessions.items.list]:
        rejected(openai.NotFoundError, operation, other.id)
    rejected(openai.NotFoundError, sessions.update, other.id, metadata={})
    rejected(
        openai.NotFoundError, sessions.events.create, other.id, events=[message("gone")]
    )
    rejected(openai.BadRequestError, sessions.list, after=other.id)
    assert [session.id for session in sessions.list()] == list(reversed(created))
