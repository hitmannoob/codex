"""Saved-agent contract coverage, called from the pinned SDK lifecycle test."""

import openai


def check_saved_agents(client):
    agents = client.beta.agents
    agent = agents.create(
        model="mock-model",
        instructions="",
        name="named",
        metadata={"界" * 64: "é" * 512},
        reasoning={"effort": "high", "summary": "detailed"},
        text={
            "verbosity": "high",
            "format": {"type": "json_schema", "schema": {"type": "object"}},
        },
        service_tier="fast",
        multi_agent={"enabled": True},
    )
    assert agents.retrieve(agent.id) == agent
    assert agent.multi_agent.max_concurrent_subagents == 6
    updated = agents.update(agent.id, model="different-model")
    expected = agent.model_dump()
    expected.update(model="different-model", updated_at=updated.updated_at)
    assert updated.model_dump() == expected
    replaced = agents.update(
        agent.id, reasoning={"effort": None}, text={"verbosity": "low"}
    )
    assert replaced.reasoning.model_dump() == {"effort": None, "summary": None}
    assert replaced.text.model_dump() == {
        "verbosity": "low",
        "format": {"type": "text"},
    }
    reset = agents.update(
        agent.id,
        instructions=None,
        name=None,
        metadata={},
        reasoning=None,
        text=None,
        service_tier=None,
        multi_agent=None,
    )
    assert reset.instructions is None and reset.name is None and reset.metadata == {}
    assert reset.service_tier == "auto"
    assert reset.text.model_dump() == {
        "verbosity": "medium",
        "format": {"type": "text"},
    }
    assert reset.multi_agent.model_dump() == {
        "enabled": False,
        "max_concurrent_subagents": None,
    }
    assert agents.update(agent.id, instructions="").instructions == ""
    tools = [
        {
            "type": "function",
            "name": "lookup",
            "description": "Lookup",
            "parameters": {"type": "object"},
            "defer_loading": True,
        },
        {"type": "tool_search"},
        {"type": "programmatic_tool_calling"},
        {
            "type": "mcp",
            "server_label": "http",
            "transport": {"type": "http", "server_url": "https://example.invalid/mcp"},
        },
        {
            "type": "mcp",
            "server_label": "stdio",
            "transport": {"type": "stdio", "command": "example", "cwd": "/example"},
        },
        {"type": "web_search"},
    ]
    configured = agents.update(agent.id, model="mock-model", tools=tools)
    assert [tool.type for tool in configured.tools] == [tool["type"] for tool in tools]
    assert configured.tools[2].enabled is True
    assert configured.tools[3].connection_origin == "service"
    assert (
        configured.tools[3].request_metadata == {}
        and configured.tools[3].transport.headers == {}
    )
    assert (
        configured.tools[4].transport.args == []
        and configured.tools[4].transport.env_vars == []
    )
    assert (
        configured.tools[5].mode == "live"
        and configured.tools[5].context_size == "medium"
    )
    assert agents.retrieve(agent.id) == configured
    for tool in tools:
        agents.update(agent.id, tools=[tool])
        try:
            client.beta.agents.sessions.create(
                agent_id=agent.id,
                environment={"type": "none"},
                input="unsupported execution",
            )
        except openai.BadRequestError:
            pass
        else:
            raise AssertionError(f"unsupported execution was accepted: {tool['type']}")
    assert agents.update(agent.id, tools=None).tools == []
    before = agents.retrieve(agent.id)
    for patch in [
        {"model": None},
        {"reasoning": {"summary": "wrong"}},
        {"reasoning": {"unknown": 1}},
        {"text": {"format": {"type": "json_schema", "schema": []}}},
        {"service_tier": "wrong"},
        {"multi_agent": {"enabled": True, "max_concurrent_subagents": 0}},
        {"metadata": {str(i): "value" for i in range(17)}},
        {"metadata": {"key": 7}},
        {"tools": [{"type": "web_search", "unknown": True}]},
    ]:
        try:
            agents.update(agent.id, extra_body=patch)
        except openai.BadRequestError:
            pass
        else:
            raise AssertionError(f"invalid update accepted: {patch}")
        assert agents.retrieve(agent.id) == before
    for query in [
        {"limit": 0},
        {"limit": 101},
        {"order": "wrong"},
        {"after": "missing"},
    ]:
        try:
            agents.list(extra_query=query)
        except openai.BadRequestError:
            pass
        else:
            raise AssertionError(f"invalid pagination accepted: {query}")
    assert agents.delete(agent.id).model_dump() == {
        "id": agent.id,
        "object": "agent.deleted",
        "deleted": True,
    }
    for operation in [agents.retrieve, agents.delete, agents.update]:
        try:
            operation(agent.id)
        except openai.NotFoundError:
            pass
        else:
            raise AssertionError("deleted agent remained accessible")
    page = agents.list()
    assert page.data == [] and not page.has_next_page()
