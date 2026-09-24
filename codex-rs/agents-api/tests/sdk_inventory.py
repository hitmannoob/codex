"""Validate the checked-in operation inventory against openai==3.17.0."""

import importlib
import inspect
import json
import sys

from openai.types.beta.agent import Agent
from openai.types.beta.agent_session import AgentSession

from sdk_helpers import SDK_VERSION, fixture, require_pinned_sdk


HTTP_CALLS = {
    "GET": ("self._get(", "self._get_api_list("),
    "POST": ("self._post(",),
    "DELETE": ("self._delete(",),
}


def resource(value):
    module_name, class_name = value.split(":", 1)
    return getattr(importlib.import_module(module_name), class_name)


def normalize(value):
    return "".join(value.split())


def main():
    require_pinned_sdk()
    with open(sys.argv[1], encoding="utf-8") as handle:
        inventory = json.load(handle)

    assert inventory["baseline"]["sdk"] == f"openai=={SDK_VERSION}"
    operations = inventory["operations"]
    assert len(operations) == inventory["summary"]["total_operations"]

    expected_by_resource = {}
    for operation in operations:
        sdk_resource = operation["sdk_resource"]
        if sdk_resource is None:
            continue
        method = getattr(resource(sdk_resource), operation["sdk_method"])
        source = inspect.getsource(method)
        assert normalize(operation["path"]) in normalize(source), operation["id"]
        assert any(token in source for token in HTTP_CALLS[operation["method"]]), (
            operation["id"]
        )
        if operation["scope"] == "agents_api":
            assert '"OpenAI-Beta": "agents=v1"' in source, operation["id"]
        expected_by_resource.setdefault(sdk_resource, set()).add(
            operation["sdk_method"]
        )

    for resource_name, expected in expected_by_resource.items():
        actual = {
            name
            for name, method in resource(resource_name).__dict__.items()
            if inspect.isfunction(method)
            and not getattr(method, "__deprecated__", None)
            and any(
                token in inspect.getsource(method)
                for tokens in HTTP_CALLS.values()
                for token in tokens
            )
        }
        assert actual == expected, (resource_name, sorted(expected), sorted(actual))

    Agent.model_validate(fixture("agent_response"))
    AgentSession.model_validate(fixture("session_response"))
    print(
        json.dumps(
            {
                "sdk": SDK_VERSION,
                "sdk_operations": sum(
                    len(value) for value in expected_by_resource.values()
                ),
            }
        )
    )


if __name__ == "__main__":
    main()
