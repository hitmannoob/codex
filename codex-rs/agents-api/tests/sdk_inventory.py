"""Validate the checked-in operation inventory against openai==3.17.0."""

import importlib
import inspect
import json
import re
import sys
from pathlib import Path

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


TEST_ATTRIBUTE = re.compile(
    r"#\[(?:tokio::)?test\b[^\]]*\]\s*(?:#\[[^\]]*\]\s*)*(?:async\s+)?fn\s+(\w+)"
)


def check_tests(inventory, crate):
    """Every implemented row names tests, and each `module::name` is a test
    function in that module's file."""
    modules = {}
    for path in [
        *crate.glob("src/*.rs"),
        *crate.glob("tests/*.rs"),
        *crate.glob("tests/suite/*.rs"),
    ]:
        modules.setdefault(path.stem, set()).update(
            TEST_ATTRIBUTE.findall(path.read_text(encoding="utf-8"))
        )
    rows = inventory["operations"] + inventory["behaviors"]
    for row in rows:
        if row["status"] == "missing":
            continue
        tests = [test for test in re.split(r";\s*", row.get("tests") or "") if test]
        assert tests, f"{row['id']} names no tests"
        for test in tests:
            module, _, name = test.rpartition("::")
            module = module.split("::")[0]
            assert name in modules.get(module, set()) or name == "tests", (
                f"{row['id']}: {test}"
            )
    return sum(row["status"] != "missing" for row in rows)


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

    covered = check_tests(inventory, Path(sys.argv[1]).resolve().parent)

    Agent.model_validate(fixture("agent_response"))
    AgentSession.model_validate(fixture("session_response"))
    print(
        json.dumps(
            {
                "sdk": SDK_VERSION,
                "sdk_operations": sum(
                    len(value) for value in expected_by_resource.values()
                ),
                "rows_with_tests": covered,
            }
        )
    )


if __name__ == "__main__":
    main()
