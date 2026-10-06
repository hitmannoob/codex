"""Controlled real-provider suite for a running Agents API.

Each scenario runs against a live service whose worker calls a real model
provider, and is classified as:
  PASS   the service met its contract and the model did what was asked
  FAIL   the service broke its contract (a service bug)
  MODEL  the service met its contract, but the model did not follow the
         prompt, so the scenario cannot judge the service
  SKIP   a prerequisite is missing, or the token budget is spent

Provider credentials belong to the service's worker and never reach this
script. Configuration comes from the environment:
  AGENTS_API_BASE_URL         default http://127.0.0.1:4501/v1
  AGENTS_API_TOKEN(_FILE)     the service's bearer token
  AGENTS_API_MODEL            default openai/gpt-5-mini
  AGENTS_API_BUDGET_TOKENS    tokens the run may spend, default 150000; no
                              scenario starts once the sessions' recorded
                              usage reaches it
  AGENTS_API_EXECUTOR         a `codex` binary, for the self-hosted scenarios
  AGENTS_API_ENVIRONMENT_KEY(_FILE)  the service's environment key

Usage: python real_provider.py [--json report.json] [--only name,name]
Requires the pinned SDK (openai==3.17.0). Exits non-zero only on FAIL.
"""

import argparse
import base64
import json
import os
import subprocess
import sys
import tempfile
import time

import openai

TURN_TIMEOUT = 240
ORDER_TOOL = {
    "type": "function",
    "name": "get_order_status",
    "description": "Look up the shipping status of an order by its ID.",
    "parameters": {
        "type": "object",
        "properties": {"order_id": {"type": "string"}},
        "required": ["order_id"],
        "additionalProperties": False,
    },
}


class Fail(Exception):
    """The service broke its contract."""


class Model(Exception):
    """The model did not follow the prompt."""


class Skip(Exception):
    """A prerequisite is missing."""


def expect(condition, message):
    if not condition:
        raise Fail(message)


def setting(name, default=None):
    value = os.environ.get(name)
    if value is None and os.environ.get(f"{name}_FILE"):
        with open(
            os.path.expanduser(os.environ[f"{name}_FILE"]), encoding="utf-8"
        ) as handle:
            value = handle.read().strip()
    return value if value is not None else default


class Run:
    def __init__(self, client, model):
        self.client = client
        self.sessions = client.beta.agents.sessions
        self.model = model
        self.created = []

    def create(self, **params):
        params.setdefault("environment", {"type": "none"})
        agent = params.pop("agent", {})
        session = self.sessions.create(agent={"model": self.model, **agent}, **params)
        self.created.append(session.id)
        return session

    def settle(self, session_id, turns=1):
        """Wait until the session has `turns` finished turns and is not running."""
        deadline = time.monotonic() + TURN_TIMEOUT
        while True:
            session = self.sessions.retrieve(session_id)
            finished = [
                turn
                for turn in self.sessions.turns.list(session_id)
                if turn.status in ("completed", "failed", "cancelled")
            ]
            if len(finished) >= turns and session.status != "in_progress":
                return session, finished
            if time.monotonic() > deadline:
                raise Fail(f"session {session_id} did not settle: {session.status}")
            time.sleep(1)

    def assistant_text(self, session_id):
        texts = [
            part.text
            for item in self.sessions.items.list(session_id, order="asc")
            if item.type == "message" and item.role == "assistant"
            for part in item.content
        ]
        return texts[-1] if texts else ""

    def spent(self):
        total = 0
        for session_id in self.created:
            try:
                usage = self.sessions.retrieve(session_id).usage
            except openai.NotFoundError:
                continue
            if usage:
                total += usage.input_tokens + usage.output_tokens
        return total

    def cleanup(self):
        for session_id in self.created:
            try:
                self.sessions.delete(session_id)
            except openai.APIStatusError:
                pass


def check_plain(run):
    session = run.create(input="Reply with exactly the word: pong")
    session, turns = run.settle(session.id)
    expect(turns[0].status == "completed", f"turn {turns[0].status}: {turns[0].error}")
    expect(session.status == "idle", session.status)
    usage = session.usage
    expect(
        usage and usage.input_tokens > 0 and usage.output_tokens > 0, f"usage {usage}"
    )
    expect(
        turns[0].usage and turns[0].usage.input_tokens > 0,
        f"turn usage {turns[0].usage}",
    )
    if "pong" not in run.assistant_text(session.id).lower():
        raise Model(f"answered {run.assistant_text(session.id)!r}")


def check_stream(run):
    deltas, events = [], []
    with run.sessions.create(
        agent={"model": run.model},
        environment={"type": "none"},
        input="Count from one to five in words, separated by spaces.",
        stream=True,
    ) as stream:
        for event in stream:
            events.append(event.type)
            if event.type == "agent.session.turn.output_text.delta":
                deltas.append(event.delta)
            if event.type == "agent.session.created":
                run.created.append(event.session.id)
            if event.type == "agent.session.idle":
                break
    session_id = run.created[-1]
    expect(
        events[-2:] == ["agent.session.turn.completed", "agent.session.idle"],
        events[-3:],
    )
    expect(
        "".join(deltas) == run.assistant_text(session_id),
        "streamed deltas differ from the saved text",
    )
    if "five" not in "".join(deltas).lower():
        raise Model(f"answered {''.join(deltas)!r}")


def check_function(run):
    session = run.create(
        agent={
            "tools": [ORDER_TOOL],
            "instructions": "Always use get_order_status for orders.",
        },
        input="What is the status of order A123?",
    )
    deadline = time.monotonic() + TURN_TIMEOUT
    while (current := run.sessions.retrieve(session.id)).status != "requires_action":
        if current.status == "idle":
            raise Model("answered without calling the tool")
        expect(current.status == "in_progress", current.status)
        expect(time.monotonic() < deadline, "no required action")
        time.sleep(1)
    action = current.required_actions[0]
    expect(action.type == "function_call" and action.name == "get_order_status", action)
    run.sessions.events.create(
        session.id,
        events=[
            {
                "type": "agent.session.input.tool_result",
                "turn_id": action.turn_id,
                "call_id": action.call_id,
                "success": True,
                "output": "shipped via ZX-PARCEL-731",
            }
        ],
    )
    run.settle(session.id)
    if "731" not in run.assistant_text(session.id):
        raise Model(f"answer ignored the result: {run.assistant_text(session.id)!r}")


def check_structured(run):
    schema = {
        "type": "object",
        "properties": {"answer": {"type": "integer"}},
        "required": ["answer"],
        "additionalProperties": False,
    }
    session = run.create(
        agent={
            "text": {
                "format": {
                    "type": "json_schema",
                    "schema": schema,
                }
            }
        },
        input="What is 6 times 7?",
    )
    run.settle(session.id)
    text = run.assistant_text(session.id)
    try:
        answer = json.loads(text)
    except json.JSONDecodeError:
        raise Model(f"not JSON: {text!r}")
    if answer != {"answer": 42}:
        raise Model(f"answered {answer}")


def check_cancel(run):
    session = run.create(input="Write a 3000-word essay on the history of mathematics.")
    deadline = time.monotonic() + 60
    while run.sessions.retrieve(session.id).status != "in_progress":
        expect(time.monotonic() < deadline, "turn never started")
        time.sleep(0.5)
    run.sessions.events.create(
        session.id, events=[{"type": "agent.session.input.cancel"}]
    )
    session, turns = run.settle(session.id)
    if turns[0].status == "completed":
        raise Model("the turn finished before the cancel arrived")
    expect(turns[0].status == "cancelled", turns[0].status)
    expect(session.status == "idle", session.status)


def check_traces(run):
    session = run.create(input="Say hello.")
    run.settle(session.id)
    response = run.client.get(
        f"/agents/sessions/{session.id}/traces",
        cast_to=object,
        options={"headers": {"OpenAI-Beta": "agents=v1"}},
    )
    traces = response["data"]
    expect(len(traces) == 1, f"{len(traces)} traces")
    resources = traces[0]["otlp"]["resourceSpans"]
    spans = [
        span
        for resource in resources
        for group in resource["scopeSpans"]
        for span in group["spans"]
    ]
    names = {span["name"].split(" ")[0] for span in spans}
    expect({"invoke_agent", "chat"} <= names, names)


def self_hosted(run, workspace):
    executor, key = (
        setting("AGENTS_API_EXECUTOR"),
        setting("AGENTS_API_ENVIRONMENT_KEY"),
    )
    if not executor or not key:
        return None
    session = run.create(
        environment={"type": "self_hosted", "workspace_directory": workspace}
    )
    environment = session.environment
    process = subprocess.Popen(
        [
            executor,
            "exec-server",
            "--remote",
            environment.remote_url,
            "--environment-id",
            environment.id,
        ],
        env={**os.environ, "CODEX_API_KEY": key},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.monotonic() + 60
    while (
        run.client.beta.agents.environments.retrieve(environment.id).status
        != "connected"
    ):
        expect(process.poll() is None, "the executor exited")
        expect(time.monotonic() < deadline, "the executor never connected")
        time.sleep(0.5)
    return session, process


def check_self_hosted(run):
    workspace = os.path.realpath(tempfile.mkdtemp(prefix="agents-api-real-"))
    started = self_hosted(run, workspace)
    if started is None:
        raise Skip("AGENTS_API_EXECUTOR and AGENTS_API_ENVIRONMENT_KEY are not set")
    session, process = started
    try:
        files = run.client.beta.agents.environments.files
        data = b"a,b\n1,10\n2,32\n3,0\n4,58\n"
        uploaded = run.client.files.create(file=("data.csv", data), purpose="user_data")
        copied = files.create(
            session.environment.id,
            type="file_id",
            path=os.path.join(workspace, "data.csv"),
            file_id=uploaded.id,
        )
        expect(copied.size_bytes == len(data), copied)
        run.client.files.delete(uploaded.id)
        run.sessions.events.create(
            session.id,
            events=[
                {
                    "type": "agent.session.input.message",
                    "input": [
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "input_text",
                                    "text": "Using shell commands, sum column b of data.csv, write only the number to result.txt, and reply with it.",
                                }
                            ],
                        }
                    ],
                }
            ],
        )
        run.settle(session.id)
        commands = [
            item
            for item in run.sessions.items.list(session.id)
            if item.type == "command_execution"
        ]
        if not commands:
            raise Model("ran no command")
        expect(
            all(item.status in ("completed", "failed") for item in commands), commands
        )
        listed = [item.path for item in files.list(session.environment.id, order="asc")]
        expect(os.path.join(workspace, "data.csv") in listed, listed)
        result = os.path.join(workspace, "result.txt")
        if (
            not os.path.exists(result)
            or open(result, encoding="utf-8").read().strip() != "100"
        ):
            raise Model("result.txt does not hold 100")
    finally:
        process.terminate()
        process.wait()
    deadline = time.monotonic() + 10
    while (
        run.client.beta.agents.environments.retrieve(session.environment.id).status
        != "disconnected"
    ):
        expect(time.monotonic() < deadline, "executor loss was not reported")
        time.sleep(0.5)


CHECKS = {
    "plain": check_plain,
    "stream": check_stream,
    "function": check_function,
    "structured": check_structured,
    "cancel": check_cancel,
    "traces": check_traces,
    "self_hosted": check_self_hosted,
}


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--json", help="write the report to this file")
    parser.add_argument("--only", help="comma-separated scenario names")
    args = parser.parse_args()
    token = setting("AGENTS_API_TOKEN")
    if not token:
        sys.exit("set AGENTS_API_TOKEN or AGENTS_API_TOKEN_FILE")
    client = openai.OpenAI(
        base_url=setting("AGENTS_API_BASE_URL", "http://127.0.0.1:4501/v1"),
        api_key=token,
        max_retries=0,
        timeout=60,
        _strict_response_validation=True,
    )
    run = Run(client, setting("AGENTS_API_MODEL", "openai/gpt-5-mini"))
    budget = int(setting("AGENTS_API_BUDGET_TOKENS", "150000"))
    names = args.only.split(",") if args.only else list(CHECKS)
    results = []
    try:
        for name in names:
            started = time.monotonic()
            spent = run.spent()
            if spent >= budget:
                outcome, detail = "SKIP", f"budget spent ({spent} of {budget} tokens)"
            else:
                try:
                    CHECKS[name](run)
                    outcome, detail = "PASS", ""
                except Fail as error:
                    outcome, detail = "FAIL", str(error)
                except Model as error:
                    outcome, detail = "MODEL", str(error)
                except Skip as error:
                    outcome, detail = "SKIP", str(error)
                except openai.APIStatusError as error:
                    outcome, detail = "FAIL", f"{error.status_code}: {error.message}"
            results.append(
                {
                    "scenario": name,
                    "outcome": outcome,
                    "detail": detail,
                    "seconds": round(time.monotonic() - started, 1),
                }
            )
            print(
                f"{outcome:5} {name:12} {results[-1]['seconds']:6}s {detail}",
                flush=True,
            )
    finally:
        tokens = run.spent()
        run.cleanup()
    report = {
        "model": run.model,
        "tokens": tokens,
        "budget": budget,
        "results": results,
    }
    print(json.dumps({"model": run.model, "tokens": tokens, "budget": budget}))
    if args.json:
        with open(args.json, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2)
    sys.exit(1 if any(result["outcome"] == "FAIL" for result in results) else 0)


if __name__ == "__main__":
    main()
