"""Harness evals: coding tasks that need Codex's shell and file tools.

Each task gets its own workspace, a self-hosted session, and an executor
(`codex exec-server`) that reaches the service's registry over loopback. A
checker runs in the workspace after the turn, so results never depend on what
the model says it did.

Against the Modal endpoint from serve.py, everything runs inside the Sandbox:
  harness_evals.py STATE_DIR
Against a local deployment, with executors and workspaces on this machine
(STATE_DIR holds its `token` and `environment-key`):
  harness_evals.py STATE_DIR --local [--url http://127.0.0.1:4501]
Options: --only a,b  --model M  --budget TOKENS  --json report.json
"""

import argparse
import base64
import json
import os
import shlex
import subprocess
import tempfile
import time

import modal
import openai

TURN_TIMEOUT = 360

# name: (files, prompt, checker shell command; exit 0 means pass)
TASKS = {
    "hello_script": (
        {},
        "Create hello.py that prints exactly: Hello, Agents API! Then run it to confirm.",
        'test "$(python3 hello.py)" = "Hello, Agents API!"',
    ),
    "fix_bug": (
        {
            "calc.py": "def add(a, b):\n    return a + b\n\n\ndef subtract(a, b):\n    return a + b\n\n\ndef multiply(a, b):\n    return a * b\n",
            "test_calc.py": "from calc import add, subtract, multiply\n\nassert add(2, 3) == 5\nassert subtract(7, 4) == 3\nassert multiply(3, 4) == 12\nprint('all tests passed')\n",
        },
        "Run test_calc.py. It fails. Fix the bug in calc.py so the tests pass. Do not modify test_calc.py.",
        'python3 test_calc.py && python3 -c "import hashlib,sys; '
        "sys.exit(hashlib.sha256(open('test_calc.py','rb').read()).hexdigest() != '{test_digest}')\"",
    ),
    "csv_stats": (
        {"scores.csv": "name,score\nada,91\nbob,78\ncy,85\ndee,66\neve,100\n"},
        "Using a shell command, compute the average of the score column in scores.csv and write it to result.txt with exactly two decimal places and nothing else.",
        'test "$(tr -d "[:space:]" < result.txt)" = "84.00"',
    ),
    "rename": (
        {
            "users.py": "def get_user_name(user):\n    return user['name'].title()\n",
            "greet.py": "from users import get_user_name\n\n\ndef greet(user):\n    return 'Hi ' + get_user_name(user)\n",
            "main.py": "from greet import greet\nfrom users import get_user_name\n\nuser = {'name': 'grace'}\nprint(greet(user), get_user_name(user))\n",
        },
        "Rename the function get_user_name to fetch_user_name everywhere in this project, then run main.py to confirm it still works.",
        '! grep -rq get_user_name --include=*.py . && test "$(python3 main.py)" = "Hi Grace Grace"',
    ),
    "search": (
        {
            **{
                f"pkg/module_{index}.py": f"VALUE_{index} = {index * 7}\n"
                for index in range(25)
            },
            "pkg/deep/config_17.py": "MAGIC_NUMBER = 4815162342\n",
        },
        "Find which file defines the constant MAGIC_NUMBER and write just its value to answer.txt.",
        'test "$(tr -d "[:space:]" < answer.txt)" = "4815162342"',
    ),
    "git_branch": (
        {"README.md": "# Demo\n"},
        "This is a git repository. Create a branch named feature, add a file NOTES.md containing the text done, and commit it with the message: add notes",
        'test "$(git rev-parse --abbrev-ref HEAD)" = "feature" && test "$(git log -1 --format=%s)" = "add notes" '
        "&& git show HEAD:NOTES.md | grep -qx done",
    ),
    "json_filter": (
        {
            "users.json": json.dumps(
                [
                    {"name": "zoe", "active": True},
                    {"name": "adam", "active": False},
                    {"name": "mia", "active": True},
                    {"name": "bo", "active": True},
                ]
            )
        },
        "users.json holds a list of users. Write active_users.json with only the active users, sorted by name, keeping each user's fields.",
        "python3 -c \"import json; d=json.load(open('active_users.json')); "
        "assert d == [{'name':'bo','active':True},{'name':'mia','active':True},{'name':'zoe','active':True}], d\"",
    ),
    "crash_fix": (
        {
            "app.py": "orders = [{'qty': 2, 'price': '3.50'}, {'qty': 1, 'price': '10'}]\n"
            "total = sum(order['qty'] * order['price'] for order in orders)\n"
            "print(f'total={total:.2f}')\n",
        },
        "Run app.py. It crashes. Find the cause and fix app.py so it prints the correct total.",
        'test "$(python3 app.py)" = "total=17.00"',
    ),
}


class ModalBox:
    """The service, executors, and workspaces all inside the Modal Sandbox."""

    def __init__(self, sandbox):
        self.sandbox = sandbox
        self.root = "/workspace"

    def sh(self, command):
        process = self.sandbox.exec("bash", "-c", command)
        process.wait()
        return process.returncode, process.stdout.read(), process.stderr.read()

    def start_executor(self, environment_id, name):
        # The executor runs beside the service and reaches its registry over loopback.
        _, pid, _ = self.sh(
            f'cd / && nohup env CODEX_API_KEY="$CODEX_AGENTS_API_ENVIRONMENT_KEY" '
            f"/opt/agents-api/bin/codex exec-server --remote http://127.0.0.1:4501/registry "
            f"--environment-id {environment_id} > /data/logs/executor-{name}.log 2>&1 & echo $!"
        )
        return pid.strip()

    def stop_executor(self, handle):
        self.sh(f"kill {handle} 2>/dev/null || true")


class LocalBox:
    """The local deployment, with executors and workspaces on this machine."""

    def __init__(self, root, executor, environment_key, registry):
        self.root, self.executor, self.environment_key, self.registry = (
            root,
            executor,
            environment_key,
            registry,
        )

    def sh(self, command):
        process = subprocess.run(
            ["bash", "-c", command], capture_output=True, text=True
        )
        return process.returncode, process.stdout, process.stderr

    def start_executor(self, environment_id, name):
        log = open(os.path.join(self.root, f"executor-{name}.log"), "w")
        return subprocess.Popen(
            [
                self.executor,
                "exec-server",
                "--remote",
                self.registry,
                "--environment-id",
                environment_id,
            ],
            env={**os.environ, "CODEX_API_KEY": self.environment_key},
            stdout=log,
            stderr=subprocess.STDOUT,
        )

    def stop_executor(self, handle):
        handle.terminate()
        handle.wait()


def run_task(client, box, model, name):
    files, prompt, checker = TASKS[name]
    workspace = os.path.join(box.root, name)
    setup = [f"rm -rf {workspace} && mkdir -p {workspace} && cd {workspace}"]
    for path, content in files.items():
        encoded = base64.b64encode(content.encode()).decode()
        setup.append(
            f"mkdir -p $(dirname {shlex.quote(path)}) && echo {encoded} | base64 -d > {shlex.quote(path)}"
        )
    if name == "git_branch":
        setup.append(
            "git init -q -b main && git -c user.email=e@x -c user.name=eval add -A && "
            "git -c user.email=e@x -c user.name=eval commit -qm init && "
            "git config user.email eval@example.com && git config user.name eval"
        )
    code, _, error = box.sh(" && ".join(setup))
    assert code == 0, error
    if name == "fix_bug":
        _, digest, _ = box.sh(
            f"python3 -c \"import hashlib; print(hashlib.sha256(open('{workspace}/test_calc.py','rb').read()).hexdigest())\""
        )
        checker = checker.format(test_digest=digest.strip())
    sessions = client.beta.agents.sessions
    session = sessions.create(
        agent={"model": model},
        environment={"type": "self_hosted", "workspace_directory": workspace},
    )
    environment_id = session.environment.id
    executor = box.start_executor(environment_id, name)
    started = time.monotonic()
    try:
        deadline = time.monotonic() + 60
        while (
            client.beta.agents.environments.retrieve(environment_id).status
            != "connected"
        ):
            assert time.monotonic() < deadline, "executor never connected"
            time.sleep(0.5)
        sessions.events.create(
            session.id,
            events=[{"type": "agent.session.input.message", "input": prompt}],
        )
        deadline = time.monotonic() + TURN_TIMEOUT
        while True:
            current = sessions.retrieve(session.id)
            turns = list(sessions.turns.list(session.id))
            if (
                current.status in ("idle", "failed")
                and turns
                and all(
                    turn.status in ("completed", "failed", "cancelled")
                    for turn in turns
                )
            ):
                break
            if current.status == "requires_action":
                raise AssertionError(
                    f"unexpected required action {current.required_actions}"
                )
            if time.monotonic() > deadline:
                raise AssertionError(f"turn did not finish in {TURN_TIMEOUT}s")
            time.sleep(1)
        elapsed = time.monotonic() - started
        items = list(sessions.items.list(session.id, limit=100))
        commands = [item for item in items if item.type == "command_execution"]
        check, out, err = box.sh(f"cd {workspace} && {checker}")
        usage = current.usage
        return {
            "task": name,
            "result": "PASS" if check == 0 else "FAIL",
            "turns": [turn.status for turn in turns],
            "commands": len(commands),
            "failed_commands": sum(item.status == "failed" for item in commands),
            "tokens": (usage.input_tokens + usage.output_tokens) if usage else 0,
            "seconds": round(elapsed, 1),
            "detail": "" if check == 0 else (err or out).strip()[-200:],
        }
    finally:
        box.stop_executor(executor)
        try:
            sessions.delete(session.id)
        except openai.APIStatusError:
            pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("state")
    parser.add_argument("--only")
    parser.add_argument("--model", default="openai/gpt-5-mini")
    parser.add_argument("--budget", type=int, default=400_000)
    parser.add_argument("--json")
    parser.add_argument(
        "--local", action="store_true", help="use a local deployment instead of Modal"
    )
    parser.add_argument(
        "--url", default="http://127.0.0.1:4501", help="local service URL"
    )
    parser.add_argument(
        "--executor",
        default=os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..",
            "..",
            "..",
            "target",
            "debug",
            "codex",
        ),
        help="local codex binary (default: this checkout's debug build)",
    )
    parser.add_argument(
        "--workspaces", help="local workspace root (default: a temporary directory)"
    )
    args = parser.parse_args()
    read = lambda name: open(f"{args.state}/{name}", encoding="utf-8").read().strip()
    if args.local:
        url = args.url.rstrip("/")
        root = os.path.realpath(
            args.workspaces or tempfile.mkdtemp(prefix="agents-api-evals-")
        )
        os.makedirs(root, exist_ok=True)
        box = LocalBox(
            root,
            os.path.realpath(args.executor),
            read("environment-key"),
            f"{url}/registry",
        )
    else:
        url = read("url")
        box = ModalBox(modal.Sandbox.from_id(read("sandbox-id")))
    client = openai.OpenAI(
        base_url=f"{url}/v1",
        api_key=read("token"),
        max_retries=0,
        timeout=60,
        _strict_response_validation=True,
    )
    names = args.only.split(",") if args.only else list(TASKS)
    results, spent = [], 0
    for name in names:
        if spent >= args.budget:
            results.append({"task": name, "result": "SKIP", "detail": "budget spent"})
            continue
        try:
            result = run_task(client, box, args.model, name)
        except Exception as error:  # A harness or service problem, not a model miss.
            result = {"task": name, "result": "ERROR", "detail": str(error)[-300:]}
        spent += result.get("tokens", 0)
        results.append(result)
        print(json.dumps(result), flush=True)
    passed = sum(result["result"] == "PASS" for result in results)
    summary = {
        "model": args.model,
        "passed": passed,
        "total": len(results),
        "tokens": spent,
    }
    print(json.dumps(summary))
    if args.json:
        with open(args.json, "w", encoding="utf-8") as handle:
            json.dump({"summary": summary, "results": results}, handle, indent=2)


if __name__ == "__main__":
    main()
