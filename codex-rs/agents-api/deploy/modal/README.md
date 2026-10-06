# Agents API on Modal

Tools for hosting the Agents API in a [Modal](https://modal.com) Sandbox and
evaluating it with coding tasks that need Codex's harness. They are not part of
the service and are not run by its tests.

## Requirements

- The `modal` Python package, and a Modal token (`modal token new`).
- A Modal secret holding the worker's provider key, created by you so the
  scripts never read it:
  `modal secret create agents-api-openrouter OPENROUTER_API_KEY=…`
- `openai==3.17.0` for the eval runner.

## Hosting

`serve.py` builds the service, its Codex worker, and the `codex` CLI from the
repository's committed `HEAD` into a cached image layer (about 20–40 minutes
on 16 CPUs the first time; later starts take seconds). It then runs them in a
Sandbox with 4 CPUs and 8 GB:

    python serve.py start STATE_DIR     # prints the https endpoint
    python serve.py logs STATE_DIR
    python serve.py stop STATE_DIR

- **Credentials:** a fresh API token, vault passphrase, and environment key
  are written to `STATE_DIR`; keep it private.
- **Networking:** the service binds loopback only, as it requires, and a TCP
  forwarder exposes it on the tunnel.
- **Lifetime:** the Sandbox stops itself after `--hours` (default 3) and
  bills while it runs.

Known gap: Modal's tunnel does not send `x-forwarded-proto`, so a self-hosted
session's `remote_url` reads `http://…/registry`. Executors inside the Sandbox
use loopback and are unaffected. One outside it needs the https form of that
URL and `--trusted-registry-host`.

## Harness evals

`harness_evals.py` runs eight coding tasks:
- write and run a script;
- fix a failing test without touching it;
- compute a CSV statistic;
- rename a function across files;
- search a tree;
- branch and commit;
- filter JSON;
- fix a crash.

Each task gets its own workspace, self-hosted session, and executor. A
checker decides the result, not the model's own report:

    python harness_evals.py STATE_DIR --json modal.json             # Modal
    python harness_evals.py ~/local-deployment --local --json local.json

Local mode needs a running service, its `token` and `environment-key` in the
given directory, and a `codex` binary (default: this checkout's debug build).
Results report pass/fail, turn status, commands run, tokens, and wall time per
task, with a token budget (`--budget`, default 400,000).

First comparison (2026-10-07, `openai/gpt-5-mini`, one run each):
- **Modal:** 8/8, 116 s, 180,697 tokens.
- **Local macOS:** 8/8, 162 s, 237,472 tokens.

Most of the gap came from the model running extra commands locally. A single
run cannot separate network latency, shell differences (`bash` on Linux,
`zsh` on macOS), and model variance.
