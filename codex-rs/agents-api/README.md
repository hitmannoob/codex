# Codex Agents API

A local HTTP facade over a dedicated Codex app-server from this checkout.
Agent definitions and session bindings live in `agents-api.sqlite`; Codex owns
the execution history. Normalized API turns and items also live in SQLite. Run one API process per data directory and retain the
app-server's `CODEX_HOME` alongside it. The text/function session path implements a subset of the OpenAI wire contract,
pinned to the official Python SDK 3.17.0. Full compatibility remains incomplete. The target is documented function-for-function parity; see
[the parity plan](PARITY.md) for the contract and gaps, and
[GOALS.md](GOALS.md) for detailed implementation steps and acceptance criteria.

Build both binaries, then start the API. It launches one app-server shared by its sessions:

```sh
cargo build -p codex-app-server -p codex-agents-api --bins
export CODEX_AGENTS_API_TOKEN="$(openssl rand -hex 32)"
target/debug/codex-agents-api --data-directory /absolute/path/agents-api-data
```

The worker executable defaults to the sibling `codex-app-server`; override it with
`--app-server-bin /absolute/path/codex-app-server`. Its persistent `CODEX_HOME`
defaults to `DATA_DIRECTORY/codex-home`; use `--codex-home` to select an existing
configured home. Configure provider credentials and MCP there, or through the
worker's inherited environment. The HTTP bearer token is not forwarded to the
worker. This dedicated home is not the operator's default `~/.codex`.

Managed startup locks the worker home, creates a private temporary socket, and
waits for the app-server initialization handshake before accepting HTTP requests.
`--worker-startup-timeout-secs` bounds that wait (default 30). Startup failures
clean up the child. On Ctrl-C or SIGTERM (Unix), the API stops accepting requests,
closes its backend connection, and requests worker shutdown, with a 10-second
worker grace period before forced termination and reaping. Shutdown can interrupt
active turns; it does not promise to finish them. Completed sessions can resume
using the retained API data directory and worker home.

Unexpected managed-worker exit stops the API with an error. This stage does not
restart workers or replay interrupted work. OS-forced API termination (for example,
SIGKILL), orphan adoption, and crash recovery are not yet managed. This worker is
the Codex harness; it does not provision a sandbox or launch a separate executor.

To use an externally managed worker, retain the explicit socket mode:

```sh
codex-app-server --listen unix:///absolute/path/app-server.sock
codex-agents-api --app-server-socket /absolute/path/app-server.sock \
  --data-directory /absolute/path/agents-api-data
```

The API neither stops nor deletes that external worker or socket. The socket flag
cannot be combined with managed executable/home flags.

The API listens on `127.0.0.1:4501`. Every request requires
`Authorization: Bearer $CODEX_AGENTS_API_TOKEN`.

## Official-style session path

The SDK sends `OpenAI-Beta: agents=v1`. This selects flattened, tagged-function
payloads for saved-agent creation/retrieval at `/v1/agents`. The original payloads
remain available without that header. New session routes are:

| Method | Path | Behavior |
| --- | --- | --- |
| POST | `/v1/agents/sessions` | Inline agent or saved `agent_id` plus overrides, required initial input, optional SSE |
| GET | `/v1/agents/sessions/{id}` | Configuration snapshot, status, metadata and current `required_actions` |
| POST | `/v1/agents/sessions/{id}/events` | Message/steering, cancel, or function result/error; empty HTTP 202 |
| GET | `/v1/agents/sessions/{id}/events` | Live normalized session, turn, item and completed-text events |
| GET | `/v1/agents/sessions/{id}/items` | Saved messages, reasoning and function records |
| GET | `/v1/agents/sessions/{id}/turns` | Saved outcomes |
| GET | `/v1/agents/sessions/{id}/turns/{turn_id}` | One saved outcome |

Lists accept `after`, `order=asc|desc`, and `limit=1..100`; item lists also accept
`turn_id`. Records persist before their events are emitted. Disconnecting an HTTP
stream does not cancel work. Streams do not replay; reconnect, read current state
and history, and merge buffered updates by item ID. A lagged stream closes.
Backend notification loss fails the connection rather than serving incomplete
history as healthy. Backend loss marks active public turns failed, without replay.

This stage accepts only `environment: {"type":"none"}`, one text user message
per request, and one input event per request. Agent fields are `model`,
`instructions`, `reasoning.effort`, and tagged function `tools`; omitted fields
inherit from a saved agent, while null instructions/tools clear those fields.
Function results currently accept a text `output` or separate text `error`.
Unsupported configuration, content, event batches, vaults and idempotency keys are
rejected explicitly. Existing prototype size limits below still apply. Reasoning
and provider defaults remain subject to Codex configuration; model-default reset,
usage accounting, incremental text deltas and inherited helper-tool item types
are not yet complete parity. Lists/updates/deletion of agents and sessions are
not implemented.

The runtime tests require the real `codex-app-server` binary: build it with the
command above before `just test -p codex-agents-api`. They exercise the actual
socket handshake, readiness timeout, process exit, home ownership, and shutdown.
Unix CLI tests additionally cover managed session execution and resume across
process restarts, and preservation of external workers.

The Python SDK acceptance test is optional in the default Rust suite because it
requires an external Python environment. Run it explicitly with:

```sh
uv venv /tmp/codex-agents-api-sdk
uv pip install --python /tmp/codex-agents-api-sdk/bin/python openai==3.17.0
CODEX_AGENTS_API_SDK_PYTHON=/tmp/codex-agents-api-sdk/bin/python just test -p codex-agents-api --run-ignored all
```

It uses strict SDK response validation against real in-process Codex with a mock
model, and covers reconnect, function success/error, pagination, cancellation,
snapshot overrides, and completed-session resume after service restart.

## Original prototype routes

| Method | Path | Body / result |
| --- | --- | --- |
| POST | `/v1/agents` | `{ "model": "…", "instructions": "…" }` → saved Agent |
| GET | `/v1/agents/{id}` | Saved Agent |
| POST | `/v1/sessions` | `{ "agentId": "…", "environment": { "type": "none" } }` → Session |
| GET | `/v1/sessions/{id}` | Configuration snapshot, thread binding, `requiredActions`, and `unresolvedActions` |
| POST | `/v1/sessions/{id}/input` | `{ "input": "…" }` → accepted Codex turn; steers if active |
| GET | `/v1/sessions/{id}/events` | SSE of session-scoped app-server notifications |
| GET | `/v1/sessions/{id}/turns?limit=20&cursor=…` | Paginated Codex turn summaries and outcomes |
| POST | `/v1/sessions/{id}/turns/{turnId}/cancel` | Interrupt that turn |
| POST | `/v1/sessions/{id}/turns/{turnId}/tool-results` | `{ "callId": "…", "success": true, "output": { "status": "shipped" } }` → submission receipt |

Subscribe before submitting input. Streams are live-only, with a 128-event
buffer; `stream/lagged` means retrieve saved turns. A disconnect requires
restarting the API connection and reading saved outcomes before retrying input.
Input requests are not deduplicated. Canceling a turn does not undo tool effects.

Agents are immutable. An Agent can additionally specify:

```json
{
  "model": "your-model",
  "instructions": "Look up the requested order.",
  "reasoning": { "effort": "low" },
  "tools": [{
    "name": "lookup_order",
    "description": "Look up an order by ID",
    "parameters": {
      "type": "object",
      "properties": { "orderId": { "type": "string" } },
      "required": ["orderId"]
    }
  }],
  "mcpServers": [{ "server": "warehouse", "allowedTools": ["lookup"] }]
}
```

Function definitions use Codex's supported JSON Schema subset. Limits: 16 functions,
1024 serialized UTF-8 bytes per definition; 16 MCP selections, 32 tool names each;
1024 instruction bytes, 8192 input bytes, and 16 KiB request bodies.
Session creation copies the configuration. Function definitions persist with the
Codex thread; MCP policy is applied at start and cold resume. Reasoning settings
remain subject to the selected model's support.

MCP selections refer to server-side configuration, resolved for the session's
workspace. Unknown or disabled servers and tools forbidden by server-side filters
are rejected before execution. Unselected servers, plugins, apps, web search, and
agent spawning are disabled for these threads. Omitted `tools`/`mcpServers` mean
empty lists, including when loading older Agent records. This replaces the first
stage's inherited MCP behavior. Addresses, credentials, and approval policy remain
server-side; configure MCP tools for unattended use there if appropriate.
The `allowedTools` list filters tools, not resources exposed by a selected server.
Other Codex helper tools (for example, goals and MCP resource access) remain under
the harness policy; `tools` is not an allowlist for all built-in Codex tools.

Application function requests are persisted before `session.requires_action` is
emitted. Retrieve `requiredActions` if the event was missed. Each action contains
`turnId`, `callId`, `name`, and `arguments`. Submit a result to the tool-results
route with the same IDs; `success: false` delivers a tool failure to the model.
Output may be JSON or text, limited to 1024 serialized UTF-8 bytes. Resolving the
request continues the same turn. MCP tools execute through Codex without this
application callback.

Identical result retries return the stored `submitted` receipt; conflicting,
cancelled, or unavailable calls return 409. Wrong session/turn/call IDs return 404.
`submitted` means the result was handed to the app-server connection, not that
the turn completed or external side effects executed exactly once. Pending calls
whose connection is lost appear in `unresolvedActions`, including after restart;
their old JSON-RPC waiters cannot be restored from SQLite. Reconcile saved turn
history before issuing more work. No tool call or result is automatically replayed.

`{ "type": "local", "cwd": "/absolute/workspace" }` selects the app-server's
local executor with read-only sandboxing. `none` disables execution-environment
access. Other interactive requests (approvals, user questions, MCP elicitation)
are explicitly rejected, not left waiting.
Empty sessions are saved immediately; the first input creates and persists a
named Codex thread before recording its binding. No turn is automatically replayed.

Validation: `just test -p codex-agents-api`. Integration tests use the real
in-process app-server with mock model responses and a local HTTP MCP server;
they need localhost access but no model credentials. Coverage includes capability
isolation, function results, MCP execution, concurrent pending sessions, result
retries, cancellation, and restart behavior. Real-provider and remote-socket
acceptance remain separate checks.

Review stages: capability configuration (`capabilities.rs` and Agent fields),
then the durable function bridge (`actions.rs`, store, routes, and worker), with
the integration fixture validating their combined behavior. Agent updates,
approval endpoints, environment provisioning, and distributed recovery remain
subsequent stages.
