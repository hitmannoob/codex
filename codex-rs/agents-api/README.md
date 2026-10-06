# Codex Agents API

A local HTTP facade over a dedicated Codex app-server from this checkout.
Agent definitions and session bindings live in `agents-api.sqlite`; Codex owns
the execution history. Normalized API turns and items also live in SQLite. Run
one API process per data directory and retain the app-server's `CODEX_HOME`
alongside it. Startup takes an advisory lock (`agents-api.lock`) on the data
directory for the life of the process, so a second API pointed at the same
directory is rejected in every worker mode rather than becoming a rival writer. The text/function session path implements a subset of the OpenAI
wire contract, pinned to the official Python SDK 3.17.0. Full compatibility
remains incomplete. The target is documented function-for-function parity; see
the [contract inventory](CONTRACT_INVENTORY.md), [parity plan](PARITY.md), and
[implementation goals](GOALS.md).

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
worker's inherited environment. The HTTP bearer token and the vault passphrase
are not forwarded to the worker. This dedicated home is not the operator's default `~/.codex`.

Managed startup locks the worker home, creates a private temporary socket, and
waits for the app-server initialization handshake before accepting HTTP requests.
`--worker-startup-timeout-secs` bounds that wait (default 30). Startup failures
clean up the child. On Ctrl-C or SIGTERM (Unix), the API stops accepting requests,
closes its backend connection, and requests worker shutdown, with a 10-second
worker grace period before forced termination and reaping. Shutdown can interrupt
active turns; it does not promise to finish them. Completed sessions can resume
using the retained API data directory and worker home.

Unexpected managed-worker exit no longer stops the CLI. A supervisor reaps the
crashed worker and respawns it with bounded attempts and exponential backoff,
completes the initialization handshake, and reattaches it via
`AgentsApi::reconnect`; the process exits only if restarts are exhausted.
Throughout the gap, saved history stays readable, mutations return 503
`app-server disconnected`, pending function calls become `unresolvedActions`,
and stale callbacks cannot resolve the replacement's work. Interrupted work is
not replayed. Externally managed workers are never restarted.

Operational diagnostics:
- **Logs:** go to stderr through `tracing`. `RUST_LOG` filters them (default
  `codex_agents_api=info`), and `LOG_FORMAT=json` writes JSON lines. Events
  carry `session_id`, `turn_id`, and similar fields. Events inside a request
  also carry its `http.request` span: `request_id`, `method`, `route`, and
  `status`.
- **Handshake lines:** the `agents-api managed worker pid=` and
  `agents-api listening on` lines stay plain, because supervisors parse them.
- **Request IDs:** every response carries `x-request-id: req_…`, including
  errors, rejected credentials, and unknown routes. The OpenAI SDK exposes it
  as `_request_id`, so a failed call can be matched to the log.
- **Export:** `--otlp-endpoint <collector base URL>` (or
  `CODEX_AGENTS_API_OTLP_ENDPOINT`) sends spans and metrics to
  `<url>/v1/traces` and `<url>/v1/metrics` over OTLP/HTTP, through the
  workspace's `codex-otel`. `--otlp-header NAME=VALUE` adds a header, repeated
  as needed, and `--otel-environment` names the deployment. Nothing is
  exported without an endpoint, and shutdown flushes pending exports.

Metrics (route tags use the route template, with braces replaced):

| Metric | Tags |
| --- | --- |
| `agents_api.http.request`, `.duration_ms` | `method`, `route`, `status` |
| `agents_api.turn`, `.duration_ms` | `status`, `agent_type` (root or subagent) |
| `agents_api.tool.call` | `type` (item type), `status` |
| `agents_api.webhook.delivery` | `outcome` (delivered, retry, failed) |
| `agents_api.backend.connection` | `event` (connected, lost) |
| `agents_api.session.cleanup` | `outcome` (deleted, failed) |
| `agents_api.worker.startup.duration_ms` | `reason` (start, restart) |
| `agents_api.worker.restart` | `outcome` (restarted, failed) |

Model request latency is measured inside the worker. Configure its own
`[otel]` section in the worker home's `config.toml` to export it.

An API-process crash (for example SIGKILL of the API itself) skips graceful
shutdown, orphaning the managed worker. On Unix, the next managed start reclaims
it before spawning: an ownership record written at spawn time
(`agents-api-worker.json` in the worker home, holding the worker PID and its
process start time) is re-read, and a live process matching both PID and start
time — the start time defeats PID reuse — is terminated and awaited so two
app-servers never share one home. A stale or reused record is cleared without
signaling anything. Windows orphan containment is not yet implemented, and this
reclaim applies only to managed workers, never to an external worker. This worker
is the Codex harness; it does not provision a sandbox or launch a separate
executor.

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

## Operating the service

- **Authentication:** one operator bearer token (at least 32 bytes) grants
  the whole API; there is no project or user scoping. The environment key
  grants only the `/registry/` routes executors use, and the worker's harness
  token never leaves the process. Pass secrets through
  `CODEX_AGENTS_API_TOKEN`, `CODEX_AGENTS_API_VAULT_PASSPHRASE`, and
  `CODEX_AGENTS_API_ENVIRONMENT_KEY`. They are hidden from `--help` output
  and never forwarded to the worker.
- **Network exposure:** the service listens on `127.0.0.1:4501` by default.
  To serve other hosts, put it behind a TLS-terminating proxy that sets
  `x-forwarded-proto: https`; `remote_url` and the registry's websocket URLs
  follow it. Executors must reach `/registry/`. Service-origin MCP and
  webhook connections go only to public addresses unless the operator allows
  a host. Executor-origin traffic uses the caller's network.
- **Health:** `GET /healthz` needs no token. It answers 200
  `{"status":"ok","worker":"connected"}`, or 503 with `"degraded"` while the
  worker is reconnecting; saved records stay readable meanwhile.
- **State and backup:** everything durable lives in the data directory:
  - `agents-api.sqlite` (with its WAL) for public records, queues, and
    metadata;
  - `secrets/` for age-encrypted credential, webhook, and env values;
  - `files/` for uploads;
  - `codex-home/` for the worker's rollouts, which sessions need to resume.
  Back them up together while the service is stopped. A restore needs the
  same vault passphrase. One process owns a data directory at a time
  (`agents-api.lock`).
- **Admission limits:**
  - JSON bodies 16 KiB; environment files 5 MiB decoded; uploads 50 MiB,
    streamed;
  - 1–32 events per input batch; 10,000-byte function results;
  - 16 tools and 16 MCP servers per agent; metadata of 16 pairs;
  - lists of 1–100 (files 1–10,000);
  - 256 harness sockets per environment and 256 KiB relay frames.
  Input to one session is serialized. There is no global rate limit or
  per-tenant quota, so add one at the proxy if you need it.
- **Resource growth:** uploads stay until deleted or expired, and rollouts
  and records stay until their session is deleted. Watch the data
  directory's disk use. One worker serves every session, and there is no
  distributed routing.
- **Deployment:** SIGTERM stops accepting requests, then stops the worker
  after a grace period, interrupting running turns rather than finishing
  them. On restart:
  - completed sessions resume;
  - webhook deliveries are retried;
  - executors re-register on their own;
  - input that was waiting for an executor fails its session rather than
    being replayed.

## Official-style session path

The SDK sends `OpenAI-Beta: agents=v1`. This selects flattened, tagged-function
payloads for saved-agent operations at `/v1/agents`. The original payloads
remain available without that header. New session routes are:

| Method | Path | Behavior |
| --- | --- | --- |
| POST / GET | `/v1/agents` | Create saved agents / list in creation order |
| GET / POST / DELETE | `/v1/agents/{id}` | Retrieve, update, or delete a saved agent |
| POST | `/v1/agents/sessions` | Inline agent or saved `agent_id` plus overrides, environment `none` or `self_hosted` (`openai_hosted` is not supported), initial input (required for `none`, optional for `self_hosted`), optional SSE |
| GET | `/v1/agents/sessions` | List in creation order, optionally filtered by `agent_id` |
| GET | `/v1/agents/sessions/{id}` | Configuration snapshot, status, metadata and current `required_actions` |
| POST | `/v1/agents/sessions/{id}` | Replace metadata; change model, reasoning effort, or service tier for later turns |
| DELETE | `/v1/agents/sessions/{id}` | Delete a session with no running turn; its Codex thread is removed afterwards |
| POST | `/v1/agents/sessions/{id}/events` | Batch of message/steering, cancel, and function result/error events; optional `Idempotency-Key`; empty HTTP 202 |
| GET | `/v1/agents/sessions/{id}/events` | Live session, turn, item, text/reasoning delta, content-part and `error` events |
| GET | `/v1/agents/sessions/{id}/items` | Saved messages, reasoning and function records |
| GET | `/v1/agents/sessions/{id}/turns` | Saved outcomes |
| GET | `/v1/agents/sessions/{id}/turns/{turn_id}` | One saved outcome |
| GET | `/v1/agents/environments/{id}` | A self-hosted environment's status (`pending`, `connected`, `disconnected`) |
| POST / GET | `/v1/agents/environments/{id}/files` | Write an inline or `file_id` file into, or list files in, a connected environment's workspace |
| POST / GET | `/v1/files` | Upload (multipart, at most 50 MiB, optional `expires_after`) or list files |
| GET / DELETE | `/v1/files/{id}` | Retrieve or delete an uploaded file; `/content` streams it after checking its SHA-256 |
| GET | `/v1/agents/sessions/{id}/artifacts` | Always empty: self-hosted files are not published as artifacts (retrieve, content, and delete return 404) |

Lists accept `after`, `order=asc|desc`, and `limit=1..100`; item lists also accept
`turn_id`. Records persist before their events are emitted. Disconnecting an HTTP
stream does not cancel work. Streams do not replay; reconnect, read current state
and history, and merge buffered updates by item ID. A lagged stream closes.

Assistant messages stream as `item.added`, `content_part.added`, one or more
`output_text.delta`, `output_text.done`, `content_part.done`, and `item.done`.
Reasoning summaries follow the same shape with `reasoning_summary_part.*` and
`reasoning_summary_text.*` events per summary index. Deltas and part events are
live only; the saved item holds the complete text. When a turn ends, any item
it left in progress is saved and closed as `incomplete`. A final provider error
emits an `error` event before `turn.failed`. Failed turns carry the documented
error codes (for example `context_length_exceeded` or `connection_failed`); a
Codex error without a public counterpart is `internal_error`.

Turn and session `usage` is best effort. It comes from Codex's running token
totals: each update adds only its increase to the turn, so repeated updates and
the totals Codex replays on resume are never counted twice. Usage stays `null`
until a response reports some; missing usage is unknown, not zero. Cached tokens
are included in input, and reasoning tokens in output.
Backend notification loss fails the connection rather than serving incomplete
history as healthy. Backend loss marks active public turns failed, without replay.

Sessions accept environment `none` or `self_hosted` (see below) and one user
message per message event. Saved agents support names,
metadata, model/instructions, reasoning effort/summary, text format/verbosity,
service tier, multi-agent configuration, and the pinned SDK tool variants.
Omitted fields preserve saved values; supplied objects replace them; null resets
optional settings. Metadata accepts at most 16 string pairs, with keys up to 64
characters and values up to 512. Lists default to 20 entries, descending creation
order, and reject unknown/deleted cursors. Updates use an atomic comparison to
prevent lost writes (a concurrent modification returns 409).

Session snapshots are independent of later saved-agent updates/deletion and
retain their settings across cold resume. Reasoning resets use the model's
default effort, summaries reset to disabled, verbosity to medium, and text format
to ordinary text. These values override inherited worker defaults. Provider
support still governs available model settings. `fast` maps to `priority` and
requires advertised model support. An explicit `default` tier is forwarded to
Responses, including after cold resume; omitted/`auto` leaves the tier unset.

Session updates follow the same omitted/null rules for their four fields. A
running turn keeps the settings it started with; the next turn uses the update.
Deleting a session that is `in_progress` or `requires_action`, or whose worker
thread is still active, returns 409: cancel first. Deletion removes the public
records at once, ends that session's open streams, and queues the Codex thread
for deletion in the worker. The queue survives API restarts and is retried on
each backend connection, so a session with a thread needs a connected backend
to be deleted (otherwise 503).

An events request holds 1–32 events. Any number of tool results may be combined
with at most one message or one cancel event, never both, and each call may be
resolved only once. The whole batch is validated first: an unknown pending
call, a call that is no longer pending, or a disconnected backend rejects it
before anything runs. Events then run in array order and stop at the first
failure; earlier events stay applied. An unknown call returns 400 with code
`invalid_request_error` and message `Unknown pending tool call: <call_id>`,
which the pinned SDK retries because a call's item event can arrive before the
call is registered. A message holds `input_text` parts totalling at most 8,192
bytes plus `input_image` parts. Images must be `data:image/` URLs: Codex rejects
remote image URLs, and invalid image data is left to Codex's image preparation.

`Idempotency-Key` (1–255 visible ASCII characters) is scoped to one session and
retained for 24 hours:
- Retrying the identical request replays its stored status.
- Reusing the key for a different request returns 400.
- Retrying while the first attempt still runs returns 409.
- A request rejected before anything ran (validation, missing session, or
  disconnected backend) leaves the key unused.
- If an attempt was interrupted after dispatch (backend loss, an internal error,
  or an API crash), the key reports an unknown outcome with 409 and never runs
  the request again. Read the session and resubmit with a new key.

The key deduplicates HTTP requests; it does not make execution exactly-once.

Input admission is serialized per session: simultaneous inputs to one session
are applied one at a time, starting or steering its turn, while other sessions
proceed concurrently. Each Codex thread is resumed once per backend connection.
Input that steers a turn just as it ends is answered by a follow-up turn:
Codex records input that arrives after a turn's last model request without
answering it, so the session starts another turn instead of going idle. A
cancel sent before Codex has reported the turn it just started (for example,
right after creating a session with input) still stops that turn.

Deferred functions, tool search, enabled web search, and enabled programmatic
calling can be saved/retrieved but their execution is rejected. Deferred functions and web search depend on model and
provider support the worker does not report, so enabling them could leave the
model silently without the tool. Disabled programmatic calling/web search are
accepted. Prototype size limits below still apply to the original routes.

`multi_agent: {enabled: true}` turns on Codex's V2 multi-agent runtime. The
root agent can spawn subagents, each a Codex child thread, up to
`max_concurrent_subagents` (default 6). How subagents appear:
- **Endpoints:** each subagent is available from `/subagents`, with its own
  turn and item history there. Session item and turn lists hold only the root
  agent's history.
- **Events:** spawns emit `agent.session.subagent.created`. Subagent turn
  events appear on the session stream with `turn.subagent_id`; only root turns
  change the session's status.
- **Call items:** root history records `create_subagent_call`,
  `send_subagent_input_call`, and `interrupt_subagent_call`. Codex does not
  report the task text, model, or effort for these, so `content` is empty and
  the settings are null. A `create_subagent_call`'s `agent_id` is the agent
  that requested it, as the SDK documents. The new subagent's ID arrives in
  `agent.session.subagent.created`, and its `parent_agent_id` names the
  requester.
- **Delegation quality depends on the model:** see G12 in GOALS.md. With models
  Codex does not treat as V2, subagents cannot delegate and may announce a
  spawn instead of doing their task.
- **Tools and usage:** subagents have no function tools, and their usage counts
  toward the session.
- **Deletion:** a session whose subagent is still working cannot be deleted.

Cancelling (`agent.session.input.cancel`) stops all of the session's running
work: its own turn and any turn a subagent is running. After a restart, the
root can hand an existing subagent more work, and its new turns are recorded
under the same subagent. Not supported with V2: `wait_for_subagents_call`,
`close_subagent_call`, `resume_subagent_call`, and `closed` status.
Codex's goal tools are disabled for every session.

Public MCP servers run when they use HTTP with `connection_origin: service`
(the default). Each becomes a session-scoped Codex server that sets only the
URL, non-secret `headers`, `allowed_tools` (all tools when omitted), and
`required`.
- **Unsupported configurations:** stdio servers and environment-origin
  connections need an execution environment. An `Authorization` header, inline
  `authorization`, and `request_metadata` are rejected, as is a label that
  matches a server configured in the worker. Authenticate with a vault
  credential instead (below).
- **Egress:** the worker host makes these connections, so a server URL must use
  https and resolve only to public addresses. Loopback, private, link-local
  (including cloud metadata), and shared addresses are refused. The check runs
  before the session is created and again before every turn. An operator can
  allow specific hosts, internal or plain http, with repeated
  `--allow-mcp-host <host>` flags or `AgentsApi::allow_mcp_hosts`. DNS is
  checked at admission and could still change before Codex connects.
- **Approvals:** tool calls run without approval prompts; the caller's
  `allowed_tools` is the approval.
- **Resources:** Codex's MCP resource tools (`list_mcp_resources`,
  `read_mcp_resource`, `list_mcp_resource_templates`) are turned off through
  its `mcp_resources` feature. They would let the model read any resource of
  a server, beyond `allowed_tools`, and the public schema has no resource
  selection.
- **Items:** calls appear as `mcp_call` items with the server label, tool name,
  arguments, output content, and error.
- **Required servers:** a `required` server that cannot initialize fails the
  turn with `connection_failed`, as the guide documents. Other provider
  rejections keep only the provider's error message, never its raw body (which
  can name the provider account). They are classified by HTTP status: 4xx is
  `invalid_request` (401/403 `authentication_error`, 429
  `rate_limit_exceeded`), and 5xx is `server_error`. The request itself
  succeeds. The failed turn gets a service-assigned ID, because Codex never
  starts it.

Self-hosted environments run a session's commands on compute the caller owns.
They are available only when the operator supplies an environment key of at
least 32 bytes, different from the API token, with `--environment-key` (or
`CODEX_AGENTS_API_ENVIRONMENT_KEY`); otherwise such sessions return 501. The key
grants only the registry endpoints under `/registry/`, never the API.
- **Creating:** `environment: {"type":"self_hosted","workspace_directory":"/abs/path"}`.
  The workspace path belongs to the executor's OS, so POSIX and Windows absolute
  forms are both accepted. Skills in `capability_directories` (up to 32
  absolute paths on the executor) are offered to the agent, and so are plugins
  found there. Plugins installed in the worker's own Codex home stay off, as
  do remote plugins. The response's
  `environment` carries its `id` and `remote_url`, this service's
  `/registry` on the host and scheme the request used (`x-forwarded-proto`
  selects https).
- **Connecting:** on the caller's compute, run
  `CODEX_API_KEY=<environment key> codex exec-server --remote <remote_url> --environment-id <id>`.
  The stock CLI sends that key only to OpenAI hosts or loopback; add
  `--trusted-registry-host <host>` for this service's https host. The executor
  registers a Noise key and dials out to a rendezvous websocket. The worker
  reaches it through the same relay over loopback, and traffic is encrypted end
  to end: the relay sees only stream IDs.
- **Events:** `agent.session.environment.pending` at creation, then `connected`
  and `disconnected` as the executor's socket opens and closes, and `failed`
  when waiting input times out.
- **Waiting input:** input sent while the executor is offline sets the session
  to `requires_action` with an `environment_connection` action. It waits up to
  five minutes (`--environment-connection-wait-secs` changes this), then
  starts in order. Input that times out is dropped and the
  session fails; a later connection does not replay it. Cancelling drops waiting
  input and returns the session to idle, and a service restart drops it and
  fails the session.
- **Execution:** commands run unsandboxed in the workspace directory; the
  caller's compute is the isolation boundary, and code there can read the
  environment key. Each command is a `command_execution` item with its exit
  code and output. If the executor goes away mid-command, Codex tries for 25
  seconds to resume, then reports the command failed to the model; it is never
  re-run. A new executor, or a replaced worker, continues the session in the
  same workspace.
- **Files:** `POST /v1/agents/environments/{id}/files` writes
  `{"type":"inline","path":…,"data":<base64>}` (at most 5 MiB) on the executor.
  The path must lie inside the workspace once `.` and `..` are resolved;
  parents are created and links are not followed. `GET` lists files under the
  workspace or `path`, ordered by path components (`order`, `limit` 1..100,
  opaque `page` token). The executor must be connected (409 otherwise).
  `{"type":"file_id",…}` copies a `/v1/files` upload instead.
- **MCP:** stdio MCP servers run on the executor with the given `cwd`, and
  `env_vars` are read from the executor's environment. Literal `env` values
  are secrets: they need the vault passphrase, are stored encrypted, are never
  returned, and a session copies its agent's values when it is created. HTTP servers with `connection_origin: "environment"` connect
  from the executor, so the service's egress policy does not apply to them.
  Both need a self-hosted session.
- **Deletion:** removes the environment from the registry and the worker. The
  caller's executor keeps running (its registration attempts get 404), and
  stopping it is the caller's job.

Vaults hold MCP credentials. They are available only when the operator
supplies a passphrase of at least 32 bytes with `--vault-passphrase` (or
`CODEX_AGENTS_API_VAULT_PASSPHRASE`); otherwise credential operations return
501. Keep that passphrase: stored secrets cannot be read without it, and a
start with a passphrase that cannot read them fails with an error naming the
secrets directory.
- **Storage:** vault and credential metadata live in the API database. Secret
  values are encrypted with age (scrypt) in `DATA_DIRECTORY/secrets`. They are
  never returned by the API or stored in public records, and they never reach
  the model. The deliberately slow key derivation makes each credential write
  take about a second.
- **Credential types:** `static_bearer` and `mcp_oauth` send their token as the
  MCP server's `Authorization: Bearer` header. `mcp_server_url` must use https
  (plain http only for an operator-allowed MCP host). `mcp_oauth` refresh
  settings are rejected, since the service does not refresh tokens.
  `environment_variable` credentials need an OpenAI-hosted environment and are
  rejected.
- **Matching:** a session lists up to 32 `vault_ids`. Each HTTP MCP server
  takes the credential its `credential_id` names, which must be in those
  vaults, or else the only credential whose `mcp_server_url` equals its URL.
  Several matches are rejected; set `credential_id` to choose.
- **Rotation and deletion:** a session copies its secrets when it is created.
  Rotating or deleting a credential, or deleting its vault, changes only
  sessions created afterwards, as the guide documents.
- **Removal:** deleting a credential, vault, session, or webhook endpoint
  commits first, then removes its secrets. Anything a crash or a missing
  passphrase leaves behind is removed the next time the passphrase is
  configured. Startup already reads the store once to check the passphrase, so
  this costs one extra write only when orphans exist.

`GET /v1/agents/sessions/{id}/traces` exports each finished turn as one
OpenTelemetry (OTLP JSON) trace, paged with `limit` (default 20, at most 100),
`order` (default `desc`), and `after`. The pinned SDK has no method for it, so
call it over HTTP. Each page item is
`{id: "trace_<turn>", object: "agent.session.trace", session_id, turn_id, created_at, otlp: {resourceSpans}}`,
so `jq '{resourceSpans: [.data[].otlp.resourceSpans[]]}'` makes one payload
for an OTLP/HTTP collector, as the tracing guide shows.
- **Spans:** the root agent's span holds the turn's model responses (`chat`)
  and tool calls (`execute_tool <name>`). Each subagent that ran during the
  turn gets its own agent span beneath the root, holding its own work.
  Attributes use the OpenTelemetry GenAI names (`gen_ai.*`), with
  `openai.agents.*` for the agent type, status, and tool call and result.
  String attributes are cut at 32 KiB.
- **Timing:** times are when the service observed each turn and item.
  Responses are split by item order: a response starts when its input is ready
  and ends at its last output. Usage is attached per response when every
  response in the turn reported it. Turns recorded before this version export
  with turn-level timing only.
- **Not recorded:** the model (a session update can change it between turns)
  and subagent instructions. Span IDs are derived from the records, so a
  repeated export returns the same trace.

Webhook endpoints are managed with the SDK's `client.webhooks` API
(`/v1/webhook_endpoints`, plus `/v1/webhook_event_types`). They need the vault
passphrase, because signing secrets are kept in the same encrypted store.
- **Event types:** `agent.session.created`, `agent.session.action_required`,
  `agent.session.in_progress`, `agent.session.idle`, and
  `agent.session.failed`. Repeated types are collapsed. Other OpenAI event
  types are rejected, since this service never produces them.
- **Secrets:** the `whsec_` signing secret is returned only when an endpoint is
  created or its secret rotated. Responses otherwise carry
  `signing_secret_hint`. Rotation with
  `keep_old_secret_active_for_24_hours` signs with both secrets for a day.
- **Signing:** requests follow the Standard Webhooks scheme that
  `client.webhooks.verify_signature` checks, using the `webhook-id`,
  `webhook-timestamp`, and `webhook-signature` headers.
- **Egress:** receiver URLs must use https and resolve only to public
  addresses. The operator can allow specific hosts, internal or plain http,
  with repeated `--allow-webhook-host <host>` flags or
  `AgentsApi::allow_webhook_hosts`. Each delivery re-checks the addresses and
  connects only to those it checked.
- **Test deliveries:** `test` sends one signed sample event and reports the
  receiver's status code. Redirects are not followed, and an unreachable
  receiver returns 502.
- **Session events:** each status change queues one delivery per subscribed
  endpoint, in the transaction that records the change. The body is
  `{id: "evt_…", object: "event", created_at, type, data: {id: <session>}}`,
  and `action_required` adds `required_action: {type: "function_call"}`.
  Sessions the service fails on a lost backend or restart report
  `agent.session.failed`.
- **Retries:** any status other than 2xx, a redirect, or an unreachable
  receiver fails the attempt. Retries back off from 5 seconds, doubling to at
  most an hour, for 72 hours, and continue across restarts. Delivery is at
  least once: each retry repeats the `webhook-id`, so receivers can drop
  duplicates. Events to an endpoint are attempted in order, but a failed one
  retries on its own schedule, so order by `created_at` when it matters.
  Given-up deliveries are logged without their content.

A successful function result carries `output`: a string, or an array of
`input_text` and `input_image` parts. Images must be `data:image/` URLs, and
JSON objects must be serialized to strings first. A failed result carries its
message in `error`. Mixing `output` with `success: false`, or `error` with
`success: true`, returns 400. Result text is limited to 10,000 UTF-8 bytes,
because Codex silently truncates longer tool output (10,000 bytes, or 10,000
tokens for catalogued models); rejecting it keeps what the model sees equal to
what is saved. The saved `function_call_output` item keeps the result exactly
as submitted. A result rejected by validation leaves its call pending. A result
for a call that was cancelled, or already resolved differently, returns 409;
an identical resubmission returns its receipt.

The runtime tests require the real `codex-app-server` binary: build it with the
command above before `just test -p codex-agents-api`. They exercise the actual
socket handshake, readiness timeout, process exit, home ownership, and shutdown.
Unix CLI tests additionally cover managed session execution and resume across
process restarts, managed-worker crash recovery with automatic restart, and
preservation of external workers.

The Python SDK inventory and lifecycle tests are optional in the default Rust
suite because they require an external Python environment. Run them explicitly
with:

```sh
uv venv /tmp/codex-agents-api-sdk
uv pip install --python /tmp/codex-agents-api-sdk/bin/python openai==3.17.0
CODEX_AGENTS_API_SDK_PYTHON=/tmp/codex-agents-api-sdk/bin/python just test -p codex-agents-api --run-ignored all
```

The inventory test detects drift in the pinned SDK resource surface. The
lifecycle test uses strict SDK response validation against real in-process Codex
with a mock model, and covers reconnect, function success/error, pagination,
cancellation, snapshot overrides, vaults and credentials, and completed-session
resume after service restart.

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
buffer; `stream/lagged` means retrieve saved turns. After `stream/disconnected`,
saved reads keep working and mutations return 503 until a replacement backend
is attached; read saved outcomes before retrying input. Input requests are not
deduplicated. Canceling a turn does not undo tool effects.

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
The `allowedTools` list is the complete set of a server's capabilities the model
can use: Codex's goal and MCP resource tools are turned off for every session.
Other Codex helper tools remain under the harness policy; `tools` is not an
allowlist for all built-in Codex tools.

Application function requests are persisted before `session.requires_action` is
emitted. Retrieve `requiredActions` if the event was missed. Each action contains
`turnId`, `callId`, `name`, and `arguments`. Submit a result to the tool-results
route with the same IDs; `success: false` delivers a tool failure to the model.
On this prototype route, output may be JSON or text, limited to 1024 serialized UTF-8 bytes. Resolving the
request continues the same turn. MCP tools execute through Codex without this
application callback.

Identical result retries return the stored `submitted` receipt; conflicting,
cancelled, or unavailable calls return 409. Wrong session/turn/call IDs return 404.
`submitted` means the result was handed to the app-server connection, not that
the turn completed or external side effects executed exactly once. Pending calls
whose connection is lost appear in `unresolvedActions`, including after restart;
their old JSON-RPC waiters cannot be restored from SQLite. No tool call or result
is automatically replayed. On reconnect, calls left uncertain by a disconnect are
reconciled against authoritative rollout history: a call whose turn is now
`completed` must have delivered (a turn cannot complete with an unresolved call),
so its stored receipt is restored and it leaves `unresolvedActions`; any other
turn status keeps the call unresolved for you to reconcile before issuing more
work, and it is never claimed as delivered on incomplete evidence.

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
the integration fixture validating their combined behavior. Approval endpoints,
environment provisioning, and distributed recovery remain subsequent stages.
