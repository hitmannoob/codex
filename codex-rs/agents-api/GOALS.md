# Remaining Agents API parity work

Updated: 2026-09-22.

## Objective and scope

Build our own service implementing the documented Agents API contract, using this
Codex checkout as the harness. Clients should be able to use the supported
official SDK operations by changing the base URL and credentials.

This is the implementation plan for the gaps summarized in [PARITY.md](PARITY.md).
[README.md](README.md) describes behavior available today. The contract baseline
is the previously reviewed Agents API reference and Python SDK `openai==3.17.0`.
G00 pins the full known operation inventory in `CONTRACT_INVENTORY.json`; its
open rows now establish the denominator for parity tracking.

Proposed module names below are implementation suggestions, not existing files
or commitments to create scaffolding before it is needed. Exact public field
names, statuses, limits, and endpoint behavior must come from the pinned contract.
Internal service states must not become invented public API states.

## Current foundation

These capabilities already exist; extend them rather than rebuilding them:

- Saved-agent creation/retrieval, inline configuration, and session snapshots for
  the supported model, instructions, reasoning, and function-tool fields.
- Session creation/retrieval, text input, steering, cancellation, and function
  success/error submission on the supported official-style session routes.
- Durable normalized session, turn, and item records with pagination/filtering.
- Live session events; HTTP stream disconnection does not cancel execution.
- API-owned app-server startup, initialization timeout, persistent worker home,
  private socket, home locking, shutdown, forced termination, and process reaping.
- External-worker mode, where API shutdown leaves the caller's worker running.
- Completed-session continuation after restarting both API and worker.
- Replaceable backend connection: HTTP and durable retrieval survive backend
  loss, mutations report the documented recovery error, and
  `AgentsApi::reconnect` retires the old connection (its pump exits and records
  its disconnect bookkeeping) before attaching a replacement, so stale
  notifications and function callbacks cannot resolve the new connection's work.
- Managed-worker supervisor: a crashed owned worker is reaped, respawned with
  bounded attempts/backoff, reinitialized, and reattached via `reconnect`; the
  CLI keeps serving durable reads throughout and only exits when restarts are
  exhausted. External workers are never restarted.
- Ordered, transactional schema migrations via an sqlx `Migrator`; a
  pre-migration database is adopted in place without data loss.
- Turn and function reconciliation after reconnect: turns and tool-result
  deliveries failed only by a connection loss are re-checked against
  authoritative rollout history and recovered when the worker actually
  completed them, without falsely recovering interrupted work or re-driving a
  lost waiter.
- API-process crash recovery on Unix: a worker orphaned by an API SIGKILL is
  authenticated (PID plus process start time) and reclaimed on the next managed
  startup before a replacement spawns, so two app-servers never share a home.

Latest verification (2026-09-24): 19 tests passed via
`just test -p codex-agents-api`, including backend-replacement fencing,
worker-SIGKILL/reconnect at the library, the managed-worker crash-and-restart
CLI test, the migration ledger/legacy-adoption test, the turn- and
function-reconciliation recovery tests, and the API-crash orphan-reclaim test;
the two pinned-SDK tests were skipped (no `CODEX_AGENTS_API_SDK_PYTHON`
configured) and were last run for G00. `just fmt` and
`just fix -p codex-agents-api` passed. Execution was on macOS with a mock
provider. Windows execution, real-provider acceptance, and full API parity
remain unverified.

Current boundaries:

- One managed app-server serves this API process's sessions. A crashed managed
  worker is restarted with bounded backoff while durable reads keep serving (the
  CLI exits only when restart attempts are exhausted), and a worker orphaned by
  an API-process crash is authenticated and reclaimed on the next Unix startup.
  Windows orphan containment and the brief spawn-to-record window are not yet
  covered.
- Official-style sessions currently accept environment `none`. The original
  prototype also supports a read-only local workspace; that is not managed
  sandbox provisioning or self-hosted environment parity.
- Event streams are live-only. Recovery reads saved state/history; it must not
  promise replay of missed events.
- Persisted function records cannot recreate a lost JSON-RPC request waiter.
  Calls interrupted by backend loss become unresolved/unavailable.
- Input/configuration/function-output limits remain prototype limits, including
  the 1,024-byte serialized function-output limit.

## Execution order and tracking

All goals below are open unless explicitly marked complete with evidence.
Dependencies identify prerequisites for completion; contract research and test
fixture design may start earlier.

| ID | Goal | Dependencies | Suggested landing units |
| --- | --- | --- | --- |
| G00 | Complete and pin the contract inventory (complete) | None | Inventory, fixtures, coverage mapping |
| G01 | Keep the service available through worker failure | G00 recovery/error contract | Replaceable connection, supervisor, reconciliation, process-crash handling |
| G02 | Version persistence and define ownership | Existing store; coordinate with G01 | Migrations, durable identity, transactional updates |
| G03 | Complete saved agents and configuration | G00, G02 | List, update/delete, field families |
| G04 | Complete sessions and input semantics | G00, G01, G02 | List, update/delete, input variants, idempotency |
| G05 | Complete events, items, turns, and usage | G00, G02 | Text deltas, item families, transitions, usage |
| G06 | Complete function-tool behavior | G00, G04, G05 | Content/limits, deferred tools, failure cases |
| G07 | Complete MCP and built-in controls | G00, G03, G05 | MCP, built-in capabilities, isolation |
| G08 | Expose remaining harness capabilities | G00, G03, G05, G07 | Delegation, compaction, programmatic calls, skills/plugins |
| G09 | Implement environment lifecycle | G00, G01, G02 | Self-hosted attachment, managed provisioning, lifecycle recovery |
| G10 | Implement files and artifacts | G00, G02; G09 for environment transfer | Storage contract, transfer, publication, cleanup |
| G11 | Implement credentials and service integrations | G00, G02, G05 | Vaults, webhooks, observability |
| G12 | Prove end-to-end parity | All applicable goals | Contract suite, real providers, platforms, failure matrix |

Ship bounded changes within each goal. Do not combine a supervisor rewrite,
public-schema expansion, and sandbox provider integration into one change.
Prefer changes under 500 lines for complex logic and under 800 changed lines
unless a documented exception is justified.

## G00 — Complete and pin the contract inventory (complete)

**Outcome:** every required public behavior has a source, implementation status,
and an acceptance test or an explicit uncovered gap.

Implementation:

- [x] Enumerate every operation and resource from the official reference and the
  pinned SDK: methods, paths, beta headers, parameters, responses, and errors.
- [x] Enumerate event/item unions, lifecycle transitions, pagination behavior,
  supported content types, size limits, and omitted/null/update semantics.
- [x] Investigate the currently incomplete vault, webhook, environment, file,
  usage, and provider-specific capability inventory. Do not implement guessed
  endpoints or signing formats.
- [x] Record an inventory entry per operation or meaningful behavior: source URL,
  review date, SDK version, implementation location, test name, evidence type,
  status (`missing`, `partial`, `implemented`, `verified`), and remaining issue.
- [x] Capture small request/response fixtures and add reusable SDK test helpers.
  Validate errors and returned objects, not just successful HTTP status codes.
- [x] Define how intentional compatibility changes update the pinned baseline
  without silently changing existing tests.

Acceptance: no known public operation is omitted from the inventory; unsupported
variants are named explicitly; each subsequent implementation change updates its
coverage entry. This goal establishes the denominator for parity tracking.

Completion evidence (2026-09-22): `CONTRACT_INVENTORY.json` records 43 Agents
API operations plus 16 required Files/Skills operations, the documented unions,
and 18 cross-cutting behavior rows. `tests/sdk_inventory.py` resolved all 58
SDK-backed operations against `openai==3.17.0`; trace export is the one documented
raw-HTTP operation absent from that SDK. The full crate run passed 15 tests with
ignored tests enabled, including strict fixture parsing and the SDK lifecycle.
Execution used macOS and a mock model; platform and real-provider evidence remain
separate G12 work.

## G01 — Worker supervision, reconnect, and reconciliation

**Outcome:** the HTTP service can survive worker failure and restore a usable
backend without misrepresenting interrupted work as successfully resumed.

Starting points: `src/runtime.rs`, `src/main.rs`, `src/lib.rs`, `src/actions.rs`,
`src/records.rs`, and `tests/suite/runtime_cli.rs`.

Implementation, in order:

- [x] Introduce an internal owner for the current backend connection and its
  generation. Replace the fixed request handle in application state with access
  to a ready generation; do not hold a shared lock across backend I/O.
- [x] Separate HTTP/store lifetime from backend notification-pump lifetime. Keep
  durable retrieval available while the backend reconnects. Define mutation
  responses during recovery from the documented error contract; do not silently
  enqueue or retry execution-changing requests.
- [x] Add bounded restart attempts/backoff to the owned worker supervisor. Each
  replacement must complete initialization before accepting execution requests.
  Expose exhausted restart attempts operationally instead of spinning forever.
- [x] Fence old-generation notifications, request IDs, and function callbacks so
  they cannot update or resolve work belonging to a replacement connection.
- [x] Reconcile persisted sessions and active turns with authoritative Codex thread
  history. Recover completed outcomes where evidence exists. Mark interrupted
  work according to the contract when completion cannot be established.
- [x] Treat functions in pending, submitting, and submitted states separately.
  Preserve known receipts; never recreate a waiter from its saved numeric ID or
  replay an external side effect merely because its acknowledgment was lost.
- [x] Handle API-process crashes separately from worker crashes. Choose and test
  an OS-appropriate child-containment or authenticated ownership protocol before
  reclaiming an orphan. A PID file alone is insufficient because PIDs are reused.
- [x] Preserve external ownership: reconnect may attach to an external worker,
  but the API must not start, replace, or terminate that caller-owned process.

Acceptance: inject failure while idle, during generation, while waiting for a
function, and around result acknowledgment. The HTTP process remains usable,
recovery is bounded, stale callbacks cannot cross generations, and no tool call
is automatically executed twice. Test SIGKILL/API restart separately from graceful
shutdown. Automatic continuation of an interrupted turn is not assumed.

Slice evidence (2026-09-23, connection ownership): `src/lib.rs` owns the backend
behind a `std::sync::Mutex` never held across backend I/O; exactly one pump task
serves a connection, and `reconnect` awaits the old pump's exit and disconnect
bookkeeping before installing a replacement, so `src/actions.rs` delivery (run
only by that pump, holding the matching client) cannot cross into a new
connection. Tests:
`replacement_backend_fences_stale_function_calls_and_serves_new_work`
(tests/suite/reconnect.rs) replaces a live backend under an outstanding
function waiter and verifies the stale result is rejected with a conflict
while new sessions run on the replacement;
`store_reads_survive_worker_loss_and_reconnect_restores_service`
(tests/suite/worker_loss.rs) SIGKILLs a real worker, verifies saved history
stays readable and mutations return 503 `app-server disconnected`, then
reconnects a replacement worker and continues the saved session with context.
Run via `cargo nextest run -p codex-agents-api` on macOS with a mock provider.

Slice evidence (2026-09-23, worker supervisor): `src/main.rs` replaces fatal
`worker_exit` handling with a supervision loop. On managed-worker exit it reaps
the dead worker (releasing its home lock and socket), respawns with bounded
attempts (`WORKER_RESTART_ATTEMPTS`) and exponential backoff capped at
`WORKER_RESTART_BACKOFF_MAX`, completes the app-server handshake via
`runtime::connect`, and reattaches with `AgentsApi::reconnect`; durable reads
keep serving throughout and the process exits only when restarts are exhausted.
External-socket mode retains no managed identity, so `restart_worker` refuses to
respawn it. `rpc` now maps a transport failure to 503 `app-server disconnected`
(a request racing a crash) and reserves 502 for genuine upstream JSON-RPC
errors. Test: `managed_worker_restarts_after_crash_and_service_continues`
(tests/suite/runtime_cli.rs) SIGKILLs the managed worker mid-session, observes
503 during the gap, and confirms the saved session continues with context and
the CLI stays alive; `api_shutdown_preserves_external_worker` still passes.

Slice evidence (2026-09-24, turn reconciliation): `src/reconcile.rs` runs after
every backend (re)connect (from `AgentsApi::reconnect`, so both fresh startup
and worker restart). It finds turns that `records::disconnected` failed only
because the connection dropped — tagged with the shared
`reconcile::CONNECTION_LOST_CODE` marker — and re-reads authoritative outcomes
via `thread/turns/list`, which serves from persisted rollout history and so
needs no prior resume. A turn the rollout records completed is recovered to
`completed` (interrupted → `cancelled`, failed → `failed` with the real
message); a turn still unfinished keeps its provisional failure, because an
interrupted turn's completion cannot be established. Recovered sessions return
to `idle`. The same pass also reconciles function calls left `unavailable`/
`submitting` by a disconnect: a call whose turn is authoritatively `completed`
is restored to `submitted`, because a turn cannot complete unless its calls
were resolved, so the result delivered even though the acknowledgment was lost;
any other turn status leaves the call unresolved for the caller, so a genuinely
lost pending call still surfaces in `unresolvedActions` and is never claimed as
delivered or re-driven from a saved request id. Determining the markers needed
no new schema, so no migration was added. It is best-effort and never fails a
reconnect. Tests: `reconcile_recovers_completed_turn_lost_to_disconnect`
(tests/suite/reconcile_recovery.rs) completes a turn on a real worker, then
simulates a lost completion notification plus disconnect by marking the public
turn provisionally failed through the approved codex-state sqlite shim, and
after reconnecting a replacement worker on the same home confirms the turn is
restored to `completed` and the session to `idle`;
`reconcile_recovers_submitted_tool_result_lost_to_disconnect`
(tests/suite/reconcile_functions.rs) submits a function result that completes
its turn, reverts the call to `unavailable` via the shim, and after reconnect
confirms the call leaves `unresolvedActions` and an identical resubmit returns
the stored receipt instead of a conflict.

Slice evidence (2026-09-24, API-crash orphan reclaim): an API-process crash
(for example SIGKILL) skips graceful shutdown and `kill_on_drop`, so the managed
worker keeps running against its home while the API-held home lock is released —
a second start would otherwise put two app-servers on one home. `runtime.rs`
now writes an authenticated ownership record (`agents-api-worker.json`: PID plus
process start time) on spawn, and managed startup calls `reclaim_orphan` before
spawning: it re-reads the record, and if a live process still matches both PID
and start time (the start time defeats PID reuse, since a PID file alone is
insufficient) it SIGKILLs that orphan and waits until it no longer runs, then
clears the record; a missing, stale, or reused record signals no process and is
cleared without touching anything. Start time is read per-OS (Linux
`/proc/<pid>/stat`, macOS `proc_pidinfo`); other platforms fall back to the home
lock alone. Externally owned workers are never recorded or reclaimed. This is
separate from worker-crash handling (the supervisor, which reaps its own child).
Test: `orphaned_worker_is_reclaimed_after_api_crash` (tests/suite/runtime_cli.rs)
SIGKILLs the real API binary, confirms its worker is orphaned and alive, then
starts a second API on the same data directory and confirms the orphan is gone,
a fresh worker replaced it, and the API serves. Remaining API-crash gaps:
Windows containment (Job Object / `LockFileEx` / `GetProcessTimes`) and the
sub-second window between child spawn and record write are not yet covered.

## G02 — Durable storage evolution and ownership

**Outcome:** new APIs and recovery logic can evolve persisted data without losing
existing sessions or introducing duplicate writers.

Starting points: `src/store.rs`, `src/records.rs`, and `src/resources.rs`.

- [x] Replace unversioned schema expansion with ordered, transactional migrations.
  Cover upgrades from the current SQLite schema and defaults for old JSON records.
- [ ] Define durable resource identity, timestamps, lifecycle/tombstone state,
  backend-generation bindings, and any principal scope required by the contract.
- [ ] Use transactions for related public records and action state transitions.
  Persist changes before broadcasting their corresponding events.
- [ ] Define ownership for one API data directory, one worker home, and external
  workers. Coordinate this with G01 so orphan handling cannot bypass ownership.
- [ ] Add indexes and stable ordering for the actual list/filter operations.
  Avoid a database migration or distributed worker pool unless its behavior is
  needed; SQLite remains the starting implementation.

Acceptance: old databases upgrade without changing completed history or session
snapshots; interrupted migrations are recoverable; duplicate owners are rejected;
concurrent updates cannot leave contradictory session/turn/action records.

Slice evidence (2026-09-23, migration runner): the scattered
`CREATE TABLE IF NOT EXISTS` startup path is replaced by an sqlx `Migrator`
(`sqlx_macros::migrate!("./migrations")`) run in `Store::open` before any
access; `migrations/0001_initial.sql` holds the baseline schema and uses
`IF NOT EXISTS` so a pre-migration database is adopted in place rather than
failing on existing tables. Each migration runs in its own transaction, so a
partial upgrade resumes from the last committed one. Startup recovery (marking
lost function waiters `unavailable` and interrupted public turns failed) now
runs after migration instead of inside table setup. Bazel embeds
`migrations/**` via crate `compile_data`. Test:
`migrations_record_a_ledger_and_adopt_a_legacy_database` (src/store_tests.rs)
asserts the `_sqlx_migrations` ledger is recorded, then drops it to simulate a
legacy unversioned database and confirms reopening adopts the baseline without
dropping tables or the existing row. Remaining in G02: durable identity /
tombstones / generation bindings, transactional record+event grouping, data
directory / worker ownership, and list/filter indexes.

## G03 — Saved agents and complete configuration

**Outcome:** all inventoried saved-agent operations and configuration semantics
work through the official SDK.

Starting points: `src/contract.rs`, `src/resources.rs`, `src/store.rs`, and
`src/capabilities.rs`. Add focused private modules as the contract module grows.

- [ ] Implement agent listing with the specified cursor, ordering, limits, and
  filters. Validate cursor ownership and invalid/expired cursor behavior.
- [ ] Implement update and delete according to the documented replacement/merge
  and deletion rules. Preserve already-created session snapshots where required.
- [ ] Represent omitted, explicit null, empty collections, and populated values
  distinctly where the contract distinguishes them.
- [ ] Add remaining configuration field families incrementally: model/reasoning
  settings, output settings, tools, and other inventoried agent capabilities.
- [ ] Resolve model-default reset and inheritance behavior explicitly. Do not
  silently substitute inherited Codex defaults for a requested API setting.
- [ ] Persist normalized configuration and apply the same translation at initial
  session creation and cold resume.

Acceptance: SDK create/read/list/update/delete tests; pagination boundaries; invalid
fields; omitted/null/empty variants; saved-agent override combinations; and a test
showing that an existing session's snapshot behaves correctly after agent changes.

## G04 — Session management and input semantics

**Outcome:** session lifecycle operations and input processing match the contract
under normal use, retries, concurrency, and restart.

Starting points: `src/contract.rs`, `src/routes.rs`, and `src/store.rs`.

- [ ] Implement session listing/filtering and the supported update fields. Define
  what may change while a turn is active and apply changes at the correct boundary.
- [ ] Implement deletion as an owned-resource lifecycle operation: stop/admit work
  as specified, terminate streams appropriately, and schedule owned cleanup.
  Never delete a caller's workspace or terminate caller-owned compute.
- [ ] Implement the remaining creation semantics, including optional initial input
  only where allowed by the pinned contract, and remaining input content variants.
- [ ] Add event batches with the documented validation, ordering, and atomicity
  rules. Define behavior for mixed message/cancel/function-result batches.
- [ ] Implement idempotency only for operations that support it. Persist scope,
  key, request fingerprint, outcome, and retention rules; reject conflicting reuse.
- [ ] Separate request deduplication from execution recovery: a crash between
  dispatch and receipt persistence is ambiguous unless reconciliation proves the
  outcome. Do not claim exactly-once execution from an HTTP idempotency table.
- [ ] Replace unnecessary global serialization with per-session coordination where
  safe, preserving the documented policy for simultaneous input to one session.

Acceptance: SDK list/update/delete; empty-initial-input cases if supported; batch
validation; retry/conflict cases; concurrent same-session versus independent-session
requests; deletion during active work; and restart around input acceptance.

## G05 — Events, items, turns, and usage

**Outcome:** clients observe the documented event/content types and can reconstruct
current state from durable records after disconnects.

Starting points: `src/records.rs`, the notification pump in `src/lib.rs`, and
session event routes in `src/contract.rs`.

- [ ] Map remaining app-server notifications to typed public events, including
  incremental text deltas and documented reasoning/tool/environment item families.
- [ ] Keep event IDs, item IDs, turn IDs, output indexes, timestamps, and terminal
  states consistent across streaming and retrieval responses.
- [ ] Specify ordering and terminal-state invariants for create/start/update/end,
  cancellation, provider errors, and backend loss. Avoid duplicate terminal events.
- [ ] Persist authoritative item/turn state before related state-change events.
  Bound in-memory stream queues and transient deltas; follow the contract's
  retention requirements rather than persisting every fragment by default.
- [ ] Preserve live-only stream semantics. Test subscribe/read/merge recovery and
  slow-consumer behavior without inventing a missed-event replay endpoint.
- [ ] Translate usage from authoritative provider/harness accounting, including
  the documented aggregation and unavailable-value behavior. Do not fabricate
  token counts or double-count retries and resumed turns.

Acceptance: validate entire event sequences and final objects through the SDK;
compare streamed text with saved text; cover reconnect races, lagged consumers,
failed/cancelled turns, multiple tool items, and usage across follow-up turns.

## G06 — Function tools and limits

**Outcome:** function definitions, discovery, invocation, and outputs support the
full inventoried contract without unsafe execution retries or unbounded context.

Starting points: `src/actions.rs`, `src/capabilities.rs`, and public input/item types.

- [ ] Implement supported structured/content variants for function outputs and
  errors. Preserve item identity and error semantics through continuation.
- [ ] Replace prototype limits with verified per-field/operation limits. Validate
  serialized byte size and encoding consistently at the HTTP and backend boundary.
- [ ] Define how larger allowed outputs reach Codex safely. Do not simply remove
  the 1,024-byte cap: preserve repository context-size rules and use a supported
  bounded representation or artifact path where the public contract permits it.
  Record unresolved incompatibilities instead of silently truncating content.
- [ ] Implement deferred definitions and tool search/discovery where required.
  Make loaded-tool changes session-scoped and persist the relevant configuration.
- [ ] Extend action transitions for duplicate submissions, simultaneous resolution,
  cancellation, and connection-generation changes, reusing G01/G04 guarantees.

Acceptance: SDK success/error/content variants, boundary-size and Unicode cases,
deferred discovery, conflicting retries, wrong-session IDs, concurrent callbacks,
and backend loss before/after result submission. Additional review is required
for new model-visible context items crossing repository size thresholds.

## G07 — MCP and built-in capability control

**Outcome:** advertised tool capabilities are selected and enforced by the API's
configuration, with verified execution behavior.

- [ ] Inventory each supported MCP transport, authentication mechanism, approval
  mode, resource/tool surface, and built-in capability separately.
- [ ] Translate public MCP configuration into session-scoped server connections.
  Use `codex-mcp/src/mcp_connection_manager.rs` for tool mutation/call behavior
  where applicable; avoid duplicating connection management in the HTTP layer.
- [ ] Wire scoped credentials through G11. Keep secrets out of persisted public
  configuration, API responses, model-visible text, and routine logs.
- [ ] Enforce selection across tools and resources, including changes on cold
  resume. Define the requested behavior for approvals and interactive actions.
- [ ] Add each built-in capability as its own implementation/test slice. Validate
  underlying provider support and represent unsupported features explicitly.
- [ ] Ensure inherited Codex helpers cannot accidentally contradict the advertised
  capability policy. Preserve Codex safety constraints; document and resolve any
  incompatibility rather than bypassing policy for apparent parity.

Acceptance: isolated mock MCP servers, selected/unselected tools and resources,
authentication failures, approval paths, disconnection, configuration changes,
and cold resume. Include concurrent sessions with different capability policies.

## G08 — Remaining harness features

**Outcome:** supported harness features are configurable through the API and have
correct session, event, and ownership semantics.

- [ ] Map documented delegation/multi-agent configuration to existing Codex
  mechanisms. Define parent/child ownership, cancellation, inherited restrictions,
  persisted outcomes, and usage accounting before exposing it.
- [ ] Expose supported compaction controls and observable outcomes. Preserve
  incremental history construction and existing context/cache invariants.
- [ ] Map programmatic tool calling to existing code-mode/tool machinery where
  suitable; define its execution boundary, callback handling, and limits.
- [ ] Add supported skills/plugin selection and configuration. Persist selections
  so restart does not silently change the session's enabled behavior.
- [ ] Keep API adaptation in this crate or an appropriate existing crate. Add to
  `codex-core` only when the missing behavior belongs in the harness itself.

Acceptance: one contract fixture per capability plus combined scenarios: child
cancellation, compaction followed by cold resume, nested function callbacks, and
skills/plugins isolated between sessions. Harness logic changes require the
repository's relevant integration tests in addition to API tests.

## G09 — Environments and executor lifecycle

**Outcome:** environment ownership and lifecycle are explicit and independent of
worker process ownership.

Implement self-hosted attachment first:

- [ ] Inventory connection actions, credentials, statuses, expiry, and reconnection
  behavior. Map them to existing app-server/exec-server protocol capabilities.
- [ ] Persist session-to-environment bindings. Route execution to the attached
  executor; do not assume the HTTP host, app-server, and executor share an OS or
  filesystem.
- [ ] Authenticate attachment and enforce workspace/path boundaries using the
  appropriate URI/path types for remote execution.
- [ ] Handle executor loss independently from model/worker loss. Report command
  outcomes and allow reconnection only as specified; never blindly replay a
  potentially side-effecting command.

Then implement service-managed environments:

- [ ] Select the compute provider and isolation model using explicit requirements:
  supported OS, lifecycle, files, network policy, credentials, quotas, and cost.
  Provider choice is an open design decision, not a prerequisite for local API work.
- [ ] Add a provider adapter for allocation, readiness, setup, executor connection,
  stop, and cleanup. Introduce only the operations needed by the selected provider.
- [ ] Persist allocation ownership before progressing lifecycle transitions so API
  restart can reconcile partially created or partially deleted resources.
- [ ] Implement supported setup, expiry, network, and resource settings. Tie cleanup
  to ownership and make retries safe without assuming provider calls are atomic.

Acceptance: attach an actual executor, execute in the expected filesystem, verify
remote path handling, lose/reconnect the executor during a command, and delete a
session without stopping caller-owned compute. For managed compute, additionally
verify setup failure, API crash during allocation, expiry, and leaked-resource
cleanup. Deployment/billing changes need separate concrete authorization.

## G10 — Files and artifacts

**Outcome:** clients can move and retrieve documented file/artifact content with
correct ownership, integrity, and lifecycle behavior.

- [ ] Inventory upload/download/publication operations, content types, size limits,
  identifier formats, access semantics, and retention rules.
- [ ] Add durable metadata and an appropriate storage implementation. Separate
  public IDs from internal paths and bind access to the required resource scope.
- [ ] Implement bounded streaming upload/download with integrity checks and
  cleanup of incomplete transfers; avoid buffering entire large files in memory.
- [ ] Connect transfer into/out of environments through the executor/provider
  boundary from G09, including foreign-OS paths.
- [ ] Implement publication and deletion only with the documented visibility and
  retention semantics. Do not expose arbitrary host paths or credentials.

Acceptance: upload/use/retrieve a file in an actual environment; verify content
integrity, interrupted transfer recovery, limits, missing/deleted files, traversal
attempts, scope isolation, and artifact behavior after environment termination.

## G11 — Credentials, webhooks, and observability

**Outcome:** remaining service integrations follow verified public contracts and
have durable, testable failure handling.

Credential/vault work:

- [ ] Finish the resource and scope inventory before selecting the storage model.
- [ ] Implement required CRUD/binding operations with encryption and explicit
  access checks. Keep public metadata separate from secret material.
- [ ] Define rotation/revocation behavior for already-running sessions, MCP
  connections, and environments; test the actual propagation path.

Webhook work:

- [ ] Verify subscription/event/signature/retry semantics from the pinned contract.
- [ ] Create an outbox transactionally with the state changes that cause delivery.
- [ ] Implement bounded dispatch, documented signing, retries, and deduplication
  identifiers. A lost acknowledgment must not be presented as exactly-once delivery.
- [ ] Persist delivery state and redact credentials/content appropriately in logs.

Observability work:

- [ ] Add request/session/turn/worker correlation and lifecycle metrics sufficient
  to diagnose startup, reconnect, provider latency, tool failures, and cleanup.
- [ ] Expose only documented public usage/observability fields; keep operational
  worker health and restart counters distinct from SDK response schemas.

Acceptance: scoped credential access, rotation/revocation, secret redaction,
webhook signature checks, delivery retries across restart, duplicate handling,
and traceable failures across API, worker, provider, and environment boundaries.

## G12 — Final acceptance and operational readiness

**Outcome:** parity claims are backed by repeatable tests and a closed inventory.

- [ ] Map every G00 inventory entry to a behavior test and evidence. Verify schema
  variants, exact errors, pagination, timestamps, transitions, and size boundaries.
- [ ] Expand official SDK scenarios to every implemented resource/capability.
  Include raw HTTP fixtures where SDK parsing hides a wire-contract discrepancy.
- [ ] Run a controlled real-provider suite with explicitly configured credentials
  and budget. Separate provider-dependent failures from service contract failures.
- [ ] Run actual supported-platform worker/executor scenarios, including Windows
  shutdown and cross-OS execution. Do not count a macOS pass as Windows evidence.
- [ ] Exercise concurrent sessions, slow subscribers, process crashes, network
  interruptions, expired credentials, partial persistence, and resource cleanup.
- [ ] Verify upgrade compatibility for saved configuration/history and any retained
  prototype routes; declare intentional breaking changes explicitly.
- [ ] Define operational deployment requirements separately: authentication scope,
  network exposure, secret storage, backup/restore, admission limits, resource
  quotas, health checks, and graceful deployment. Add distributed worker routing
  only if deployment requirements call for it; it is not proof of public parity.

Completion gate: all inventoried target behaviors have passed their required
acceptance tests, remaining unsupported capabilities are zero within the declared
parity target, and real-provider/platform gaps are closed. Mock-only or partial
SDK success is not full parity.

## Immediate next implementation slices

1. **G00 recovery subset:** pin failure/status and pending-function semantics
   needed for supervision; continue the broader inventory alongside later work.
2. **G01 connection ownership (complete 2026-09-23):** HTTP/store state stays
   alive when the backend disconnects; generations are fenced; disconnect and
   reconnect tests cover replacement and worker SIGKILL.
3. **G01 worker replacement (complete 2026-09-23):** the CLI supervisor restarts
   owned workers with bounded backoff and initialization and reattaches via
   `AgentsApi::reconnect`; externally owned workers are left untouched.
4. **G02 migrations (complete 2026-09-23):** ordered, transactional sqlx
   migrations with in-place adoption of the pre-migration database.
5. **G01 turn + function reconciliation (complete 2026-09-24):** after
   reconnect, turns and tool-result deliveries failed only by a connection loss
   are recovered from authoritative rollout history; interrupted work is not
   falsely recovered and lost waiters are not re-driven. No schema was needed.
6. **G01 API-crash orphan reclaim (complete 2026-09-24, Unix):** an orphaned
   worker is authenticated by PID plus start time and reclaimed before a
   replacement spawns. Windows containment remains a follow-up.
7. **G03/G04 resource completion:** complete saved-agent and session CRUD in
   independently reviewable changes, then input/idempotency semantics.
8. **G05/G06 richer execution contract:** finish deltas/items/usage and function
   content/limits before layering on additional capabilities and environments.

## How to maintain this plan

For each completed slice, record its goal ID, changed files, acceptance tests,
actual command/result, provider/platform, and remaining limitations. Update the
inventory and README when behavior changes. Check off a goal only when all of its
required acceptance criteria are met; distinguish implemented from verified.

Use `just test -p codex-agents-api` for this crate. Run the pinned SDK test with
its configured Python environment when its contract is affected. Build required
first-party test binaries first. Follow repository requirements for scoped lint,
formatting, dependency lock updates, schema generation, and additional harness or
protocol tests. Do not expand to a full workspace suite without the required
approval, and do not rerun tests after the final `fix`/`fmt` steps.
