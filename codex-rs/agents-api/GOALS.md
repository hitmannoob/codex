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
- Session creation/retrieval/list/update/delete, text and data-URL image input,
  steering, cancellation, and function success/error submission on the
  supported official-style session routes. Input events accept validated ordered
  batches and a session-scoped `Idempotency-Key`. Deletion refuses running work and removes the
  owned Codex thread through a durable cleanup queue.
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
- Data-directory ownership: `Store::open` holds an advisory lock on the API data
  directory for its lifetime, rejecting a second API process in any worker mode.
- Atomic public records with post-commit events: each notification handler writes
  its related records in one transaction and broadcasts events only after commit,
  so a mid-handler failure cannot leave contradictory records or premature events.

Latest verification (2026-09-27): 24/24 tests passed, none skipped, using
`CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 CODEX_AGENTS_API_SDK_PYTHON=/private/tmp/codex-g03-sdk/bin/python just test -p codex-agents-api --run-ignored all`.
This includes strict `openai==3.17.0` inventory/lifecycle checks, saved-agent
configuration coverage, provider request captures before/after cold resume,
and existing recovery/ownership tests. `just test -p codex-features` passed
42/42, and the focused core integration test
`default_service_tier_override_is_omitted_from_http_turn` passed. The complete
core crate run had failures in connector, MCP, and executor tests; those failures
were not resolved in this G03 change. Execution was on macOS with mock providers;
Windows and real-provider acceptance remain unverified. Scoped
`just fix -p codex-features -p codex-core -p codex-agents-api`, `just fmt`, and
`git diff --check` passed.

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
| G03 | Complete saved agents and configuration (complete locally) | G00, G02 | List, update/delete, field families |
| G04 | Complete sessions and input semantics (complete locally) | G00, G01, G02 | List, update/delete, input variants, idempotency |
| G05 | Complete events, items, turns, and usage (complete locally) | G00, G02 | Text deltas, item families, transitions, usage |
| G06 | Complete function-tool behavior (complete locally except deferred functions) | G00, G04, G05 | Content/limits, deferred tools, failure cases |
| G07 | Complete MCP and built-in controls (HTTP MCP complete locally) | G00, G03, G05 | MCP, built-in capabilities, isolation |
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
- [x] Use transactions for related public records and action state transitions.
  Persist changes before broadcasting their corresponding events.
- [x] Define ownership for one API data directory, one worker home, and external
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
dropping tables or the existing row.

Slice evidence (2026-09-24, data-directory ownership): `Store::open` now claims
an advisory lock (`agents-api.lock`) on the API data directory before touching
the database and holds it for the store's lifetime, so a second API process on
the same directory is rejected with "data directory is already in use by another
agents-api process". This closes the external-worker-mode double-writer gap the
worker-home lock (G01) does not cover, and complements it: managed startup is
guarded by both, and the data lock releases when the owning store drops. Test:
`data_directory_lock_rejects_a_second_owner` (src/store_tests.rs) opens a store,
confirms a second open on the same directory fails, and confirms a new open
succeeds once the first is dropped.

Slice evidence (2026-09-24, transactional records + events): the public-record
notification handler in `src/records.rs` previously wrote each related record
under its own autocommit and emitted events interleaved with those writes, so a
mid-handler failure could leave a turn record without its session-status change
and events could precede a later write. Each handler branch (turn transition,
requires-action, item + tool-output) now performs all of its related writes in
one transaction and broadcasts the buffered events only after the commit. The
write helpers `save`/`save_session` became executor-generic (pool or
transaction) and `publish_item` takes the transaction and returns its events in
order; the pure `transition` helper computes a session-status change and its
event without I/O. Callers that do a single write (`contract` session creation,
`session_status` for `deliver`/`reconcile`) still pass the pool, so recovery and
delivery paths are unchanged. Emit order is preserved and verified by the
existing exact-sequence event tests (`api.rs`, `capabilities.rs`), which pass
unchanged. No schema change.

Remaining in G02, deferred to their consumers to avoid dead schema (the plan's
own guidance against a migration whose behavior is not yet needed):

- Durable identity / timestamps / tombstones / generation columns — timestamps
  already live in the record JSON; tombstones gain a consumer with G04 session/
  agent delete, and creation-ordered columns with the G03/G04 list endpoints.
  These land as migration `0002` alongside those endpoints.
- List/filter indexes and stable ordering — the list operations that exist
  today (turns, items) are already covered by `public_records_page`; agent and
  session list indexes land with the G03/G04 list endpoints that query them.

## G03 — Saved agents and complete configuration (complete locally 2026-09-27)

**Outcome:** all inventoried saved-agent operations and configuration semantics
work through the official SDK.

Implemented (2026-09-27): saved-agent CRUD and stable creation-order pagination,
name/metadata constraints, atomic replacement with conflict detection, complete
pinned-SDK configuration field families, and independent session snapshots.

- [x] Implement local agent listing, exclusive cursors, ordering, and 1..100 limits.
  Unknown/deleted cursors are rejected; project-scoped ownership remains G11.
- [x] Implement update/delete and preserve already-created session snapshots.
- [x] Distinguish omitted, null, empty collections, and populated values; supplied
  configuration objects replace previous objects.
- [x] Store/retrieve reasoning, text, tier, multi-agent and all SDK tool variants.
  G06–G08 execution capabilities remain explicitly gated.
- [x] Reset effort to the model default, summary to disabled, verbosity to medium,
  and structured output to ordinary text; override inherited worker settings.
- [x] Apply saved snapshots at initial creation and cold resume, including after
  deletion of the saved agent during active work.
- [x] Forward explicit `service_tier: default` through a session-scoped Codex
  feature; ordinary Codex sessions still omit that tier. Provider/model setting
  compatibility and prototype limits remain tracked separately.

Acceptance coverage: `tests/sdk_agents.py` (called by the strict pinned SDK
lifecycle test), `store_tests.rs`, and
`tests/suite/configuration.rs`. The runtime test inspects actual mock-provider
requests for effort, summary, JSON schema, verbosity and
flex/priority/default/auto tier settings before and after worker restart, with
hostile inherited defaults.
The migration/store tests cover old database adoption and stable list ordering.
Concurrent session coverage exposed a deferred SQLite read-to-write upgrade
race in item publication; reserving the writer with `BEGIN IMMEDIATE` before
reading fixes it. The SDK cancellation mock now matches the latest user message
so cancelled text in history cannot stall the post-restart request.
The session feature `features.explicit_default_service_tier` defaults off in
Codex and is enabled only by this facade's configuration overrides. The core
integration test for ordinary Codex requests still verifies omitted default.
Real-provider and cross-platform acceptance remain G12; project ownership
remains G11 and extended capability execution remains G06–G08.

Review staging (the combined working diff exceeds 800 lines): land saved-agent
CRUD/storage/ordering and its migration first; then configuration normalization
in `configuration.rs` / `agent_tools.rs` and its resource/call-site changes;
then execution translation and the SDK/provider acceptance coverage. These stages
have dependencies in that order; avoid landing schema types without their callers.

## G04 — Session management and input semantics (complete locally 2026-09-28)

**Outcome:** session lifecycle operations and input processing match the contract
under normal use, retries, concurrency, and restart.

Starting points: `src/contract.rs`, `src/routes.rs`, and `src/store.rs`.

- [x] Implement session listing/filtering and the supported update fields. Define
  what may change while a turn is active and apply changes at the correct boundary.
- [x] Implement deletion as an owned-resource lifecycle operation: stop/admit work
  as specified, terminate streams appropriately, and schedule owned cleanup.
  Never delete a caller's workspace or terminate caller-owned compute.
  Self-hosted environment ownership is covered when G09 adds attachment.
- [x] Implement the remaining creation semantics, including optional initial input
  only where allowed by the pinned contract, and remaining input content variants.
  The pinned SDK requires initial input for environment `none`; optional input
  arrives with the environments that allow it (G09). Codex rejects remote image
  URLs, so only `data:image/` images are accepted.
- [x] Add event batches with the documented validation, ordering, and atomicity
  rules. Define behavior for mixed message/cancel/function-result batches.
  The public docs define no batch rules, so the rules below are local decisions.
- [x] Implement idempotency only for operations that support it. Persist scope,
  key, request fingerprint, outcome, and retention rules; reject conflicting reuse.
- [x] Separate request deduplication from execution recovery: a crash between
  dispatch and receipt persistence is ambiguous unless reconciliation proves the
  outcome. Do not claim exactly-once execution from an HTTP idempotency table.
- [x] Replace unnecessary global serialization with per-session coordination where
  safe, preserving the documented policy for simultaneous input to one session.

Acceptance: SDK list/update/delete; empty-initial-input cases if supported; batch
validation; retry/conflict cases; concurrent same-session versus independent-session
requests; deletion during active work; and restart around input acceptance.

Slice evidence (2026-09-28, session management): `src/sessions.rs` adds list,
update, and delete on `/v1/agents/sessions`. Migration `0003` gives public
sessions a creation sequence that is never reused (the agents pattern) and adds
a `session_cleanup` queue. Listing uses exclusive cursors and rejects unknown,
deleted, or filter-mismatched ones. Updates replace metadata and change only
model, reasoning effort (keeping the snapshot's summary), and service tier.
Each turn reads the snapshot at `turn/start`, so a running turn keeps its
settings and the next turn uses the update. Delete holds input admission and
returns 409 while the public status is `in_progress`/`requires_action` or the
worker reports the thread `active`. It then removes the public records in one
transaction, queues the thread, ends that session's streams, and calls
`thread/delete` in the background. Queue rows are removed only after the worker
confirms, and every reconnect retries them. The G02 tombstone item is covered
by this queue: deleted public data is removed outright and only the worker
thread outlives the request. To keep concurrent status changes from overwriting
update fields, session transitions now write only status/error/last_active_at
inside write transactions. Notification handlers resolve their session inside
one `BEGIN IMMEDIATE` transaction, so a concurrent deletion is skipped cleanly
instead of failing the backend pump. Agents and sessions share one metadata
validator. Tests: `tests/sdk_sessions.py` (from the strict SDK lifecycle)
covers ordering, pagination, `agent_id` filtering, cursor errors, update/reset
and rejection cases, 409 during pending-function and running turns, and 404s
after deletion. `session_updates_apply_next_turn_and_deletion_waits_then_cleans_up`
(tests/suite/sessions.rs) checks provider requests for the updated
model/effort/tier on the next turn, 409 then cancel then delete, stream
termination, removal of only the deleted session's rollout, and a queued cleanup
finishing after an API restart. `migrations_record_a_ledger_and_adopt_a_legacy_database`
now also adopts a legacy `public_sessions` table and assigns its creation order.
Run on macOS with a mock provider.

Slice evidence (2026-09-28, input semantics): `src/input.rs` replaces the
single-event handler. The pinned SDK is the only contract evidence:
`events.create` takes an `Idempotency-Key`; `sessions.stream` sends one per
input and per tool result and retries a result only on 400 with code
`invalid_request_error` and message `Unknown pending tool call: <call_id>`. The
app-server emits a function call's `item/started` before its request, so that
registration race is real and now gets exactly that error. The public guides
define neither batch nor idempotency semantics, so these are local decisions:
- **Batches:** 1–32 events; tool results plus at most one message or one
  cancel; each call resolved once.
- **Validation:** parsing, limits, call state, and backend connectivity are
  checked for the whole batch before anything runs. Events then run in order
  and stop at the first failure.
- **Messages:** one user message of `input_text` (≤8,192 bytes total) and
  `data:image/` `input_image` parts. They map to Codex `text`/`image` input and
  are saved as public `input_image` items. Codex rejects remote image URLs, so
  they are rejected up front.
- **Keys:** migration `0004` stores the request, state, and outcome per
  (session, key) for 24 hours. A key is claimed in a `BEGIN IMMEDIATE`
  transaction. It is released if nothing was dispatched, and stored as
  `completed` for replay otherwise. A 503/500 during dispatch leaves it
  `unknown`, and startup turns every `pending` key `unknown`. Unknown keys
  return 409 and never re-run: deduplication is kept separate from execution
  recovery.

Tests: `tests/sdk_input.py` (strict SDK lifecycle) runs `sessions.stream` with a
tool handler, checks the unknown-call error with the SDK's own
`is_pending_call_race`, replays a keyed text-and-image message without a second
turn, checks the saved image content, and rejects each invalid batch and key
shape without running anything. The lifecycle also asserts that a provider
request carried `input_image`. It now runs on a multi-threaded runtime: Codex
image preparation calls `block_in_place`, which panics on the current-thread
test runtime. The CLI binary already runs multi-threaded.
`keyed_batches_run_once_and_interrupted_keys_report_unknown`
(tests/suite/input.rs) covers:
- a `[tool_result, cancel]` batch that delivers the result and then cancels the
  resumed turn;
- a keyed message replayed with exactly one provider request;
- reuse of a key with different input returning 400;
- a key left `pending` by a simulated API crash reporting 409 unknown after
  restart, while a new key runs.

Codex runs dynamic tools one at a time, so a batch resolving two pending calls
cannot occur against this worker and is not exercised. Run on macOS with a mock
provider.

Slice evidence (2026-09-28, per-session coordination): `src/gates.rs` replaces
the global input semaphore with one async mutex per session. An entry lives only
while a caller holds or awaits it. Turn start and session deletion take their
own session's gate, so independent sessions are admitted concurrently while one
session's bootstrap, submissions, and deletion check never interleave. This
matches the pinned SDK's single-writer guidance: simultaneous inputs to one
session are serialized and steer the active turn.

A new concurrency test found a bug that predates this change. Every input called
`thread/resume`, and resuming a thread whose first turn had just started could
fail with 502 while its rollout file was still empty. Each backend connection
now records the threads it has started or resumed and resumes each thread once.
Per-turn model, effort, summary, and tier already travel with `turn/start`, and
a replacement connection starts empty, so cold resume still happens.
Tests:
- `one_session_waits_while_independent_sessions_proceed` (src/gates_tests.rs)
  checks admission and pruning.
- `concurrent_first_inputs_share_one_thread` (tests/suite/input.rs) races four
  first inputs on an empty session and requires every returned turn to complete
  in the session's single thread. It passed five repeated runs.
- The session-update test still sees the updated model/effort/tier on later
  turns.

G04 acceptance, all on macOS with a mock provider:
- SDK list/update/delete: covered.
- Empty initial input: not allowed for environment `none`.
- Batch validation, and key retry/conflict cases: covered.
- Concurrent same-session and independent-session admission: covered.
- Deletion during active work: covered.
- Restart around input acceptance: covered by a simulated crash state.

Real-provider and Windows acceptance remain G12.

## G05 — Events, items, turns, and usage (complete locally 2026-09-28)

**Outcome:** clients observe the documented event/content types and can reconstruct
current state from durable records after disconnects.

Starting points: `src/records.rs`, the notification pump in `src/lib.rs`, and
session event routes in `src/contract.rs`.

- [x] Map remaining app-server notifications to typed public events, including
  incremental text deltas and documented reasoning/tool/environment item families.
  Tool, environment, and subagent families need their capabilities (G07–G09).
- [x] Keep event IDs, item IDs, turn IDs, output indexes, timestamps, and terminal
  states consistent across streaming and retrieval responses.
- [x] Specify ordering and terminal-state invariants for create/start/update/end,
  cancellation, provider errors, and backend loss. Avoid duplicate terminal events.
- [x] Persist authoritative item/turn state before related state-change events.
  Bound in-memory stream queues and transient deltas; follow the contract's
  retention requirements rather than persisting every fragment by default.
- [x] Preserve live-only stream semantics. Test subscribe/read/merge recovery and
  slow-consumer behavior without inventing a missed-event replay endpoint.
- [x] Translate usage from authoritative provider/harness accounting, including
  the documented aggregation and unavailable-value behavior. Do not fabricate
  token counts or double-count retries and resumed turns.

Acceptance: validate entire event sequences and final objects through the SDK;
compare streamed text with saved text; cover reconnect races, lagged consumers,
failed/cancelled turns, multiple tool items, and usage across follow-up turns.

Evidence (2026-09-28): `src/streaming.rs` translates `item/agentMessage/delta`,
`item/reasoning/summaryTextDelta`, and `item/reasoning/summaryPartAdded` into
the pinned SDK's delta and part events. It also emits `error` for non-retried
provider errors. None of these events are persisted.
- **Framing:** a small per-connection map holds each streaming item's session,
  turn, output index, and announced parts. Entries are removed when the item
  finishes, its turn ends, or the connection drops.
- **Ordering:** `records.rs` publishes each item's `item.added` and `item.done`
  around the streaming events, so every item follows the SDK's
  added → part → delta → done → part done → item done order. Parts are announced
  even when no delta arrived.
- **Terminal invariants:** a terminal turn closes any item still `in_progress`
  as `incomplete`, with one `item.done`, before `turn.{status}`. Failed turns
  and `error` events use the SDK's documented codes, mapped from Codex's error
  kinds. Reconciliation uses the same mapping.
- **Buffer:** the public event buffer grew from 128 to 1,024 so token-rate
  deltas don't close streams. A consumer lagging past that is still closed
  rather than skipped.

Usage (`src/usage.rs`, migration `0005`) comes from `thread/tokenUsage/updated`. The raw
per-response notification is opt-in only at `thread/start`, so it would be lost
after a cold resume.
- Each session stores the last cumulative total seen, and each update adds only
  its increase to the turn. Repeated totals, and the total Codex replays on
  resume, add nothing.
- An update for a turn that is no longer running only moves the stored total.
- Without a usable stored total, only that response's `last` usage counts.
- Turn and session usage are `null` until a response reports usage.

Tests:
- `tests/sdk_events.py` (strict SDK lifecycle) validates every streamed event
  against `AgentSessionEvent` and checks each item's exact event order. It also
  checks that deltas rebuild the saved message and summary, that `item.done`
  payloads equal the saved items, and output indexes and exact turn/session
  usage.
- `usage_sums_each_response_once_across_turns_and_restart` covers per-turn and
  session sums across a cold resume, and `null` for a response without usage.
- `failed_turn_reports_its_error_and_closes_open_items` checks the exact
  sequence item.added → part → delta → `error` → item.done(`incomplete`) →
  turn.failed → idle, with `context_length_exceeded` on both the event and the
  saved turn.
- `reconnected_stream_merges_with_saved_items` follows the documented
  subscribe/read/merge recovery across a function call and requires the merged
  state to equal the final saved items.
- Unit tests in `src/contract_tests.rs` show that a lagged subscriber is closed,
  and that a stream carries only its own session and ends on deletion.

Not covered:
- Cancellation leaving a partially streamed item: the mock provider cannot
  stall mid-stream. The failure path exercises the same sweep.
- Several tool items in one turn, because Codex runs dynamic tools one at a
  time.
- Real-provider accounting (G12).

## G06 — Function tools and limits (complete locally except deferred functions)

**Outcome:** function definitions, discovery, invocation, and outputs support the
full inventoried contract without unsafe execution retries or unbounded context.

Starting points: `src/actions.rs`, `src/capabilities.rs`, and public input/item types.

- [x] Implement supported structured/content variants for function outputs and
  errors. Preserve item identity and error semantics through continuation.
- [x] Replace prototype limits with verified per-field/operation limits. Validate
  serialized byte size and encoding consistently at the HTTP and backend boundary.
- [x] Define how larger allowed outputs reach Codex safely. Do not simply remove
  the 1,024-byte cap: preserve repository context-size rules and use a supported
  bounded representation or artifact path where the public contract permits it.
  Record unresolved incompatibilities instead of silently truncating content.
- [ ] Implement deferred definitions and tool search/discovery where required.
  Make loaded-tool changes session-scoped and persist the relevant configuration.
- [x] Extend action transitions for duplicate submissions, simultaneous resolution,
  cancellation, and connection-generation changes, reusing G01/G04 guarantees.

Acceptance: SDK success/error/content variants, boundary-size and Unicode cases,
deferred discovery, conflicting retries, wrong-session IDs, concurrent callbacks,
and backend loss before/after result submission. Additional review is required
for new model-visible context items crossing repository size thresholds.

Slice evidence (2026-09-28, function results): `actions::result_output`
validates official results. Success takes a string or `input_text` /
`input_image` parts, and a JSON object must be serialized first, as the guide
says. Failure takes `error`, and mixing the two returns 400. Images must be
`data:image/` URLs, since Codex rejects remote ones. Parts map one to one to
Codex `inputText`/`inputImage` content items. The saved `function_call_output`
uses the stored submission, so parts and strings round-trip exactly; it falls
back to Codex's text only when nothing was stored.

Limits: the public docs publish no result limit. Codex silently truncates tool
output at 10,000 bytes, or 10,000 tokens for catalogued models. Result text is
therefore capped at 10,000 UTF-8 bytes, which no truncation policy shortens, so
the model sees exactly what is saved. Larger outputs are rejected, not
truncated; no artifact path exists before G10. **Review flag:** this raises a
model-visible item from about 256 tokens (1,024 bytes) to as many as roughly
2,500–10,000 tokens. That crosses the repository's 1k-token review threshold
while staying within its 10k hard cap. The prototype route keeps its 1,024-byte
limit.

Transitions: validation failures leave the call pending. Racing different
results resolve once: the winner gets 202, the loser 409, and the winner's
identical retry gets its receipt. A misrouted session gets the pending-call
race error, and a result after cancellation gets 409. Backend loss before and
after submission is covered by G01's reconcile tests.

Test: `function_results_keep_their_content_limits_and_semantics`
(tests/suite/functions.rs) checks that parts reach the provider as parts, the
10,000-byte boundary with two-byte characters (accepted at the limit, rejected
one character over), invalid shapes, a failed result with a Unicode error, a
misrouted result, the race, and a late result after cancel.

Deferred functions remain open by decision (2026-09-28). Codex supports
deferred dynamic tools through tool search, but only when the model's
`supports_search_tool` is set. The app-server's `model/list` does not report
that flag, so a deferred function on an unsupported model would be silently
unreachable. Execution therefore stays rejected with an explicit error. Lifting
it needs an additive `supportsSearchTool` field on `model/list` (a shared-crate
protocol change with schema regeneration) or equivalent worker-reported
support.

## G07 — MCP and built-in capability control (complete locally for HTTP MCP)

**Outcome:** advertised tool capabilities are selected and enforced by the API's
configuration, with verified execution behavior.

- [x] Inventory each supported MCP transport, authentication mechanism, approval
  mode, resource/tool surface, and built-in capability separately.
- [x] Translate public MCP configuration into session-scoped server connections.
  Use `codex-mcp/src/mcp_connection_manager.rs` for tool mutation/call behavior
  where applicable; avoid duplicating connection management in the HTTP layer.
- [ ] Wire scoped credentials through G11. Keep secrets out of persisted public
  configuration, API responses, model-visible text, and routine logs.
- [x] Enforce selection across tools and resources, including changes on cold
  resume. Define the requested behavior for approvals and interactive actions.
- [ ] Add each built-in capability as its own implementation/test slice. Validate
  underlying provider support and represent unsupported features explicitly.
- [x] Ensure inherited Codex helpers cannot accidentally contradict the advertised
  capability policy. Preserve Codex safety constraints; document and resolve any
  incompatibility rather than bypassing policy for apparent parity.

Acceptance: isolated mock MCP servers, selected/unselected tools and resources,
authentication failures, approval paths, disconnection, configuration changes,
and cold resume. Include concurrent sessions with different capability policies.

Evidence (2026-09-28, HTTP MCP):

Inventory. The pinned SDK has an inline transport that can carry
`authorization` and stdio `env`, and a saved transport whose `headers` are
documented as non-secret. The guide lists three placements:
- HTTP with `connection_origin: service`: runs in the service, no environment.
- HTTP with `connection_origin: environment`: needs an environment.
- stdio: needs an environment.

Credentials come inline or from vaults; `required` makes initialization
mandatory; `allowed_tools` filters tools. The public schema has no approval
field and no resource selection.

Implementation. `src/mcp.rs` executes service-origin HTTP servers. Everything
else is rejected with an explicit reason:
- stdio or environment origin: G09.
- `credential_id`, an `Authorization` header, or inline `authorization`: G11.
- `request_metadata`.

Each server becomes a session-scoped Codex `mcp_servers` entry through thread
configuration, so Codex's own MCP connection manager connects and calls it. The
entry sets only `url`, `http_headers`, `enabled_tools`, `required`, `enabled`,
and auto-approval, so no public field can reach worker-local options such as
header helper commands. Worker-configured servers stay disabled (the existing
policy), and a public label that shadows one is rejected. Plugins, apps, and
agent spawning remain off.

Egress policy (decision 2026-09-28). URLs must be https and resolve only to
public addresses; loopback, RFC 1918, link-local/metadata, shared, multicast,
IPv6 ULA/link-local, and IPv4-mapped forms are refused. An operator allowlist
(`--allow-mcp-host`, `AgentsApi::allow_mcp_hosts`) can permit specific hosts.
The check runs before a session is created and before each turn. DNS can still
change between the check and Codex's connection.

Behavior:
- **Approvals:** the caller's `allowed_tools` is the approval, so calls run
  without prompts. Interactive MCP requests stay rejected by the pump.
- **Required servers:** a required server that cannot initialize makes Codex
  refuse the thread, so `turns::fail_unstarted_turn` records the documented
  failed turn (`connection_failed`, a service-assigned turn ID, then `error`,
  `turn.failed`, and idle) instead of a 502 carrying Codex internals.
- **Items:** calls map to `mcp_call` items.

Test: `public_mcp_servers_are_scoped_filtered_and_egress_checked`
(tests/suite/mcp.rs) with a mock HTTP MCP server. It checks that nine
blocked or unsupported configurations fail before any session exists; that an
allowed server exposes `lookup` but not `secret`, receives its tenant header,
and yields the expected `mcp_call`; that a concurrent session without MCP never
sees the server; that cold resume restores the server with the same filter;
and the required-server failed turn. Unit test:
`only_public_addresses_are_reachable_without_approval` (src/mcp_tests.rs).

Remaining:
- MCP credentials: `[ ]` wire scoped credentials through G11.
- Built-in web search (decision 2026-09-28): stays rejected, like deferred
  functions. The hosted tool is only sent when the provider reports
  web-search capability, and some models use Codex's standalone `web.run`.
  The worker reports neither, so enabling it could leave the model silently
  without search. The `[ ]` built-in item stays open until the worker reports
  that support.
- Programmatic tool calling belongs to G08.

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
7. **G02 data-directory ownership + transactional records (complete 2026-09-24):**
   `Store::open` holds a data-directory lock for its lifetime, and each public-
   record notification handler now writes atomically and emits after commit.
   Durable-identity columns and list indexes stay deferred until their G03/G04
   consumers exist (they land as migration `0002` with those endpoints), to
   avoid dead schema.
8. **G03/G04 resource completion:** complete saved-agent and session CRUD in
   independently reviewable changes, then input/idempotency semantics.
9. **G05/G06 richer execution contract:** finish deltas/items/usage and function
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
