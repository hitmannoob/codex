# Agents API functional parity target

Decision: reproduce the documented Agents API behavior on our infrastructure,
using this Codex checkout as the execution engine. The existing HTTP facade is
an implementation prototype, not the target public contract.

Baseline reviewed: 2026-09-22, official Python SDK `openai==3.17.0` and the
HTML Agents API reference. The text/function session slice now has an executable
SDK contract check (`tests/sdk_lifecycle.py`) with strict response validation.
This is a version-pinned acceptance baseline, not a complete schema inventory.

Detailed implementation steps, dependencies, and acceptance gates are maintained
in [GOALS.md](GOALS.md).

## Definition of parity

- Match public operations, request/response schemas, event types, status
  transitions, pagination, configuration semantics, and documented failure behavior.
- Run supported official SDK examples against our service by changing the base
  URL and credentials. SDK compatibility is an acceptance target, not a current claim.
- Own harness startup, worker supervision, session routing, and persistent state.
- Supply our own implementation for managed compute and external service
  integrations. Preserve the public contract where compatibility is intended;
  explicitly track any provider-specific feature that is not implemented.
- Mark each operation implemented only after a behavioral contract test passes.
  Mock-provider tests and real-provider acceptance are separate evidence.
- Do not infer private OpenAI implementation details or promise stronger recovery
  guarantees than the documented contract.

## Initial gap inventory

| Area | Documented target | Current implementation / gap |
| --- | --- | --- |
| Saved agents | Create, retrieve, list, update, delete; full configuration | Create/read support SDK-shaped function configuration under the beta header; list/update/delete and remaining configuration still missing |
| Configuration | Inline agents or saved IDs; session overrides; omitted/null/object replacement semantics; allowed session setting updates | Inline or saved IDs with snapshot overrides for the supported fields; broader configuration and updates missing |
| Sessions | Create with optional streaming/initial input as permitted; retrieve/list/update/delete; runtime status and required actions | Official create/retrieve routes, initial input/streaming and normalized lifecycle; list/update/delete missing |
| Input | Message, cancel, and tool-result events through the session events endpoint | Unified events endpoint implemented for one text message, cancel, or text function result/error; batching and idempotency missing |
| Turns and items | List/retrieve turn outcomes; saved item history with pagination and turn filtering | Durable normalized item and turn records with after/order/limit and turn filtering; remaining item variants missing |
| Streaming | Agent/session/turn/item events with stable IDs and text updates | Normalized lifecycle/item/completed-text events; text deltas and remaining event variants missing |
| Functions | Tagged function tools, supported output content, separate error field, deferred loading and tool search | Tagged functions and SDK result/error flow; structured content, deferred tools and official size limits missing |
| MCP and built-ins | API-selected tools/transports/controls; capability-specific behavior | Server-side MCP references and filters; built-ins partly inherited |
| Harness features | Configurable multi-agent execution, context compaction, programmatic tool calling, skills/plugins | Codex has underlying machinery; API integration and parity tests missing |
| Environments | None, service-hosted, self-hosted executor; connection actions and lifecycle | None or read-only local workspace |
| Files/artifacts | Documented upload, publication, retrieval and cleanup behavior | No public implementation |
| Webhooks, credentials, usage | Documented webhook, vault and observability operations | No corresponding public implementation; detailed inventory still required |
| Service operation | Service owns the harness and saved progress | API-owned local worker startup, initialization deadline and shutdown; optional external worker; automatic restart and crash reconciliation still missing |

## Implementation sequence

1. **Freeze the public contract and build its test harness.** Inventory each
   operation from the reference or published SDK types, including experimental
   headers, payload variants, status/error shapes, limits and pagination. Record
   evidence and gaps per operation. Keep current routes usable during migration.
2. **Implement the session contract with no environment.** Saved-agent operations,
   inline configuration, snapshots and overrides; session operations; input events;
   normalized events; saved items and turn retrieval; function result/error flow.
   Acceptance: official SDK lifecycle and function examples run against our URL.
3. **Make the runtime service-owned.** Start and supervise Codex workers, separate
   HTTP lifecycle from execution, reconcile backend state, and support retrieving
   pending actions after a client reconnect. Test failures at each actual process
   boundary; do not mistake persisted request IDs for restorable execution.
4. **Complete capabilities.** Map documented built-in and MCP controls, deferred
   tools, output settings, delegation, programmatic tool calling and skills/plugins.
   Test both advertised capabilities and execution, including session isolation.
5. **Implement environments and artifacts.** Self-hosted executor attachment and
   connection actions, then our managed sandbox provider, setup/files/network
   settings and artifacts. Define cleanup ownership for each mode.
6. **Finish the remaining service APIs and acceptance suite.** Webhooks, vaults,
   usage/observability, provider-backed capabilities and real-provider tests.

These are reviewable milestones, not exclusions from the full parity objective.
Do not call the service functionally equivalent while inventory rows remain open.

## Recovery contract corrections

- Official event streams do not replay missed events. Recovery opens a new
  stream, retrieves current session/items, and merges buffered updates by item ID.
- Pending functions are discovered through current `required_actions`, not merely
  a function-call item in history. Return saved results using the same turn/call IDs.
- Application client restart, API worker failure, harness failure, and environment
  loss are different cases and require separate tests.
- Self-hosted environment loss can fail a tool without failing its entire turn.
  The documented API does not automatically restart a killed command or guarantee
  pending-input recovery after a process crash. Later input may request reconnection.
- Deleting a self-hosted session does not stop caller-owned compute. Model service
  ownership must remain distinct from environment ownership.

## Sources reviewed

- [Overview](https://developers.openai.com/api/docs/guides/agents-api/overview)
- [Architecture](https://developers.openai.com/api/docs/guides/agents-api/architecture)
- [Configuration](https://developers.openai.com/api/docs/guides/agents-api/configuration)
- [Run sessions](https://developers.openai.com/api/docs/guides/agents-api/sessions)
- [Manage sessions](https://developers.openai.com/api/docs/guides/agents-api/sessions/manage)
- [Events and items](https://developers.openai.com/api/docs/guides/agents-api/sessions/events)
- [Functions](https://developers.openai.com/api/docs/guides/agents-api/tools/functions)
- [Sandbox lifecycle](https://developers.openai.com/api/docs/guides/agents-api/environments/lifecycle)

## Review boundaries for the session slice

The implementation spans two dependent review units: request/configuration
translation (`contract.rs`, shared agent route selection), followed by persisted
public records and normalized events (`records.rs`, action/worker hooks). The
second unit needs the first to associate public sessions with Codex threads;
review the HTTP/SDK integration fixtures with that second unit. Neither unit
changes codex-core, provider policy, process ownership, or environment provisioning.

Reference: [session creation](https://developers.openai.com/api/reference/resources/beta/subresources/agents/subresources/sessions/methods/create).
The local SDK test does not establish real-provider execution or full API parity.

Verification for this slice: all five `codex-agents-api` tests passed with
`--run-ignored all`, including strict `openai==3.17.0` SDK validation. The SDK
exercise covers saved agents, session snapshots, function success/error, client
reconnect, item pagination, steering/cancellation, and follow-up after restarting
the API and in-process harness. The provider was mocked; remote-socket and
real-provider acceptance remain unverified.
