# Agents API contract inventory

Updated: 2026-09-22.

This is the pinned G00 parity denominator for this service. The machine-readable
inventory is [CONTRACT_INVENTORY.json](CONTRACT_INVENTORY.json). It records every
HTTP operation exposed by the Agents API resource tree in `openai==3.17.0`, the
trace-export route documented outside that SDK surface, and the Files and Skills
operations required by documented Agents API workflows.

## Baseline and scope

- North star: [Agents](https://developers.openai.com/api/docs/guides/agents).
- Contract guides: the Agents API pages in the
  [official guide index](https://developers.openai.com/api/docs/llms.txt).
- SDK baseline: official Python SDK `openai==3.17.0`.
- Review date: 2026-09-22.
- Agents requests use `OpenAI-Beta: agents=v1` unless the operation is a listed
  Files or Skills dependency with its own contract.

The operation denominator is 59:

- 43 Agents API operations, including raw-HTTP trace export.
- 5 Files API operations needed for `file_id` environment inputs and cleanup.
- 11 Skills API operations needed for hosted `skill_reference` inputs and
  version selection.

The SDK resource surface contains 58 of those operations. Trace export is
documented as `GET /v1/agents/sessions/{session_id}/traces` but has no Python
SDK 3.17.0 resource method, so it stays in the inventory with raw-HTTP
acceptance coverage required.

Provider guides for Blaxel, Cloudflare, Daytona, DigitalOcean, E2B, Modal, OCI,
Runloop, and Vercel were reviewed as integrations with the common self-hosted
environment contract. They do not add service endpoints to this denominator.

## Status accounting

Statuses apply to the entire inventoried operation, including its request and
response variants and documented errors:

- `missing`: no target-contract implementation.
- `partial`: a useful subset works, but one or more inventoried variants or
  failure behaviors are absent.
- `implemented`: the code path is complete, but required acceptance evidence is
  not yet available.
- `verified`: the complete operation passed its required behavioral contract
  tests in the recorded environment.

The current count is 9 partial and 50 missing. No operation is called
implemented or verified yet because the existing tests cover only constrained
variants. A partial operation does not count as completed parity.

## What is pinned

The JSON inventory contains:

- Method, path, SDK resource/method, source URL, implementation location, test,
  evidence type, status, and remaining gap for every operation.
- Exact discriminated-union members for environments, input events, content,
  tools, items, required actions, live events, and webhooks.
- Session, turn, subagent, environment, item, and function-call status values.
- Cross-cutting semantics for omission/null updates, pagination, idempotency,
  recovery, environments, tools, files, credentials, webhooks, usage, tracing,
  and provider interoperability.
- The compatibility-baseline update procedure.

For SDK-backed operations, `sdk_resource` plus `sdk_method` identifies the exact
pinned request signature and response type; transport-only SDK escape hatches
(`extra_headers`, `extra_query`, `extra_body`, and `timeout`) are not product
parameters. The drift check resolves those methods and their generated response
models directly from 3.17.0. Cross-operation constraints and errors live in the
behavior rows because the public Agents guides do not currently publish a
complete per-operation error matrix. Missing error detail is therefore an
explicit coverage gap, not an inferred contract.

Small SDK-shaped request and response examples live in
[`tests/fixtures/sdk_contract.json`](tests/fixtures/sdk_contract.json). Shared
Python helpers in [`tests/sdk_helpers.py`](tests/sdk_helpers.py) keep SDK version
and fixture loading consistent between acceptance scripts.

## Verification

`tests/sdk_inventory.py` validates that:

- The installed SDK is exactly `openai==3.17.0`.
- Every SDK-backed inventory entry resolves to the expected generated resource
  method, HTTP verb, path, and Agents beta header where applicable.
- No HTTP method in a tracked SDK resource class is omitted from the inventory.
- The representative agent and session response fixtures parse through the
  pinned SDK models.

Run it directly:

```sh
python tests/sdk_inventory.py CONTRACT_INVENTORY.json
```

The ignored Rust integration test runs the same check when
`CODEX_AGENTS_API_SDK_PYTHON` points to the pinned Python environment.

## Maintaining the baseline

Do not edit the denominator implicitly while implementing another goal. A
contract update must change the pinned SDK/docs review metadata, run the SDK
inventory diff, record added or removed behavior with a source, update fixtures
and acceptance tests, and explain whether the change is compatible. Mock,
real-provider, and platform evidence remain separate fields; one must not be
substituted for another.
