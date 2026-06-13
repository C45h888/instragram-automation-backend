# Phase 8 — Cross-Kernel Communication Suite

The 8th phase of the constitutional runtime validation framework.
Phase 8 sits **above** phase 7 as an additive validation layer. It
exercises the kernelized architecture end-to-end through:

  - **Real workers** (the actual worker modules in
    `publishing-kernel/workers/`, `acquisition-kernel/workers/`,
    `graph-capability-kernel/workers/`, etc.)
  - **Simulated Graph** (the existing `tests/phase-7/runtime/graph-simulator.js`
    on port 9100)
  - **Simulated Webhooks** (the new `tests/phase-8/runtime/webhook-simulator.js`
    on port 9200)

The only thing simulated is the Meta environment. Everything else
is the production system. The runtime must prove it operates
correctly under realistic ingress, not just under mocks.

## What phase 8 validates

| Domain | Asserts |
|---|---|
| Constitutional Path A | Webhook → ingress → parser → governance → FSM → worker → state |
| Constitutional Path B | Graph → worker → governance → state |
| Webhook ingress | 6 canonical fixtures, 7 chaos scenarios |
| Cross-kernel | 20 directed wires, sentinel isolation, foreign-write rejection |
| Worker subordination | Workers never import scheduler/governance/FSM |
| Retry cadence | Survives 7 chaos scenarios with constitutional path intact |
| Architectural drift | Authority / semantic / ownership / FSM / governance leakage detection |
| Multi-tick survival | 50 / 250 / 1000 ticks depending on tier |

## Directory layout

```
tests/phase-8/
├── docker-compose.phase-8.yml         # extensions to docker-compose.test.yml
├── phase-8-runner.sh                  # standalone runner
├── vitest.config.js
├── MANIFEST.js                        # test inventory
├── diagram.html                       # dark SVG architecture diagram
├── QUICKSTART.md                      # this file
│
├── runtime/
│   ├── webhook-simulator.js           # 9200 delivery / 9201 control
│   ├── webhook-fixtures.js            # client SDK
│   ├── constitutional-recorder.js     # event_id → chain recorder
│   ├── cross-kernel-probe.js          # sentinel-isolation harness
│   ├── ingress-adapter.js             # Meta-shape → IngressEvent
│   ├── report-writer.js               # per-test JSON writer
│   ├── render-diagram.js              # generates diagram.html
│   └── index.js                       # public surface
│
├── constitutional-flow/   (4 tests)
├── webhook/               (4 tests)
├── cross-kernel/          (20 tests — one per directed pair)
├── integration/           (3 tests)
└── reports/               (per-test JSON; vitest-results.json)
```

## Cadence tiers

| Tier    | Ticks | When to run             |
|---------|-------|-------------------------|
| short   | 50    | every commit            |
| medium  | 250   | CI / Phase 8 (default)  |
| long    | 1000  | manual / pre-release    |

## Usage

```bash
# All suites, medium tier
./tests/phase-8/phase-8-runner.sh

# Short tier (faster, less coverage)
./tests/phase-8/phase-8-runner.sh --tier=short

# Long tier (1000 ticks — manual)
./tests/phase-8/phase-8-runner.sh --tier=long

# Run one suite
./tests/phase-8/phase-8-runner.sh --suite=constitutional
./tests/phase-8/phase-8-runner.sh --suite=webhook
./tests/phase-8/phase-8-runner.sh --suite=cross-kernel
./tests/phase-8/phase-8-runner.sh --suite=integration

# Re-render the architecture diagram only
./tests/phase-8/phase-8-runner.sh --diagram-only

# Keep the docker stack up after the run
./tests/phase-8/phase-8-runner.sh --keep-up
```

## Reports

Each test writes a JSON file to:

```
tests/phase-8/reports/<suite>/<test-name>.<run-id>.json
```

Plus a top-level `tests/phase-8/reports/vitest-results.json` for
the vitest run.

Per-test JSON shape:

```json
{
  "run_id": "2026-06-13T...",
  "test_name": "capability-to-acquisition",
  "suite": "cross-kernel",
  "timestamp": "2026-06-13T...",
  "started_at": 1700000000000,
  "finished_at": 1700000000123,
  "status": "pass",
  "vitest_assertion_count": 4,
  "constitutional_summary": [{ "event_id": "...", "ok": true, "violations": [] }],
  "drift_findings": [],
  "event_ids": ["pkt_..."],
  "timeline_sample": [...],
  "extras": { "sentinel_isolation": { "ok": true, ... } },
  "error": null,
  "duration_ms": 123
}
```

To infer system state from a run, walk the `event_ids` → `constitutional_summary`
→ `drift_findings` graph. Any non-empty `drift_findings` indicates an
architectural violation that requires investigation before merge.

## Docker

Phase 8 extends the existing `docker-compose.test.yml` with one
new service:

  - `webhook-simulator` (node:22-alpine, ports 9200/9201, healthcheck
    on `/health`)

The `test-runner` service gains four env vars:

  - `WEBHOOK_SIMULATOR_HOST=webhook-simulator`
  - `WEBHOOK_SIMULATOR_PORT=9200`
  - `WEBHOOK_CONTROL_PORT=9201`
  - `PHASE8_REPORT_DIR=/app/tests/phase-8/reports`

The webhook-simulator image is mounted read-only at
`/sim/server.js` and serves the 6 canonical Meta-shaped fixtures
(message-created, comment-created, mention-created, story-reply,
media-update, conversation-update) on its delivery port.

## Architectural rules enforced

- Workers MUST NOT import scheduler / governance / FSM modules.
- Workers MUST NOT mutate foreign kernel state.
- Governance MUST NOT call transport primitives (axios / fetch / redis).
- Every webhook event_id MUST traverse the full chain
  (ingress → governance → fsm → worker → mutation) in order.
- Cross-kernel packets MUST NOT leak internal sentinels across the
  boundary; the sink receives only the constitutional public_signal.

## Failure handling

When a test fails, the runner prints the first 2KB of the latest
report.json. For full inspection:

```bash
cat tests/phase-8/reports/<suite>/<test-name>.<run-id>.json | jq .
```

The `error.message` and `error.stack` fields pinpoint the failing
assertion. The `drift_findings` array lists every architectural
violation detected during the run, even ones that did not directly
cause the failure — treat any non-empty `drift_findings` as a
must-investigate before merge.
