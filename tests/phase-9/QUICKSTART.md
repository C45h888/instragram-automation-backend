# Phase 9 — Runtime Verification & Constitutional Validation Conversion

Tier 1 (webhook-driven) + Tier 2 (graph-driven) + cross-kernel + ownership + sovereignty + replay + drift + cadence.

## Run

```bash
# Short tier (default, fast smoke)
./tests/phase-9/phase-9-runner.sh

# Other tiers
./tests/phase-9/phase-9-runner.sh --tier=medium
./tests/phase-9/phase-9-runner.sh --tier=long
./tests/phase-9/phase-9-runner.sh --tier=epic
```

## Suites

- `tests/phase-9/webhook/` — Tier 1 (canonical fixtures, chaos variants)
- `tests/phase-9/graph/` — Tier 2 (worker survival)
- `tests/phase-9/cross-kernel/` — 20 directed pairs
- `tests/phase-9/ownership/` — owner of every link in the chain
- `tests/phase-9/sovereignty/` — per-kernel isolation
- `tests/phase-9/replay/` — causality leak detection
- `tests/phase-9/drift/` — 7 drift classes
- `tests/phase-9/integration/` — composition, multi-tick, long-running integrity, scenario

## Artifacts

Per-run output lands in `tests/phase-9/reports/<run-id>/`:
- `lineage-observation.json` — passive bus subscriber output
- `lineage-snapshot.json` — derived recorder shape
- `ownership-trace.json` — owner of every link
- `drift-findings.json` — drift detector output
- `replay-delta.json` — replay vs actual
- `<suite>__<test>.summary.json` — per-test observation-only summary

## What is NOT in this layer

- The phase-8 `constitutional-recorder.mjs` write API. Phase 9 only OBSERVES.
- Test fabrication. There is no `recorder.ingress/governance/fsm/worker/mutation()` to call.
- Meta emulation. The graph-emulator is a worker-validation tool, not a Meta simulator.

## Captured fixtures

`tests/phase-9/fixtures/webhooks/captured/` is reserved for real Meta
payloads from phase 10 (VPS). Until populated, `runtime-webhook-captured.test.mjs`
is `it.skip` with a phase-10 TODO.
