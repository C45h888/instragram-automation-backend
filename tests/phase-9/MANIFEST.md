# Phase 9 — Runtime Verification & Constitutional Validation Conversion

This is the canonical manifest for phase 9. It is the source of truth
that other tools (runner, IDE) read to find what phase 9 contains.

## Tier 1 — Webhook (critical)

| File | Purpose |
|------|---------|
| `webhook/runtime-webhook-ingress.test.mjs` | 6 canonical fixtures delivered through real runtime |
| `webhook/runtime-webhook-captured.test.mjs` | `captured/` fixtures (skipped until phase 10) |
| `webhook/runtime-webhook-retry.test.mjs` | 429 → runtime re-queues |
| `webhook/runtime-webhook-duplicate.test.mjs` | dedup-kernel absorbs |
| `webhook/runtime-webhook-schema-drift.test.mjs` | unknown shape rejected pre-governance |
| `webhook/runtime-webhook-signature.test.mjs` | bad sig → ingress rejects |

## Tier 2 — Graph (support)

| File | Purpose |
|------|---------|
| `graph/runtime-graph-insights.test.mjs` | insights worker |
| `graph/runtime-graph-publishing.test.mjs` | publishing worker |
| `graph/runtime-graph-capability.test.mjs` | capability worker |
| `graph/runtime-graph-recovery.test.mjs` | recovery worker |
| `graph/runtime-graph-reconciliation.test.mjs` | reconciliation worker |

## Cross-kernel (20 pairs)

Mirrors phase-8 layout: 5 sources × 4 sinks = 20 directed files in
`cross-kernel/`.

## Ownership

| File | Purpose |
|------|---------|
| `ownership/ownership-trace-<fixture>.test.mjs` | 6 files, one per canonical fixture |

## Sovereignty

| File | Purpose |
|------|---------|
| `sovereignty/kernel-sovereignty-<kernel>.test.mjs` | 5 files, one per kernel |
| `sovereignty/kernel-sovereignty-acquisition-coupling.test.mjs` | negative: confirm coupling detector works |

## Replay

| File | Purpose |
|------|---------|
| `replay/lineage-replay.test.mjs` | replay observation log → reconstructed state → diff |

## Drift

| File | Purpose |
|------|---------|
| `drift/authority-drift.test.mjs` | no kernel writes outside its domain |
| `drift/semantic-drift.test.mjs` | public_signal vs internal sentinel |
| `drift/ownership-drift.test.mjs` | no foreign-table mutations |
| `drift/cross-kernel-contamination.test.mjs` | sink never sees source internals |
| `drift/governance-leakage.test.mjs` | workers never call escalate() |
| `drift/worker-autonomy.test.mjs` | workers never import scheduler/governance/fsm |
| `drift/fsm-ownership.test.mjs` | fsm transitions emit lineage |

## Integration

| File | Purpose |
|------|---------|
| `integration/phase-9-runtime-composition.test.mjs` | full chain across all domains |
| `integration/phase-9-multi-tick-survival.test.mjs` | 25/100/500/1000 ticks |
| `integration/phase-9-long-running-integrity.test.mjs` | non-increasing violations over time |
| `integration/phase-9-scenario-message-thread.test.mjs` | end-to-end scenario |
