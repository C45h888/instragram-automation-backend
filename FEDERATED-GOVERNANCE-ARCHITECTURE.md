# Federated Governance Architecture — Contextual Reference

> This document captures the architectural philosophy, phase progression, and audit state of the federated governance runtime. It is **contextual only** — it describes what the system is, what it aims to become, and where the gaps are. It does not prescribe specific file changes or implementation steps, as execution approach may evolve during implementation.

---

## Table of Contents

1. [Architectural Philosophy](#architectural-philosophy)
2. [Phase 0 — Pre-Architecture State](#phase-0--pre-architecture-state)
3. [Phase 1 — Federated Governance Decomposition](#phase-1--federated-governance-decomposition)
4. [Phase 2 — Lineage as Canonical Truth](#phase-2--lineage-as-canonical-truth)
5. [Phase 3 — Reconciliation Engine](#phase-3--reconciliation-engine)
6. [Phase 4 — Dedup Lineage Elevation](#phase-4--dedup-lineage-elevation)
7. [Phase 5 — Remaining Domain FSMs](#phase-5--remaining-domain-fsms)
8. [Phase 6 — Constitutional Policy Broadcasts](#phase-6--constitutional-policy-broadcasts)
9. [Complete Audit Findings Summary](#complete-audit-findings-summary)

---

## Architectural Philosophy

This repository is a **deterministic orchestration runtime with bounded cognition layered on top**. It is not a traditional automation dashboard or SaaS application. The platform operates through explicit hierarchical runtime ownership:

- **Constitutional Kernel** — sole legality authority, validates transitions, registers domains, maintains runtime equilibrium
- **Domain FSMs** — bounded constitutional domains (acquisition, publishing, scheduling)
- **Orchestrators** — constitutional membranes that route governance authority downward and observations upward without interpreting runtime meaning
- **Substrates** — semantically blind mechanical infrastructure (retry, dedup, metrics, sync, mutation, normalization, persistence, quota, realtime, telemetry)
- **Execution Bridge** — error classification and observation emission layer

The system prioritizes:
- Deterministic orchestration over emergent behavior
- Explicit contracts over implicit assumptions
- Reconciliation over reactive synchronization
- Bounded authority over fragmented runtime ownership
- Persistence-backed canonical state over transient execution-local assumptions
- Observable execution lineage over opaque orchestration behavior

Earlier generations of the system suffered from orchestration ambiguity, synchronization debt, recursive acquisition loops, and probabilistic runtime topology. The current architecture intentionally rejects fragmented authority and emergent orchestration semantics.

---

## Phase 0 — Pre-Architecture State

### What Existed Before Phase 1

The original architecture was a **monolithic HSM (Hierarchical State Machine)** housed in a single file. All governance concerns — global lifecycle, domain transitions, retry logic, circuit breakers, auth strike tracking, event routing, lineage recording, and action subscription — existed horizontally across procedural services and procedural loops.

This created distributed semantic ownership where multiple layers partially governed the system simultaneously. Realtime listeners could independently wake execution flows. Synchronization logic leaked into backend infrastructure. Orchestration assumptions accumulated over time in every subsystem.

### Legacy Problems

| Problem | Root Cause |
|---------|------------|
| Recursive acquisition loops | Multiple layers could independently wake execution |
| Orchestration drift | Synchronization debt from fragmented authority |
| Probabilistic topology | Retry semantics unpredictable |
| Opaque state mutations | No observable lineage, no causal debugging |
| No crash recovery | Fresh boot on restart, no state reconstruction |

---

## Phase 1 — Federated Governance Decomposition

### Phase 1 Intent

Phase 1 establishes the constitutional foundation of the runtime by decomposing governance into bounded federated domains while preserving a single canonical authority center. The system transitions from procedural orchestration into hierarchical runtime governance. Governance now exists as an explicit constitutional layer rather than an emergent side effect of distributed coordination logic.

### Architectural Transformation

```
BEFORE: Monolithic HSM → all logic in one file, horizontal procedural ownership

AFTER:
  Constitutional Kernel (sole legality authority)
      │
      ├── acquisition-fsm   (bounded domain)
      ├── publishing-fsm     (bounded domain)
      └── scheduling-fsm    (bounded domain)
      │
      Orchestrators (constitutional membranes — routing only)
      │
      Substrates (semantically blind infrastructure)
```

### Key Architectural Outcomes

1. **Authority centralization** — Constitutional kernel becomes the sole legality authority
2. **Jurisdiction isolation** — Domain FSMs own bounded lifecycle transitions within their domain
3. **Deterministic routing** — Orchestrators route without interpreting runtime meaning
4. **Vertical isolation** — Governance owns legality, orchestrators coordinate routing, execution layers perform bounded work, substrates remain semantically blind

### What Phase 1 Built

- `constitutional-kernel.js` — Global lifecycle (BOOTING/HEALTHY/DEGRADED/RECOVERY/HALTED), DOMAIN_EVENT_MAP routing, general guards, domain registration, validateDomainTransition(), action subscription, watchdog
- `lineage-ledger.js` — Append-only event ledger
- `acquisition-fsm.js` — IDLE/ACQUIRING with retry/auth/circuit-breaker logic
- `publishing-fsm.js` — IDLE/BUFFERING/EVALUATING/EMITTING pipeline
- `scheduling-fsm.js` — CADENCE_TICK sequencing with metrics evaluation
- Orchestrator wiring updated to register domains before membrane setup

### Phase 1 Status

Phase 1 is structurally complete (~85%). The federation architecture is sound. Governance has been decomposed into constitutional kernel + domain FSMs. Orchestrators are correctly wired.

**Known gaps (detailed in audit findings):**
- Blocked transitions silently dropped from lineage (no audit trail for rejected governance decisions)
- Mutations in buildActions occur before constitutional validation — no rollback mechanism
- Domain lineage recorded before constitutional validation — inconsistent dual audit trails
- Scheduling FSM has no enforcement guards
- Circuit breaker cooldown not enforced by any substrate

---

## Phase 2 — Lineage as Canonical Truth

### Phase 2 Intent

Phase 2 transforms the runtime from a mutable-state architecture into an event-sourced constitutional system. Currently, governance state is primary and lineage is a secondary audit trail appended after transitions occur. If the process crashes, the runtime loses constitutional continuity and reboots with no ability to reconstruct prior state.

Phase 2 inverts this completely. **Lineage becomes the canonical source of truth. Runtime state becomes a materialized projection derived from lineage replay.** The lineage ledger evolves from a logging mechanism into constitutional memory.

### Architectural Transformation

```
CURRENT (State-Primary):
  Guard → Validate → State Mutates → Lineage Appended (shadow log)

TARGET (Lineage-Primary):
  Guard → Validate → Lineage Commits → State Materialized from Ledger
```

### What Changes

Every governance transition, legality rejection, policy escalation, degradation event, retry decision, and lifecycle mutation becomes append-only constitutional history. Runtime state is no longer trusted independently — it is reconstructed deterministically from lineage replay.

This enables:
- Deterministic crash recovery
- Historical replay and causal debugging
- Governance introspection ("why is state X?")
- Single authoritative audit trail (no more dual shadow logs)

### Preconditions

- Lineage must be made durable (Redis persistence) before reconciliation can work
- Write order must be inverted throughout (lineage first, state second)
- Materialization engine must be added to lineage ledger
- Rehydration on boot must replace hardcoded default initialization

---

## Phase 3 — Reconciliation Engine

### Phase 3 Intent

Phase 3 introduces constitutional equilibrium and runtime self-correction. Even with deterministic governance and canonical lineage, distributed runtimes inevitably drift. Infrastructure state diverges from governance assumptions. Queues partially commit. Workers fail silently. Redis expires keys. External APIs desynchronize.

The reconciliation engine continuously compares constitutional expectations against observed substrate state and runtime reality. **Governance no longer merely assumes correctness; it actively verifies equilibrium.** The runtime becomes a feedback-regulated system rather than a static orchestration engine.

### What Reconciliation Does

| Function | Description |
|----------|-------------|
| State comparison | Compare observed substrate state against governance lineage |
| Drift detection | Identify mismatches before systemic failure cascades |
| Lineage recovery | Replay lineage after crashes to restore domain continuity |
| Intent vs lineage check | Detect duplicate execution vs replay collisions |

### Preconditions

- Phase 2 must be complete (lineage must be durable and materializable)
- Substrates must expose observable state for comparison
- Reconciliation loop must run continuously, not just on staleness

### Architectural Outcome

The runtime operates as a self-correcting constitutional organism. Governance detects divergence before it cascades. Recovery is deterministic, not probabilistic.

---

## Phase 4 — Dedup Lineage Elevation

### Phase 4 Intent

Phase 4 upgrades deduplication from infrastructure coordination into constitutional identity management. Currently, deduplication is **resource-centric and transport-oriented** — identity is tied to `(accountId, actionType, resourceId)`. This cannot distinguish replay epochs, recovered intents, or lineage continuity.

Two independent intents targeting the same resource collide semantically even if they belong to separate constitutional execution histories.

### What Changes

Deduplication becomes **lineage-sensitive rather than transport-sensitive**. Intent IDs and lineage epochs become part of runtime identity. The runtime gains the ability to distinguish:
- Original execution
- Replayed execution
- Recovered execution
- Duplicated execution

### Architectural Outcome

- Replay systems can safely reconstruct execution history without triggering duplicate mutations
- Reconciliation engines can compare intent continuity across replay epochs
- Deduplication becomes an extension of constitutional memory rather than a transport optimization

---

## Phase 5 — Remaining Domain FSMs

### Phase 5 Intent

Phase 5 completes constitutional federation across the runtime. Currently, acquisition, publishing, and scheduling are constitutional domains. Engagement, deduplication, and reconciliation still exist procedurally outside governance federation — residual procedural islands inside an otherwise constitutional runtime.

This phase fully constitutionalizes the remaining domains. Every major subsystem becomes governed state rather than procedural execution logic.

### Current Domain Inventory

| Domain | Status |
|--------|--------|
| `acquisition` | ✅ Constitutional domain FSM |
| `publishing` | ✅ Constitutional domain FSM |
| `scheduling` | ✅ Constitutional domain FSM |
| `engagement` | ❌ Procedural — no FSM |
| `deduplication` | ❌ Procedural — no FSM |
| `reconciliation` | ❌ Procedural — no FSM |

### Note on "Engagement"

The term "engagement" in acquisition FSM comments refers to circuit-breaker engagement (retry behavior), not a separate domain. There is no engagement domain FSM currently.

### Architectural Outcome

No major execution domain remains semantically autonomous. Governance federation becomes horizontally complete while constitutional authority remains vertically centralized.

---

## Phase 6 — Constitutional Policy Broadcasts

### Phase 6 Intent

Phase 6 introduces adaptive constitutional synchronization. Currently, governance is **reactive and pull-based**. Domains discover degraded constitutional state indirectly when transitions fail or guards reject execution.

This phase introduces **proactive policy broadcasting**. The constitutional kernel no longer merely validates transitions — it actively synchronizes runtime posture across the entire federation. When governance enters DEGRADED, RECOVERY, or HALTED modes, domains receive proactive constitutional broadcasts informing them of new runtime policy conditions.

### What Changes

| Before | After |
|--------|-------|
| Domains discover DEGRADED via failed validate() calls | Domains receive POLICY_BROADCAST proactively |
| CK emits LOG_DEGRADED only | CK emits policy payload with mode/restrictions |
| Reactive legality | Proactive constitutional coordination |
| Isolated reactive FSMs | Constitutionally-aware domain participants |

### Architectural Outcome

The runtime becomes an adaptive cybernetic system capable of systemic self-regulation. Acquisition domains may proactively reduce cadence during degradation. Publishing domains may enter conservation states during recovery. Governance evolves from reactive legality into proactive constitutional coordination.

---

## Complete Audit Findings Summary

### Phase 1 Audit — Overall Status: ~85% Complete

**What works:**
- Constitutional kernel as sole legality authority ✅
- DOMAIN_EVENT_MAP routing (15 events → 3 domains) ✅
- General guards (HALTED lockdown, RECOVERY blocking) ✅
- Domain FSM registration before membrane wiring ✅
- No references to old monolithic HSM ✅
- Proxy methods for domain FSM state queries ✅
- All 10 substrates compliant with membrane architecture ✅

**Critical gaps (Phase 1 P0):**
1. Blocked transitions (guard rejection) do NOT write to lineage — rejected governance decisions silently dropped
2. buildActions mutates state before ctx.validate() commits — no rollback if constitutional rejects
3. Domain lineage recorded before constitutional validation — dual audit trails that can diverge
4. result.allowed === false path never calls lineageLedger.record()

**Should fix (Phase 1 P1):**
- Dead code: unreachable branches in EXECUTION_OBSERVATION.target (acquisition-fsm)
- Circuit breaker cooldown never enforced by any substrate
- Scheduling FSM has zero enforcement guards (all always-allow)
- EMISSION_OBSERVATION collapses error and success to IDLE — no residual error state

**Should fix (Phase 1 P3):**
- Lineage ledger unbounded growth (no TTL or rotation)

### Phase 2 Audit — Status: ~5% (Not Started)

**Current state:** State is primary everywhere. Lineage is a shadow log appended after transitions. No rehydration. No materialization. No crash recovery.

**What needs to exist:**
- Lineage ledger with materialization engine (compute current state from recorded entries)
- Rehydration on boot (replay lineage to reconstruct state, not hardcoded defaults)
- Write order inversion (lineage commits first, state materializes second)
- Single authoritative audit trail (no dual domain-local shadow arrays)
- Durable lineage persistence (Redis-backed, not in-memory only)

### Phase 3 Audit — Status: ~5% (Not Started)

**Current state:** No reconciliation infrastructure. Watchdog is purely temporal (TTL only, no state comparison). No drift detection. No intent vs lineage duplicate detection.

**What needs to exist:**
- Reconciliation loop (continuous state comparison vs lineage)
- Drift detectors (substrate state vs governance assumptions)
- State recovery from lineage after crash
- Intent continuity checking across replay epochs

### Phase 4 Audit — Status: ~15% (Not Started)

**Current state:** Dedup is resource-centric using `(accountId, actionType, resourceId)` triple. No intentId tracking. No lineage awareness.

**What needs to exist:**
- IntentId in dedup key (to distinguish replay epochs)
- Lineage-aware deduplication keys
- Replay collision detection at dedup layer

### Phase 5 Audit — Status: ~50% (Partial)

**Current state:** 3 domain FSMs exist (acquisition, publishing, scheduling). 3 missing (engagement, deduplication, reconciliation).

**No orphaned logic** — old monolithic HSM fully replaced. No skeleton or thin re-export files.

**What needs to exist:** Engagement, deduplication, and reconciliation domain FSMs when those domains require formal governance.

### Phase 6 Audit — Status: ~0% (Not Started)

**Current state:** Purely reactive pull-based policy discovery. No POLICY_BROADCAST event type. No subscribePolicy pattern. No ctx.getPolicy() in domain context.

**What needs to exist:**
- POLICY_BROADCAST event type
- ctx.getPolicy() in domain FSM context
- Proactive push of constitutional state to domains on DEGRADED/RECOVERY/HALTED transitions
- Domain FSM handlers for policy updates

---

## Phase Progression Map

```
Phase 0: Monolithic HSM (horizontal procedural ownership)
    ↓
Phase 1: Constitutional Kernel + Domain FSMs (federated authority)
    ↓
Phase 2: Lineage as canonical truth (event-sourced, crash-recoverable)
    ↓
Phase 3: Reconciliation engine (cybernetic self-correction)
    ↓
Phase 4: Dedup lineage elevation (intent-aware constitutional identity)
    ↓
Phase 5: Complete domain FSM federation (horizontal completeness)
    ↓
Phase 6: Constitutional policy broadcasts (proactive adaptive synchronization)
```

---

## Key Architectural Principles

1. **Explicit authority boundaries** — No subsystem reclaims orchestration authority independently
2. **Deterministic runtime behavior** — No emergent orchestration, no autonomous wake behavior
3. **Reconciliation-driven synchronization** — No timer-driven sync, no graph-triggered acquisition
4. **Canonical persistence** — Truth lives in persistence, not transient execution flows
5. **Observable lineage** — Every state mutation traceable to causal event chain
6. **Bounded domain ownership** — Each FSM owns its domain's lifecycle absolutely

---

*Contextual reference for coding agents operating in this repository. Not a change log or implementation prescription. Phase 1 structural implementation complete. Phases 2–6 pending.*
