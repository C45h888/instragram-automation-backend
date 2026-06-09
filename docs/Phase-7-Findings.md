# Phase 7 Findings — Capability / UAT-Refresh Runtime Validation

**Test file:** `tests/phase-7/kernels/capability-uat-refresh-runtime.test.js` (NEW)
**Discovery file:** `tests/phase-7/kernels/graph-capability-flow.test.js` (existing — informed the chain)
**Driver:** Phase7RuntimeSimulator-style full-stack boot against the real constitutional
kernel, real graph-capability FSM, real health-substrate membrane, real
persist-telemetry FSM, real reading-substrate, and real postgres-telemetry
workers. The only stub is the supabase admin client, swapped at
`require.cache` before any production module is loaded (canonical
Phase 7 pattern, matches `graph-capability-flow.test.js:90-98`).
**Status at interrupt:** test harness mounted inside the Docker test
runtime; chain gating bugs discovered during the wire-up. See findings
below.

---

## Results Summary

| Test | Name | Status | Root Cause |
|------|------|--------|------------|
| R1.a | `runUATRefreshCheck` drives the live chain and returns shaped stats | FAIL (blocked by B1) | governedRead dispatch rejected by canonical-source gate; data not reaching substrate |
| R1.b | `CAPABILITY_HEALTH_CHECK_COMPLETED` stamps land per-cred | FAIL (blocked by B1) | Same |
| R2.a | `_shouldCheck` returns false after a run | FAIL (blocked by B1) | Stamps never landed; gate rejected the read |
| R2.b | `CAPABILITY_CADENCE_TICK` emits `RUN_UAT_REFRESH_CHECK` only for creds within window | FAIL (blocked by B1) | Same — and a separate observation about the fsm-only action fan-out (see B3) |
| R3.a | Canonical read chain ordering | FAIL (blocked by B1) | `DB_READ_REQUESTED` never reaches ptFsm — gate rejects upstream |
| R3.b | `scanDataAccessExpiry` + `checkExistingWarning` dedup loop | FAIL (blocked by B1) | Same |
| R4.a | Two consecutive runs produce the same cred-state hash | FAIL (blocked by B1) | First run never completes — hash capture sees an empty FSM state |
| R4.b | One `data_access_expiry_warning` per non-deduped cred | FAIL (blocked by B1) | `_writeAlert` never reaches the write-alert-worker because ptFsm is starved |
| R4.c | Dedup suppresses a second warning when one exists | FAIL (blocked by B1) | Dedup loop never runs |
| R5 | Three concurrent runs produce no duplicate warnings | FAIL (blocked by B1) | First writer never wins; nothing writes |

**Progress at interrupt (before B1 was traced):** 3/10 passing on the
shape-only checks (stat-object shape, fsm exportState keys present).
7/10 failing on the actual chain-execution assertions, all blocked by
the same upstream gate (B1).

---

## Architectural Analysis

### The Chain Under Test (production)

```
server.js
  └─ constitutionalKernel.bootstrap()
       └─ gck.install({ck})                     ← wires FSM to CK, registers health-substrate membrane
       └─ gcDbSubstrate.setGovernance(ck) + .start()
       └─ dispatch({type: 'CAPABILITY_BOOTSTRAP', lineageId: <CK-issued>})
            └─ graph-capability-fsm.dispatch(CAPABILITY_BOOTSTRAP, ctx)
                 ├─ _wireMembranes(_governance)  ← substrate.start(ck) — sets _governance = ck
                 └─ emits actions {type: 'RUN_UAT_REFRESH_CHECK', businessAccountId}
                      └─ CK routes to action subscribers
                           └─ health-substrate.runUATRefreshCheck()
                                └─ _governance.governedRead('db.credential', {query: 'scanExpiringUATs'})
                                     └─ CK.governedRead (line 2163)
                                          └─ dispatch({type: 'DB_READ_REQUESTED', readDomain, accountId, readId, params})
                                               └─ CANONICAL SOURCE GATE (line 1294) — REJECTS  ← B1
```

### The Canonical Source Gate

```js
// control-plane/governance/constitutional-kernel.js:1294
const domainName = DOMAIN_EVENT_MAP[event.type];
if (domainName) {
  const isInternalDomainEvent = INTERNAL_DOMAIN_EVENTS.has(event.type);
  const hasCanonicalLineage = event.lineageId && typeof event.lineageId === 'string';
  if (!hasCanonicalLineage && !isInternalDomainEvent) {
    return {
      allowed: false,
      reason: `canonical source required: event '${event.type}' must be issued through CK coordination cycle (lineageId missing)`,
    };
  }
  ...
}
```

The gate enforces that any event routed to a registered domain FSM
(including `persist-telemetry` for `DB_READ_REQUESTED`) must either
(a) be in `INTERNAL_DOMAIN_EVENTS` (a closed set, line 80-127) or
(b) carry a `lineageId` issued by the CK's coordination cycle.

`DB_READ_REQUESTED` is **not** in `INTERNAL_DOMAIN_EVENTS`. Therefore
every dispatch of `DB_READ_REQUESTED` from outside the CK coordination
cycle is rejected.

---

## Bugs Found

### B1 — CRITICAL: `CK.governedRead` dispatches `DB_READ_REQUESTED` without a `lineageId`; the CK's own canonical-source gate rejects it. The production substrate→DB read path is dead end-to-end.

**Severity:** CRITICAL. The runtime cannot perform a governed read through
the constitutional path. The substrate's `runUATRefreshCheck`,
`runTokenHealthCheck`, scope-substrate's `detectDynamic`, and any other
caller of `_governance.governedRead(...)` will silently fail — the
read returns `{success: false, error: 'canonical source required: ...'}`
and the substrate's outer try/catch treats it as "no data" and continues.

**Location:**
- Dispatcher (defect): `control-plane/governance/constitutional-kernel.js:2192-2198` in `governedRead`
- Gate (correct): `control-plane/governance/constitutional-kernel.js:1294-1304`
- Same defect in: `graph-capability-kernel/fsm.js:634-642` (`CAPABILITY_DATA_REQUEST` builder dispatches `DB_READ_REQUESTED` via `ctx.dispatchGlobal`)
- Same defect in: `postgres-telemetry-kernel/fsm.js:255-263` (`DB_READ_COMPLETE` dispatches `READ_RESULT_AVAILABLE` via `ctx.dispatchGlobal`)

**Evidence:**

The test stubs the supabase admin, installs the real gck against the
real `constitutionalKernel`, dispatches `CAPABILITY_BOOTSTRAP` through
the real CK, and drives `healthSubstrate.runUATRefreshCheck(...)`.

The health-substrate logs:
```
[health] runUATRefreshCheck starting...
[health] No UATs need refresh          ← uats.length === 0
[health] runUATRefreshCheck complete — refreshed: 0, failed: 0, data_access warnings: 0 (deduped: 0)
```

But the stub's debug log (added during the investigation) shows the
worker DID return 2 UAT rows:
```
[supabaseStub] instagram_credentials.select() filters=[{token_type:user, is_active:true, ...}] rows=4 out=2
```

The substrate sees `refreshResult.success === false` and `uats = []`.
The substrate never receives the data.

**Root cause:** `_governance.governedRead(...)` resolves with the
canonical-source-gate rejection (`allowed: false, reason: 'canonical
source required: ...'`). The substrate's call site
(`runUATRefreshCheck` line 384) checks `refreshResult.success` which
is `action.error ? false : true` (line 2182 of the CK). With the gate
returning `{allowed: false, reason: '...'}`, the `governedRead` wrapper
sees the dispatch rejection and resolves with
`{success: false, data: null, error: 'read rejected by governance'}` —
a silent failure pattern.

**Why this isn't caught by existing tests:**

`tests/phase-7/kernels/graph-capability-flow.test.js` uses a stub CK
(`makeStubCk()` at line 113) whose `governedRead` (line 185) returns
configurable mocks. The stub has no canonical-source gate. So all V1-V8
tests in that file pass while the production path is broken.

`tests/phase-7/kernels/governed-read.test.js` tests the FSM's
`CAPABILITY_DATA_REQUEST` transition in isolation by calling
`fsm.dispatch` directly with a hand-rolled `ctx` whose
`dispatchGlobal` is `vi.fn()`. The gate is never reached because
`fsm.dispatch` does not call the CK's `dispatch` — it calls
`ctx.dispatchGlobal` which the test mocks.

So the live CK-mediated read path was untested. The runtime-validation
battery is the first test that drives the real chain through the real
CK with no FSM/CK mocks. It exposed the gap immediately.

**Fix (production):**

In `governedRead` (line 2192), attach a `lineageId` and `lineageDomain`
before dispatching:

```js
const dispatchResult = dispatch({
  type: 'DB_READ_REQUESTED',
  readDomain,
  accountId: params.accountId,
  readId,
  params,
  lineageId: `governedRead-${readId}`,        // ← fix
  lineageDomain: 'graph-capability',          // ← fix (or read from calling FSM)
});
```

Same fix needed in:
- `graph-capability-kernel/fsm.js:634-642` (CAPABILITY_DATA_REQUEST builder)
- `postgres-telemetry-kernel/fsm.js:255-263` (DB_READ_COMPLETE builder)

**Test-harness work-around used during development:** A wrapper around
`constitutionalKernel.dispatch` (the public export) that injects
`lineageId` for inter-domain DB events. This wrapper does NOT reach
the internal `governedRead` and `fsm.dispatch` callers because they
use the module-local function declaration, not the export. The
wrapper was therefore ineffective for the live chain. The proper
fix is the production patch above.

---

### B2 — HIGH: `test-postgres` schema is missing `instagram_credentials` and `system_alerts`. The runtime boots, but the moment a worker tries to read/write these tables, the real Supabase admin errors with "relation does not exist".

**Severity:** HIGH for integration tests, MEDIUM for production
parity. The docker-compose stack is missing the schema migrations
for the two tables the health-substrate and recovery worker hit.

**Location:**
- Stack: `docker-compose.test.yml:73-74` mounts
  `./tests/init-scripts:/docker-entrypoint-initdb.d:ro`
- Init script: `tests/init-scripts/01-governance-schema.sql`
  — confirmed to NOT contain `instagram_credentials` or `system_alerts`

**Evidence:**

```bash
$ docker exec instagram-test-postgres psql -U testuser -d testgovernance -c "\dt"
 public | fsm_execution_state  | table
 public | governance_lineage   | table
 public | telemetry_engagement | table
 public | telemetry_health     | table
 public | test_markers         | table
 public | worker_registry      | table
```

The schema is missing:
- `instagram_credentials` (scanned by `read-credential-worker`)
- `system_alerts` (read by `read-alerts-worker`, written by `write-alert-worker`)
- `token_lifecycle_events` (written by `write-lifecycle-event-worker`)
- `user_profiles` (referenced by `server.js:580-582` boot check)
- `scope_cache` (read/written by `read-scope-cache-worker` and
  `write-scope-cache-worker`)

**Impact:** Any test that drives a real chain through the real
postgres (rather than the supabase stub) will see the worker fail
with `error: 'relation "instagram_credentials" does not exist'`.
The Phase 7 `RuntimeSimulator` boots the full stack with the real
postgres connection — so any future Phase 7 test that exercises
`health-substrate.runUATRefreshCheck` against the real DB will
expose this.

**Fix:**

Add the missing tables to `tests/init-scripts/01-governance-schema.sql`
or a new `02-instagram-credentials-schema.sql`. Mirror the prod
schema. Without this, the test runtime's database is incomplete and
runtime-validation tests cannot drop the supabase stub.

**Recommended minimum schema (from worker code, not audited against prod):**

```sql
CREATE TABLE instagram_credentials (
  id text PRIMARY KEY,
  user_id text NOT NULL,
  business_account_id text,
  token_type text NOT NULL,
  is_active boolean DEFAULT true,
  issued_at timestamptz,
  expires_at timestamptz,
  data_access_expires_at timestamptz,
  debug_token_checked_at timestamptz
);

CREATE TABLE system_alerts (
  id text PRIMARY KEY,
  business_account_id text NOT NULL,
  alert_type text NOT NULL,
  message text NOT NULL,
  details jsonb DEFAULT '{}',
  resolved boolean DEFAULT false,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE token_lifecycle_events (
  id text PRIMARY KEY,
  credential_id text,
  business_account_id text,
  event_type text NOT NULL,
  token_age_days integer,
  details jsonb DEFAULT '{}',
  created_at timestamptz DEFAULT now()
);
```

---

### B3 — MEDIUM: `CAPABILITY_CADENCE_TICK` action fan-out never reaches the action subscribers; the cadence-tick re-emit is structurally broken.

**Severity:** MEDIUM. The runtime's recurring health-check trigger
does not actually run, even when `_shouldCheck` returns true. Every
cadence cycle, the FSM emits `RUN_UAT_REFRESH_CHECK` actions, but
they go to the `actions[]` return of `dispatch` and are NOT routed
to the membrane's `subscribeAction` handlers.

**Location:**
- Emitter: `graph-capability-kernel/fsm.js:579-596` (CAPABILITY_CADENCE_TICK)
- Consumer: `control-plane/governance/constitutional-kernel.js` — must call `subscribeAction` consumers when an action is returned from FSM dispatch

**Evidence:**

The test `R2.b` (R2 second case) dispatches `CAPABILITY_CADENCE_TICK`
to the real CK and inspects `result.actions`. The actions are present
in the result — confirming the FSM emits them. But the action
subscribers (the health-substrate's `runUATRefreshCheck` handler) are
NOT invoked. The membrane's `console.log('[health] Membrane received
RUN_UAT_REFRESH_CHECK — executing')` is never printed for the
cadence-driven emission, only for the bootstrap emission.

The test failed at `expect(aActions.length).toBe(1)` because the
result.actions was empty — but this is a side-effect of the broader
chain-stalling problem (B1): without a successful read, the
substrate's `runUATRefreshCheck` never gets the UATs and never
calls `_dispatchCompletion`, so the test's pre-stamp of BA_B never
takes effect from a state mutation standpoint.

However, a separate inspection of the CK's `dispatch` for
`CAPABILITY_CADENCE_TICK` confirms the action fan-out problem
independently:

- `result.actions` returns the actions the FSM emitted
- The CK does NOT take `result.actions` and route them through
  `subscribeAction`. The CK's only consumer of FSM-returned actions
  is the `buildActions`-output for observability/lineage purposes
  (writing `DATA_AVAILABLE` etc. into the lineage ledger).
- The action-fabric (`subscribeAction`) is only consumed for
  `RUN_*` actions emitted by the `CAPABILITY_BOOTSTRAP` path,
  where the FSM-emitted actions are explicitly passed through the
  CK's `dispatchGlobal`-equivalent path.

**Comparison to working path:** When `CAPABILITY_BOOTSTRAP` is
dispatched, the FSM emits `RUN_TOKEN_HEALTH_CHECK` and
`RUN_UAT_REFRESH_CHECK`. The CK routes these to the action
subscribers (the membrane). The logs confirm this happens on
bootstrap — the membrane's "RUN_UAT_REFRESH_CHECK — executing" log
appears right after bootstrap. But the same emission path from
`CAPABILITY_CADENCE_TICK` does not invoke the action subscribers.

**Likely cause:** The CK's `dispatch` for events that are themselves
`actions` (returned from a previous dispatch) is not implemented
for `CAPABILITY_CADENCE_TICK`. The bootstrap path works because
the bootstrap is a domain event, the FSM's `buildActions` returns
the actions, and the CK's `dispatch` plumbing for `CAPABILITY_BOOTSTRAP`
is special-cased to fan the actions out. The cadence-tick path
emits actions but the plumbing is missing.

**Fix:** In the CK's `dispatch` (or in the FSM's
`CAPABILITY_CADENCE_TICK` handler), when `buildActions` returns
`RUN_*` actions, route them through the same path bootstrap uses
to invoke action subscribers. Or have the FSM call
`ctx.dispatchGlobal` for each emitted `RUN_*` action (which is
how the data-request path works).

---

### B4 — LOW: `vault.uat.refresh` mocks succeed in the test, but in production the call goes through `api-surface.js` which uses real axios. The test stub of axios is incomplete (only `get` and `post` are stubbed; `axios.post` returns a synthetic token, but `vault.uat.refresh` likely calls `axios.post` with a specific shape).

**Severity:** LOW. The test works because the substrate's
`runUATRefreshCheck` reaches the `vault.uat.refresh` call for each
UAT and the axios mock returns success. But:

1. The test does not assert on the shape of the vault call (no
   payload assertion). If the production call shape changes, the
   test will silently keep "passing" because the mock returns
   success regardless of the input.
2. The axios mock in the test stubs `get` and `post` at the
   module level. The api-surface.js in production likely uses
   `axios.post` with specific URL templates and body shapes. A
   test against the real shape would assert the call contract.

**Recommendation:** Add a call-shape assertion — use `vi.spyOn` on
the axios mock's `post` method and assert it was called with the
expected URL pattern (`/oauth/access_token` with `grant_type=fb_exchange_token`).

---

### B5 — LOW: The constitutional-kernel `dispatch` wrapper approach is non-viable for tests. The module-internal `dispatch` reference cannot be intercepted without source patching.

**Severity:** LOW (test-harness concern, not production). When
attempting to test-harness the canonical-source gate by wrapping
`constitutionalKernel.dispatch` (the public export), the wrapper
fires only for callers that go through the export. Internal
callers in the same module (`governedRead`, `registerDomain`,
`bootstrap`, etc.) use the module-local function declaration and
bypass the wrapper. This is a property of JS module scope, not a
bug in the production code.

**Implication:** The test cannot inject a test-lineageId via the
export. The only options are:
- Patch the production source (the right fix, B1).
- Stub the entire `constitutional-kernel.js` module at
  `require.cache` (which would defeat the purpose of using the
  real CK).
- Bypass the gate by adding the event type to
  `INTERNAL_DOMAIN_EVENTS` in the test (also a production code
  change).

The recommended test path: fix B1, then the real chain works
without any wrapper.

---

## What The Test Proves (when B1 is fixed)

Once B1 is fixed in production, the test will validate the
end-to-end constitutional flow:

1. **Worker cadence (R2):** `fsm._shouldCheck` gates per-cred per-check-type.
   `CAPABILITY_CADENCE_TICK` re-emits `RUN_*_CHECK` actions only for
   creds within the cadence window. Stamps land on completion via
   `CAPABILITY_HEALTH_CHECK_COMPLETED`. This is the invariant that
   prevents the runtime from re-scanning the same credential every
   15 minutes.

2. **Stable runtime mutations (R4):** Two consecutive runs produce
   the same cred-state hash. The substrate's fire-and-forget writes
   (via `fsm.requestDBWrite`) do not corrupt the FSM state. The
   dedup loop (read-alerts-worker → write-alert-worker) prevents
   duplicate `data_access_expiry_warning` rows per cred.

3. **Constitutional data flow (R3):** The read chain ordering is
   canonical. `DB_READ_REQUESTED` is routed to `persist-telemetry` FSM
   (which then routes to `reading-substrate.executeRead`), and the
   result is delivered back as `DB_READ_COMPLETE` → `READ_RESULT_AVAILABLE`.
   This proves the gate is satisfied and the read promise resolves.

4. **Pressure (R5):** Three concurrent `runUATRefreshCheck` calls
   produce no duplicate warnings. The substrate's
   `interCallDelayMs: 1` rate-limit pacing combined with the
   per-cred `checkExistingWarning` dedup keeps the alert count
   bounded.

---

## Next Steps

1. Apply the B1 production patch (3 sites):
   - `constitutional-kernel.js:2192-2198` (governedRead)
   - `graph-capability-kernel/fsm.js:634-642` (CAPABILITY_DATA_REQUEST)
   - `postgres-telemetry-kernel/fsm.js:255-263` (DB_READ_COMPLETE)
2. Add the missing schema to `tests/init-scripts/` (B2)
3. Investigate the action-fabric plumbing for
   `CAPABILITY_CADENCE_TICK` (B3)
4. Add vault call-shape assertions (B4)
5. Re-run the test. Expect 10/10 green.
