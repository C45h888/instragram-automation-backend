# Constitutional Runtime Monitoring Report

**Observation Period:** 600 seconds (10 minutes)  
**Interval:** 15 seconds  
**Generated:** 2026-05-28  
**Output File:** `monitor-20260528-183701.jsonl`

---

## 1. Configured Tick/Timing Parameters Across All Phases

### Phase 2C: Long-Run Constitutional Endurance (Soak Test)

| Parameter | Environment Variable | Default Value | Phase 2C Fast Profile |
|-----------|---------------------|---------------|----------------------|
| Soak Duration | `PHASE2_SOAK_MS` | 180000 (3 min) | **20000 (20 sec)** |
| Tick Interval | `PHASE2_TICK_MS` | 120 ms | **20000 ms** |
| Checkpoint Interval | `PHASE2_CHECKPOINT_MS` | 30000 (30 sec) | **5000 (5 sec)** |
| Recycle Interval | `PHASE2_RECYCLE_MS` | 45000 (45 sec) | **7000 (7 sec)** |

### Phase 2A: Continuous Lineage Accumulation

| Parameter | Value |
|-----------|-------|
| Tick Interval | 100 ms |
| Test Duration | 3000 ms |

### Phase 2B: Reconciliation Drift Resistance

| Parameter | Value |
|-----------|-------|
| Tick Interval | 80 ms |
| Test Duration | 2500 ms |

### Phase 2D: Redis Durability

| Parameter | Value |
|-----------|-------|
| Tick Interval | 120 ms |
| Pre-restart Duration | 1600 ms |
| Post-restart Duration | 1600 ms |

### Observability Plane (projection.js)

| Parameter | Value |
|-----------|-------|
| Snapshot Interval | 30000 ms (30 sec) |
| Max Log Entries | 10,000 |
| Redis TTL | 300 sec (5 min) |
| Stall Warning Threshold | 80% of max (8,000 entries) |

### Telemetry Workers

| Worker | Default Poll Interval |
|--------|---------------------|
| All 5 workers (systemic, health, integrity, authority, runtime) | 30 ms (phase 2A/2B), 50 ms (phase 2C/2D) |

---

## 2. State Mutation Patterns Observed

### 2.1 Observation Summary

The 10-minute observation captured the runtime in an **idle state** with no active test tick injection occurring during the monitoring window.

### 2.2 Redis Key Observations

| Key | Initial State (iter 0) | Final State (iter 37) | Pattern |
|-----|----------------------|----------------------|---------|
| `governance:observability:projection` | type=string, size=1 | type=none, size=0 | Snapshot expired/deleted |
| `lineage:ledger:entries` | type=none, size=0 | type=none, size=0 | Never populated (no lineage worker active) |
| `lineage:worker:cursor` | type=none, size=0 | type=none, size=0 | Never populated |
| `lineage:projection:snapshot` | type=none, size=0 | type=none, size=0 | Never populated |

### 2.3 Projection Log Behavior

**Iterations 0-9:** 
- `governance:observability:projection` key existed as type=string
- `projection_log_size` returned `WRONGTYPE Operation against a key holding the wrong kind of value`
- This indicates a JSON snapshot string was stored (not a LIST), which is correct behavior for the snapshot key
- The `LLEN` command failed because it expects a LIST type

**Iterations 10-37:**
- All keys transitioned to `type=none, size=0`
- This indicates the Redis TTL (300 seconds) expired the snapshot
- No active event injection was occurring during observation

### 2.4 Lineage Ledger Growth

| Metric | Value |
|--------|-------|
| Lineage Ledger Size | 0 (no entries) |
| Observation | No lineage worker was consuming events during the idle observation period |

---

## 3. Consumer Lag Behavior

### 3.1 Consumer Cursor Status

| Consumer | Cursor Value |
|----------|--------------|
| `phase-2c-consumer` | null (not registered) |
| `phase-2d-consumer` | null (not registered) |

**Analysis:** No consumers were registered during the observation period because no test was actively running. Consumer cursors are only created when:
1. A test calls `observability.query.registerConsumer(name)`
2. The consumer updates its cursor via `updateConsumerCursor(name, cursor)`

### 3.2 Consumer Lag Architecture

The projection maintains a consumer cursor registry to protect against truncation:

```
Consumer cursors tracked: _consumerCursors Map
  - Cursor position = 0-based index of last consumed entry + 1
  - Stall warning at: STALL_WARNING_THRESHOLD = 8000 (80% of 10000 max)
  - getConsumerLag() returns: { cursor, head, lag, atRisk }
```

---

## 4. Memory Stability Assessment

### 4.1 Redis Memory Usage

| Iteration Range | Used Memory | Stability |
|----------------|-------------|-----------|
| 0-9 | 1.17M | Stable |
| 10-37 | 1.16M → 1.16M | **Stable** (no growth) |

**Assessment: ✅ MEMORY STABLE**

No memory leak detected during the 10-minute observation. The slight decrease (1.17M → 1.16M) is within normal variance due to Redis internal memory management.

### 4.2 Connected Clients

| Iteration Range | Connected Clients |
|----------------|-------------------|
| 0-9 | 1 |
| 10 | 2 (transient spike) |
| 11-37 | 1 |

**Assessment: ✅ CONNECTED CLIENTS STABLE**

The single connected client indicates the Node.js test runner was idle. The transient spike to 2 clients (iteration 10) may indicate a brief connection from monitoring or health-check tooling.

---

## 5. Container Health Status

| Container | Status | Running | Health Check |
|-----------|--------|---------|--------------|
| `instagram-test-redis` | healthy | true | ✅ Passing |
| `instagram-test-postgres` | healthy | true | ✅ Passing |
| `instagram-test-runner` | unknown | true | ⚠️ No health check defined |
| `instagram-test-partitioner` | N/A | true | N/A |

### Assessment: ✅ ALL CONTAINERS HEALTHY

- Redis and PostgreSQL have defined health checks and report healthy status
- The runner container does not have a health check defined (acceptable for test workloads)
- No container restarts or failures observed during the 10-minute window

---

## 6. Anomalies Detected

### 6.1 WRONGTYPE Error on projection_log_size (iterations 0-9)

**Observation:** `LLEN governance:observability:projection` returned error: `WRONGTYPE Operation against a key holding the wrong kind of value`

**Root Cause:** The key `governance:observability:projection` stores a **JSON string snapshot** (type=string), not a Redis LIST. The `LLEN` command expects a LIST type.

**Code Reference:** `projection.js` line ~93:
```javascript
await redis.set(REDIS_KEY, JSON.stringify(snapshot), 'EX', REDIS_TTL_S);
```

**Impact:** None - this is expected behavior. The monitoring script was using an incorrect Redis command for this key type. The actual projection log is stored in **memory** (`_transitionLog` array), not in Redis.

**Correct Approach:** To get the actual projection log size, use `getLogSize()` from the observability query API, which returns `projection.getLogSize()` (the in-memory array length).

### 6.2 Snapshot Key Expiration (iterations 10-37)

**Observation:** All Redis keys transitioned from `size=1, type=string` to `type=none, size=0`

**Root Cause:** The Redis TTL of 300 seconds (5 minutes) expired the snapshot key. This is expected behavior for an idle system with no active event injection.

**Impact:** None - snapshots are only needed for crash recovery. Without active event injection, the in-memory projection remains the source of truth.

### 6.3 No Active Tick Events During Observation

**Observation:** The monitoring script observed zero state mutations throughout the 10-minute window.

**Root Cause:** The monitoring was conducted during an **idle period** where no test was actively running. The system's tick-driven event injection only occurs when a test (phase-2c-long-run-endurance, etc.) is actively executing.

**Impact:** The observation captured baseline/idle behavior only. To observe active state mutations, monitoring must be conducted **concurrently with an active soak test** using the Phase 2C fast profile configuration.

---

## 7. Key Findings Summary

| Category | Finding | Status |
|----------|---------|--------|
| Memory Stability | Redis memory stable at 1.16-1.17M with no growth | ✅ PASS |
| Container Health | All containers healthy and running | ✅ PASS |
| Consumer Lag | No consumers registered during idle observation | ⚠️ IDLE |
| Projection Growth | No growth observed (idle state) | ⚠️ IDLE |
| Lineage Ledger | No entries (no lineage worker active) | ⚠️ IDLE |
| Redis Key Durability | Snapshot expired after TTL (expected) | ✅ EXPECTED |
| Tick/Event Generation | No ticks observed (no active test) | ⚠️ IDLE |

---

## 8. Recommendations

1. **For Active Monitoring:** Run the monitor script **concurrently** with an active Phase 2C soak test to observe actual tick events and state mutations:
   ```bash
   # Terminal 1: Start Phase 2C soak test
   PHASE2_SOAK_MS=600000 PHASE2_TICK_MS=20000 PHASE2_CHECKPOINT_MS=5000 PHASE2_RECYCLE_MS=7000 npm test -- phase-2c
   
   # Terminal 2: Run monitoring in parallel
   DURATION=600 INTERVAL=15 ./tests/monitor-runtime.sh
   ```

2. **Fix Monitoring Script:** The `monitor-runtime.sh` script uses `LLEN` on the snapshot key which is a STRING type. Either:
   - Remove `projection_log_size` monitoring (in-memory, not Redis)
   - Use a different key for the in-memory log if persistence is needed

3. **Add Runner Health Check:** Define a health check for the `test-runner` container to enable complete container health monitoring.

---

## 9. Event Flow Architecture (Reference)

```
Event Injector (event-injector.js)
         ↓
observability.transition() [observability/index.js]
         ↓
transitionEmitter.transition() [emitters/transition-emitter.js]
         ↓
normalizer.normalize() [normalizer.js]
         ↓
projection.project() [projection.js] ← In-memory state index
         ↓
Periodic Snapshot [projection.js] → Redis (TTL 300s)
         ↓
Telemetry Workers (5 workers) [telemetry-workers/index.js]
         ↓
SEMANTIC_PROJECTION_TRANSITION events
         ↓
Lineage Worker (consumes via cursor)
         ↓
Immutable Unified Ledger
```

---

*Report generated from 38 observation iterations over 600 seconds*  
*Metrics file: `tests/output/runtime-observations/monitor-20260528-183701.jsonl`*
