// Phase 9 — DB Reset.
// Phase 9 tests need a clean lineage/mutation state per file.
// The test postgres is provisioned by docker-compose.test.yml,
// but the runtime harness boots the real substrates that depend
// on Redis. The harness's underlying RuntimeSimulator will create
// the substrate snapshots it needs. This module exposes a no-op
// reset for now (the runtime's per-boot isolation is sufficient
// inside vitest's singleFork mode).
//
// If a future phase adds per-test isolation for postgres-backed
// tables, this is where the TRUNCATE statements land.

export async function dbReset() {
  // Runtime is per-test-file isolated in vitest's singleFork mode.
  // The simulator's shutdown drains the projection workers and
  // resets the lineage ledger in-memory. No DB-level reset needed
  // for the current substrate topology.
  return Promise.resolve();
}
