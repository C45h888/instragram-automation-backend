// control-plane/governance/lineage-checkpointer.js
// Constitutional Lineage Checkpointer: failure-domain asymmetric recovery anchor.
//
// Owns: bounded equilibrium snapshots of canonical lineage state to a
//        filesystem checkpoint that survives Redis annihilation.
//        Checkpoint is CK-governed — only created when ALL stability gates pass.
//
// Does NOT own: lineage persistence (lineage-ledger owns Redis canonical store),
//               checkpoint timing (CK evaluates gates and calls createSnapshot),
//               task preservation (CK snapshots in-flight tasks separately).
//
// Failure-domain asymmetry:
//   Primary lineage:  Redis (lineage:ledger:entries) — high-frequency, volatile
//   Checkpoint:       Filesystem (CHECKPOINT_PATH) — low-frequency, survivable
//   FLUSHALL destroys Redis but cannot reach the filesystem checkpoint.
//
// Architectural invariant:
//   This module is a DUMB SUBSTRATE. It reads/writes a file only.
//   It NEVER evaluates constitutional meaning, stability, or timing.
//   The CK governs when checkpoints are created, read, and cleared.
//
// Death-detection criteria (evaluated by CK, not here):
//   C1: Total extinction (ledgerSize === 0)
//   C2: Partial truncation (ledgerSize < checkpoint.entryCount * 0.5)
//   C3: Epoch regression (reconEpoch < checkpoint.epochCount)
//   C4: Hash discontinuity — deferred (requires hash chain in ledger)
//
// Checkpoint format:
// {
//   version: 1,
//   ts: <timestamp>,
//   hash: <sha256>,
//   entryCount: <number>,
//   entries: [ ...last N entries, max 200 ],
//   domainStates: { acquisition: "...", publishing: "...", ... },
//   epochCount: <number>
// }

const fs = require('fs');
const path = require('path');

const CHECKPOINT_PATH = process.env.CHECKPOINT_PATH
  || path.resolve(process.cwd(), 'tests/output/lineage-checkpoint.json');

const MAX_ENTRIES = 200;

// ═══════════════════════════════════════════════════════════════════════════════
// Read / Write
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Create a bounded equilibrium snapshot of the current constitutional state.
 * Writes to the filesystem checkpoint file.
 *
 * Called ONLY by CK when all stability gates pass.
 *
 * @param {object} opts
 * @param {Array<object>} opts.entries — last N lineage entries (max MAX_ENTRIES)
 * @param {string} opts.hash — current constitutional hash
 * @param {number} opts.entryCount — total lineage entry count
 * @param {object} opts.domainStates — { domainName: state } for all registered domains
 * @param {number} opts.epochCount — reconciliation epoch count
 * @returns {{ ts: number, hash: string, entryCount: number }}
 */
function createSnapshot({ entries, hash, entryCount, domainStates, epochCount }) {
  const snapshot = {
    version: 1,
    ts: Date.now(),
    hash: hash || '',
    entryCount: entryCount || 0,
    entries: (entries || []).slice(-MAX_ENTRIES),
    domainStates: domainStates || {},
    epochCount: epochCount || 0,
  };

  try {
    const dir = path.dirname(CHECKPOINT_PATH);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    fs.writeFileSync(CHECKPOINT_PATH, JSON.stringify(snapshot, null, 2) + '\n', 'utf8');
    console.log(`[lineage-checkpointer] Snapshot written: ${snapshot.entryCount} total entries, hash=${snapshot.hash.slice(0, 16)}`);
  } catch (err) {
    console.error('[lineage-checkpointer] Failed to write snapshot:', err.message);
    throw err;
  }

  return { ts: snapshot.ts, hash: snapshot.hash, entryCount: snapshot.entryCount };
}

/**
 * Read the current checkpoint from the filesystem.
 *
 * @returns {object|null} checkpoint object, or null if missing/corrupt
 */
function getCheckpoint() {
  try {
    if (!fs.existsSync(CHECKPOINT_PATH)) return null;
    const raw = fs.readFileSync(CHECKPOINT_PATH, 'utf8');
    const checkpoint = JSON.parse(raw);
    if (!checkpoint || typeof checkpoint.version !== 'number') return null;
    return checkpoint;
  } catch (err) {
    console.warn('[lineage-checkpointer] Failed to read checkpoint:', err.message);
    return null;
  }
}

/**
 * Clear the checkpoint file after successful rehydration.
 */
function clearCheckpoint() {
  try {
    if (fs.existsSync(CHECKPOINT_PATH)) {
      fs.unlinkSync(CHECKPOINT_PATH);
      console.log('[lineage-checkpointer] Checkpoint cleared');
    }
  } catch (err) {
    console.warn('[lineage-checkpointer] Failed to clear checkpoint:', err.message);
  }
}

module.exports = {
  createSnapshot,
  getCheckpoint,
  clearCheckpoint,
  CHECKPOINT_PATH,
  MAX_ENTRIES,
};
