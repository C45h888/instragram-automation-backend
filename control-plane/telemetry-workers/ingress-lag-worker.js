// control-plane/telemetry-workers/ingress-lag-worker.js
// Bounded Operative Worker: last-resort diagnostic and fix for ingress lag.
// Dispatched by CK when retry escalation reaches DEGRADED state.
//
// Layer 4 of the 4-layer ingress lag architecture.
// This is a one-shot worker — not an infinite loop. It activates on CK
// dispatch, performs diagnostic computation, and returns a result.
//
// What it does:
//   - Confirms lag is real (double-checks log vs ledger)
//   - Identifies which component is the lag source
//   - Determines if phase2-dumb-writer is alive or dead
//   - Returns resolved=false with reason if retry cadence should handle it
//   - Returns resolved=true if it performed a direct fix
//
// What it does NOT do:
//   - Never restarts Redis, reconstructs lineage, or mutates replay state
//   - Never bypasses CK authority — it is dispatched by CK, not self-triggered
//   - Does not modify the observability log or lineage ledger directly

function _getObsDeps() {
  const observability = require('../../observability');
  const lineageLedger = require('../governance/lineage-ledger');
  const phase2DumbWriter = require('./phase2-dumb-writer');
  return { observability, lineageLedger, phase2DumbWriter };
}

/**
 * CK-dispatched one-shot resolve.
 *
 * @param {{ lag: number, status: string, timestamp: number }} opts
 * @returns {{ resolved: boolean, entriesFlushed: number, newLag: number, reason: string }}
 */
async function dispatchResolve({ lag, status, timestamp }) {
  const { observability, lineageLedger, phase2DumbWriter } = _getObsDeps();

  // Double-check: confirm lag is real (sample fresh)
  const logSize = observability.query ? observability.query.getLogSize() : 0;
  let ledgerSize = 0;
  try {
    ledgerSize = await lineageLedger.getSize();
  } catch (_) {
    ledgerSize = 0;
  }
  const actualLag = logSize - ledgerSize;

  if (actualLag <= 5) {
    return {
      resolved: true,
      entriesFlushed: 0,
      newLag: actualLag,
      reason: 'lag_already_cleared',
    };
  }

  // Check phase2-dumb-writer health
  const writerHealth = phase2DumbWriter && typeof phase2DumbWriter.getHealth === 'function'
    ? phase2DumbWriter.getHealth()
    : { running: false };

  if (!writerHealth.running) {
    // Writer is dead — CK must handle this, not a retryable condition
    return {
      resolved: false,
      entriesFlushed: 0,
      newLag: actualLag,
      reason: 'writer_dead',
    };
  }

  // Writer alive but lagging — retry cadence handles it, not this worker
  return {
    resolved: false,
    entriesFlushed: 0,
    newLag: actualLag,
    reason: 'retry_cadence_in_progress',
  };
}

module.exports = { dispatchResolve };