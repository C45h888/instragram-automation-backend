// control-plane/orchestration/cadence-orchestrator.js
// Cadence Orchestrator: constitutional coordination membrane.
//
// Owns: routing CADENCE_TICK maintenance actions downward,
//        forwarding maintenance observations upward.
// Does NOT own: governance policy, domain semantics, execution intelligence.
//
// Constitutional purity: this orchestrator is a PACKET ROUTER, not a manager.
// It mechanically dispatches maintenance work and forwards results upward.
// It NEVER interprets runtime meaning.

const dbScanner = require('../runtime/db-scanner');
const lifecycle = require('../runtime/lifecycle');
const safety = require('../runtime/operational-safety');
const metricsSubstrate = require('../../substrates/metrics-substrate');
const persistence = require('../../substrates/persistence');

/**
 * Wire this orchestrator to the governance kernel.
 * Registers per-action-type subscribers for maintenance actions.
 * Called once at startup by the composition root.
 *
 * @param {object} governance — governance kernel module
 */
function wire(governance) {
  // ── SCAN_DATABASE → dbScanner ──────────────────────────────────────────
  governance.subscribeAction('SCAN_DATABASE', (action) => {
    dbScanner.runScan(governance).then(r => {
      if (r.totalEmitted > 0) {
        console.log(`[cadence-orchestrator] DB scanner emitted ${r.totalEmitted} intents`);
      }
      governance.dispatch({ type: 'DATABASE_SCANNED', intentCount: r.totalEmitted });
    }).catch(err => {
      console.error('[cadence-orchestrator] DB scanner error:', err.message);
      governance.dispatch({ type: 'DATABASE_SCANNED', intentCount: 0 });
    });
  });

  // ── REFRESH_LIFECYCLE → lifecycle → persistence ────────────────────────
  governance.subscribeAction('REFRESH_LIFECYCLE', (action) => {
    lifecycle.refresh().then(() => {
      return persistence.getActiveAccounts();
    }).then(accounts => {
      governance.dispatch({ type: 'LIFECYCLE_REFRESHED', accountIds: accounts.map(a => a.id) });
    }).catch(err => {
      console.error('[cadence-orchestrator] Lifecycle refresh error:', err.message);
      governance.dispatch({ type: 'LIFECYCLE_REFRESHED', accountIds: [] });
    });
  });

  // ── CHECK_SAFETY → safety module ───────────────────────────────────────
  governance.subscribeAction('CHECK_SAFETY', (action) => {
    safety.runChecks().then(() => {
      governance.dispatch({ type: 'SAFETY_CHECK_COMPLETE' });
    }).catch(err => {
      console.error('[cadence-orchestrator] Safety check error:', err.message);
      governance.dispatch({ type: 'SAFETY_CHECK_COMPLETE' });
    });
  });

  // ── REPORT_METRICS → metricsSubstrate ──────────────────────────────────
  governance.subscribeAction('REPORT_METRICS', (action) => {
    const signals = metricsSubstrate.getHealthSignals();
    governance.dispatch({
      type: 'WORKER_METRICS_REPORTED',
      total: signals.total,
      failed: signals.failed,
      failureRate: signals.failureRate,
      windowMs: signals.windowMs,
    });
  });
}

module.exports = { wire };
