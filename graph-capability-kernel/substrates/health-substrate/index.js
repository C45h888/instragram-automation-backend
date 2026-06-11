// graph-capability-kernel/substrates/health-substrate/index.js
// Health substrate façade — thin delegation shell.
// All operational logic lives in TokenHealthWorker (substrates/workers/).
//
// Constitutional role:
//   - Delegates to TokenHealthWorker for token health checks + UAT refresh
//   - Acts as FSM membrane: subscribes to RUN_TOKEN_HEALTH_CHECK, RUN_UAT_REFRESH_CHECK
//   - Signal dispatch still flows through this layer (envelope + evaluate)
//
// This façade owns:
//   - Membrane lifecycle (start/stop/subscribe)
//   - Delegation to TokenHealthWorker
//
// This façade does NOT own:
//   - Token validation logic (TokenHealthWorker)
//   - UAT refresh logic (TokenHealthWorker)
//   - Recovery logic (TokenHealthWorker)
//   - DB reads/writes (delegated through CK.governedRead)
//   - Error classification (ig-reliability-substrate)
//
// Migration: operational logic extracted to TokenHealthWorker (Pass 1, 2026-06-11).
//            RecoveryWorker absorbed into TokenHealthWorker._recoverPatViaUat().
//            This file is the membrane shell only.

const TokenHealthWorker = require('../workers/token-health-worker');

// ── Singleton worker instance ──────────────────────────────────────────────

let _worker = null;
let _started = false;
let _governance = null;

function _getWorker() {
  if (!_worker) {
    _worker = new TokenHealthWorker();
  }
  return _worker;
}

// ── Public lifecycle ─────────────────────────────────────────────────────────

function start(governance) {
  if (_started) return;
  _started = true;
  _governance = governance;

  const worker = _getWorker();
  worker.start(governance);

  console.log('[health] Membrane wired — delegation to TokenHealthWorker');
}

function stop() {
  if (!_started) return;
  _started = false;
  if (_worker) {
    _worker.stop();
  }
}

function isStarted() {
  return _started;
}

// ── Public operations (delegated, kept for backward compat) ────────────────

function runTokenHealthCheck(opts) {
  const worker = _getWorker();
  return worker.executeTokenHealth(opts);
}

function runUATRefreshCheck(opts) {
  const worker = _getWorker();
  return worker.executeUatRefresh(opts);
}

module.exports = {
  start,
  stop,
  isStarted,
  runTokenHealthCheck,
  runUATRefreshCheck,
};
