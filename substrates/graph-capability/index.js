// substrates/graph-capability/index.js
// Graph Capability substrate façade.
//
// Owns: worker lifecycle (start/stop), observation aggregation, FSM dispatch.
// Does NOT own: Graph API calls, vault I/O, DB queries, FSM state.
//
// Architectural invariant:
//   Workers run on their own setInterval timers (substrate-encapsulated cadence).
//   Façade aggregates observations and emits CAPABILITY_OBSERVATION to FSM.
//   FSM is the only writer of canonical capability state.

const cadence = require('./cadence');
const observations = require('./observations');
const PatWorker = require('./workers/pat-worker');
const ScopeWorker = require('./workers/scope-worker');
const UatWorker = require('./workers/uat-worker');
const DetectionWorker = require('./workers/detection-worker');

// ── Worker registry ──────────────────────────────────────────────────────────

let _patWorker = null;
let _scopeWorker = null;
let _uatWorker = null;
let _detectionWorker = null;

let _aggregateInterval = null;
let _reevaluateInterval = null;
let _started = false;

// Latest observation envelopes (one per worker)
let _latestPat = null;
let _latestScope = null;
let _latestUat = null;
let _latestDetection = null;

// ── FSM dispatch bridge ──────────────────────────────────────────────────────

let _fsm = null;
let _ckContext = null;

function bindFsm(fsm, ctx) {
  _fsm = fsm;
  _ckContext = ctx;
}

function _dispatchToFsm(event) {
  if (!_fsm || typeof _fsm.dispatch !== 'function') {
    return;
  }
  try {
    _fsm.dispatch(event, _ckContext);
  } catch (err) {
    console.warn(`[graph-capability] FSM dispatch failed for ${event.type}:`, err.message);
  }
}

// ── Observation collection ──────────────────────────────────────────────────

function _recordPatObservation(envelope) { _latestPat = envelope; }
function _recordScopeObservation(envelope) { _latestScope = envelope; }
function _recordUatObservation(envelope) { _latestUat = envelope; }
function _recordDetectionObservation(envelope) { _latestDetection = envelope; }

// ── Aggregation tick — collapses all four workers into one observation ───────

function _aggregateTick() {
  const aggregate = observations.normalize({
    pat: _latestPat,
    scope: _latestScope,
    uat: _latestUat,
    detection: _latestDetection,
  });

  // Map normalized state → FSM event type
  const eventType = (() => {
    switch (aggregate.state) {
      case 'AUTHORIZED': return 'CAPABILITY_OK';
      case 'LIMITED': return 'CAPABILITY_PARTIAL';
      case 'DEGRADED': return 'CAPABILITY_DEGRADED';
      case 'UNAUTHORIZED': return 'CAPABILITY_FAILED';
      case 'UNKNOWN':
      default: return null; // do not dispatch on UNKNOWN — wait for more data
    }
  })();

  if (eventType) {
    _dispatchToFsm({
      type: eventType,
      observedAt: aggregate.observedAt,
      evidence: aggregate.evidence,
      missingScopes: aggregate.missingScopes,
      reason: aggregate.reason,
    });
  }
}

function _reevaluateTick() {
  _dispatchToFsm({ type: 'CAPABILITY_REEVALUATE', cadence: 'substrate' });
}

// ── Public lifecycle ────────────────────────────────────────────────────────

/**
 * Start the substrate façade. Initializes workers, registers intervals.
 *
 * @param {{ fsm: object, ctx: object }} bindings — FSM + constitutional context
 */
function start(bindings = {}) {
  if (_started) {
    console.log('[graph-capability] Substrate already started');
    return;
  }
  if (bindings.fsm) bindFsm(bindings.fsm, bindings.ctx);

  // Initialize workers — they register their own setInterval in start()
  _patWorker = new PatWorker({ onObservation: _recordPatObservation });
  _scopeWorker = new ScopeWorker({ onObservation: _recordScopeObservation });
  _uatWorker = new UatWorker({ onObservation: _recordUatObservation });
  _detectionWorker = new DetectionWorker({ onObservation: _recordDetectionObservation });

  _patWorker.start();
  _scopeWorker.start();
  _uatWorker.start();
  _detectionWorker.start();

  // Façade aggregation cadence
  _aggregateInterval = setInterval(_aggregateTick, cadence.AGGREGATION_INTERVAL_MS);
  _reevaluateInterval = setInterval(_reevaluateTick, cadence.FULL_REEVALUATE_INTERVAL_MS);

  _started = true;
  console.log('[graph-capability] Substrate started');
}

/**
 * Stop the substrate façade. Clears all intervals.
 */
function stop() {
  if (!_started) return;

  if (_patWorker) _patWorker.stop();
  if (_scopeWorker) _scopeWorker.stop();
  if (_uatWorker) _uatWorker.stop();
  if (_detectionWorker) _detectionWorker.stop();

  if (_aggregateInterval) clearInterval(_aggregateInterval);
  if (_reevaluateInterval) clearInterval(_reevaluateInterval);

  _aggregateInterval = null;
  _reevaluateInterval = null;
  _started = false;
  console.log('[graph-capability] Substrate stopped');
}

// ── Public read surface (for Phase 4 consumer migration) ────────────────────

function getLatestObservations() {
  return {
    pat: _latestPat,
    scope: _latestScope,
    uat: _latestUat,
    detection: _latestDetection,
  };
}

function isStarted() {
  return _started;
}

module.exports = {
  start,
  stop,
  bindFsm,
  getLatestObservations,
  isStarted,
  cadence,
  observations,
};
