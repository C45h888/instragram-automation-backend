// control-plane/observability/index.js
// Observability Plane: unified state projection backbone for the runtime.
//
// Owns: normalizing runtime mutations into canonical STATE_TRANSITION events,
//        maintaining the in-memory live projection, and providing query interfaces.
//
// Does NOT own: lineage ledger, governance policy, FSM logic, or orchestration.
//
// Public API (read-only consumers call these):
//   query.getState(domain, entity, entityId)    — current entity state
//   query.getDomainState(domain)               — all entities in a domain
//   query.getTransitionLog(domain, entity, n)  — last n transitions for an entity
//   query.getCrossDomain(domains)              — states across multiple domains
//   query.getFullSnapshot()                    — entire projection (CK, lineage writer)
//   getSnapshot()                              — alias for getFullSnapshot()
//
// Subsystems call:
//   transition({ domain, entity, entityId, previousState, nextState, authority, raw })
//   capture(topic, data) — for signal-bus events (auto-wired via signal-bus-integration)
//
// Initialization:
//   observability.init() — call once at system boot
//   observability.stop() — call at shutdown for clean Redis snapshot
//
// Access control (enforced in Pass 2):
//   CK receives full query access via getFullSnapshot()
//   Lineage writer receives full query access (to be wired in Pass 2)
//   FSMs receive scoped domain-only queries via domainJurisdiction query proxy

const projection = require('./projection');
const transitionEmitter = require('./emitters/transition-emitter');
const signalBusIntegration = require('./bus/signal-bus-integration');

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Emit a state transition into the observability plane.
 * Call this before every state mutation in every subsystem.
 *
 * @param {object} params — see transition-emitter.js for full signature
 */
function transition(params) {
  transitionEmitter.transition(params);
}

/**
 * Capture a signal-bus event into the observability plane.
 * Normally not called directly — signal-bus-integration calls this automatically.
 *
 * @param {string} topic — signal bus topic
 * @param {object} data — signal bus payload
 */
function capture(topic, data) {
  transitionEmitter.captureSignal(topic, data);
}

/**
 * Query the projection — returns the current state of a specific entity.
 *
 * @param {string} domain
 * @param {string} entity
 * @param {string} entityId
 * @returns {string|null}
 */
function getState(domain, entity, entityId) {
  return projection.getState(domain, entity, entityId);
}

/**
 * Query the projection — returns all entity states within a domain.
 *
 * @param {string} domain
 * @returns {{ [entity]: { [entityId]: string } }}
 */
function getDomainState(domain) {
  return projection.getDomainState(domain);
}

/**
 * Query the projection — returns the last N transitions for a specific entity.
 *
 * @param {string} domain
 * @param {string} entity
 * @param {string} entityId
 * @param {number} [n=10]
 * @returns {Array<object>}
 */
function getTransitionLog(domain, entity, entityId, n = 10) {
  return projection.getTransitionLog(domain, entity, entityId, n);
}

/**
 * Query the projection — returns states across multiple domains.
 * Used by FSMs for cross-domain state awareness.
 *
 * @param {Array<string>} domains
 * @returns {object}
 */
function getCrossDomain(domains) {
  return projection.getCrossDomain(domains);
}

/**
 * Get the full projection snapshot.
 * Used by CK for reconciliation (Pass 3) and lineage writer (Pass 2).
 *
 * @returns {object}
 */
function getFullSnapshot() {
  return projection.getFullSnapshot();
}

/**
 * Alias for getFullSnapshot() — for consumers who prefer this name.
 */
function getSnapshot() {
  return projection.getFullSnapshot();
}

// ── Module lifecycle ──────────────────────────────────────────────────────────

/**
 * Initialize the observability plane.
 * Call once at system boot, after Redis is connected.
 *
 * - Rehydrates projection from Redis snapshot (if any)
 * - Starts periodic snapshot timer (every 30s)
 * - Patches signalBus.emit() for automatic signal capture
 *
 * @returns {Promise<void>}
 */
async function init() {
  await projection.init();
  signalBusIntegration.init();
  console.log('[observability] Plane initialized — projection active, signal capture wired');
}

/**
 * Stop the observability plane.
 * Persists final snapshot to Redis and stops the snapshot timer.
 * Call at system shutdown for clean persistence.
 *
 * @returns {Promise<void>}
 */
async function stop() {
  await projection.stopSnapshotTimer();
  console.log('[observability] Plane stopped — final snapshot persisted');
}

module.exports = {
  transition,
  capture,
  query: {
    getState,
    getDomainState,
    getTransitionLog,
    getCrossDomain,
    getFullSnapshot,
  },
  getSnapshot,
  init,
  stop,
};
