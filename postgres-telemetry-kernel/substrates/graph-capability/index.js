// postgres-telemetry-kernel/substrates/graph-capability/index.js
// Graph Capability DB Substrate — bounded Supabase operations for the graph-capability domain.
//
// Owns: worker registry, dispatch routing, start/stop lifecycle.
// Does NOT own: governance policy (CK + FSM), encryption (vault concern),
//               Graph API calls, signal dispatch, audit logging.
//
// Every worker is operationally bounded to ONE Supabase operation.
// Workers are semantically blind — they know table/column, not business intent.
//
// Phase 2: CK wires this substrate via governance membrane.
//   graph-capability façade → CK.dispatch(DB_READ_REQUESTED) → FSM → substrate.dispatch()

const workers = {
  readScopeCache:          require('./workers/read-scope-cache-worker'),
  readCredential:          require('./workers/read-credential-worker'),
  readKey:                 require('./workers/read-key-worker'),
  writeScopeCache:         require('./workers/write-scope-cache-worker'),
  updateCredentialStatus:  require('./workers/update-credential-status-worker'),
};

const WORKER_MAP = {
  read_scope_cache:         'readScopeCache',
  read_credential:          'readCredential',
  read_key:                 'readKey',
  write_scope_cache:        'writeScopeCache',
  update_credential_status: 'updateCredentialStatus',
};

let _started = false;
let _governance = null;

function start() {
  if (_started) return;
  _started = true;
  console.log('[graph-capability-substrate] Started — 5 workers armed');
}

function stop() {
  if (!_started) return;
  _started = false;
  _governance = null;
  console.log('[graph-capability-substrate] Stopped');
}

function isStarted() {
  return _started;
}

function setGovernance(gov) {
  _governance = gov;
}

/**
 * Dispatch an operation to the bound worker.
 * Returns the worker's result. Synchronous dispatch — worker.execute() is async.
 *
 * @param {string} operation — one of WORKER_MAP keys
 * @param {object} params — worker-specific parameters
 * @returns {Promise<{ success: boolean, data?: any, error?: string }>}
 */
async function dispatch(operation, params) {
  const workerKey = WORKER_MAP[operation];
  if (!workerKey) {
    return { success: false, data: null, error: `unknown_operation: ${operation}` };
  }

  const worker = workers[workerKey];
  if (!worker || typeof worker.execute !== 'function') {
    return { success: false, data: null, error: `worker_not_found: ${operation}` };
  }

  return worker.execute(params, _governance);
}

module.exports = {
  start,
  stop,
  isStarted,
  setGovernance,
  dispatch,
  // Exposed for direct worker access (Phase 2 wiring)
  workers,
  WORKER_MAP,
};
