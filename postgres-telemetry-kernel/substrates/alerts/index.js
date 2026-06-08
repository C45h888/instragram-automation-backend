// postgres-telemetry-kernel/substrates/alerts/index.js
// Alerts Substrate — bounded Supabase operations for system_alerts and token_lifecycle_events.
//
// Owns: worker registry, dispatch routing, start/stop lifecycle.
// Does NOT own: governance policy (CK + FSM), signal dispatch, audit logging.
//
// Every worker is operationally bounded to ONE Supabase operation.
// Writers: fire-and-forget best-effort INSERT (matches existing health-substrate semantics).
// Readers: governed SELECT, returned through CK → READ_RESULT_AVAILABLE.
//
// Phase 2: health-substrate/index.js wires through CK → DB_WRITE_REQUESTED / CAPABILITY_DATA_REQUEST
//   health-substrate → CK.dispatch(DB_WRITE_REQUESTED) → FSM → substrate.dispatch()
//   consumer → CK.dispatch(CAPABILITY_DATA_REQUEST) → FSM → reading-substrate → read worker

const workers = {
  writeAlert:              require('./workers/write-alert-worker'),
  writeLifecycleEvent:     require('./workers/write-lifecycle-event-worker'),
  readAlerts:              require('./workers/read-alerts-worker'),
  readLifecycleEvents:     require('./workers/read-lifecycle-events-worker'),
};

const WORKER_MAP = {
  insert_alert:              'writeAlert',
  insert_lifecycle_event:    'writeLifecycleEvent',
  read_alerts:               'readAlerts',
  read_lifecycle_events:     'readLifecycleEvents',
};

let _started = false;
let _governance = null;

function start() {
  if (_started) return;
  _started = true;
  console.log('[alerts-substrate] Started — 4 workers armed (write-alert, write-lifecycle-event, read-alerts, read-lifecycle-events)');
}

function stop() {
  if (!_started) return;
  _started = false;
  _governance = null;
  console.log('[alerts-substrate] Stopped');
}

function isStarted() {
  return _started;
}

function setGovernance(gov) {
  _governance = gov;
}

/**
 * Dispatch an operation to the bound worker.
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
  workers,
  WORKER_MAP,
};
