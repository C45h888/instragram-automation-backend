// substrates/db/writers/index.js
// DB Writers — canonical Supabase write authority.
//
// Owns: dispatching write operations to operationally bounded workers.
// Does NOT own: governance policy, table validation (persist-telemetry-fsm),
//               read operations, parse/normalize logic.

const registry = require('./registry');
let _governance = null;

function setGovernance(gov) { _governance = gov; }

function dispatchWrite(operation, params) {
  const writer = registry.getWriter(operation);
  if (!writer) {
    if (_governance) {
      _governance.dispatch({
        type: 'DB_WRITE_COMPLETE',
        ...params,
        count: 0, error: `unknown_operation: ${operation}`,
      });
    }
    return;
  }
  // Async — fire and forget, worker emits completion via governance
  setImmediate(() => writer.execute(params, _governance));
}

module.exports = { dispatchWrite, setGovernance };
