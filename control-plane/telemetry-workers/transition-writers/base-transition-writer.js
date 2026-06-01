// control-plane/telemetry-workers/transition-writers/base-transition-writer.js
// Base Transition Writer: mechanical append pipe for FSM-coordinated transitions.
//
// Owns: reading from bounded domain partition of the transition log,
//       appending FSM output to canonical ledger, notifying CK for async validation.
//
// Does NOT own: semantic validation, namespace projection state, serialization,
//               intent interpretation, or any governance decision.
//
// Architectural identity:
//   This writer is a DUMB MECHANICAL PIPE. It receives FSM-coordinated output
//   (SEMANTIC_PROJECTION_TRANSITION with coordinatedBy === 'telemetry-coordination-fsm')
//   and appends it to the canonical ledger. It does not interpret, validate, or derive.
//
// Trigger model: event-driven via observability.onWrite() — zero timers.
//   onWrite fires synchronously on every _transitionLog.push().
//   Writer filters: coordinatedBy === 'telemetry-coordination-fsm' AND domain === <namespace>
//   → recordWorkerEntry() → CK.dispatch(PROJECTION_PERSISTED)
//   → returns immediately — never blocks on CK validation.
//
// Constitutional constraints:
//   - Does NOT write to namespace projection state (lineage:projection:{domain})
//   - Does NOT implement _serializeIntent — FSM's _serializeIntent is the authoritative serializer
//   - Does NOT filter by nextState === 'PROJECTION_INTENT' — only FSM output is consumed

// Transition writers are template-generated per namespace.
// Each writer receives the namespace it owns and the CK dispatch function.

// eslint-disable-next-line no-unused-vars
const NAMESPACES = ['runtime', 'integrity', 'authority', 'health', 'systemic'];

/**
 * Creates a transition writer for a given namespace.
 * Each writer subscribes to observability.onWrite() and filters for:
 *   1. transition.raw?.coordinatedBy === 'telemetry-coordination-fsm'
 *   2. transition.domain === namespace
 *
 * @param {string} namespace — bounded domain: runtime | integrity | authority | health | systemic
 * @returns {{ start: Function, stop: Function, getHealth: Function }}
 */
function createTransitionWriter(namespace) {
  let _unsubscribe = null;
  let _writeCount = 0;
  let _startedAt = null;

  function start() {
    // eslint-disable-next-line global-require
    const observability = require('../../observability');
    // eslint-disable-next-line global-require
    const lineageLedger = require('../../governance/lineage-ledger');
    // eslint-disable-next-line global-require
    const CK = require('../../governance/constitutional-kernel');

    _unsubscribe = observability.onWrite((transition) => {
      // Gate 1: Coordinated filter — FSM is the sole coordinator
      // Skip all entries without the coordinatedBy marker (PROJECTION_INTENT entries are uncoordinated)
      if (transition.raw?.coordinatedBy !== 'telemetry-coordination-fsm') return;

      // Gate 2: Domain filter — bounded to this writer's namespace
      if (transition.domain !== namespace) return;

      try {
        // Write to canonical ledger — mechanical append, no interpretation
        lineageLedger.recordWorkerEntry(transition).catch(err => {
          console.error(`[${namespace}-transition-writer] Ledger write error:`, err.message);
        });

        // Notify CK for async validation — fire-and-forget
        CK.dispatch({
          type: 'PROJECTION_PERSISTED',
          ledgerId: transition.ledgerId,
          entry: transition,
        });

        _writeCount++;
      } catch (err) {
        console.error(`[${namespace}-transition-writer] Write error:`, err.message);
      }
    });

    _startedAt = Date.now();
    console.log(`[${namespace}-transition-writer] Started — bounded to domain:${namespace}, event-driven`);
  }

  function stop() {
    if (_unsubscribe) {
      _unsubscribe();
      _unsubscribe = null;
    }
    console.log(`[${namespace}-transition-writer] Stopped — ${_writeCount} writes`);
  }

  function getHealth() {
    return {
      namespace,
      running: _unsubscribe !== null,
      writeCount: _writeCount,
      uptimeMs: _startedAt ? Date.now() - _startedAt : 0,
    };
  }

  return { start, stop, getHealth };
}

module.exports = { createTransitionWriter, NAMESPACES };