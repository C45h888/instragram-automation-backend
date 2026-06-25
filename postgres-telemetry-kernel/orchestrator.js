// postgres-telemetry-kernel/orchestrator.js
// Persist Telemetry Orchestrator: constitutional coordination membrane.
//
// Owns: routing EXECUTE_DB_READ / EXECUTE_DB_WRITE actions from the FSM
//       through bounded substrates.
// Does NOT own: event emission, FSM state transitions, read/write policy,
//               whitelist validation, backpressure tracking, idempotency keys.
//
// Constitutional purity: this orchestrator mechanically dispatches reads and
// writes without understanding what the data means. It translates FSM actions
// into bounded I/O and passes raw results back to the FSM for emission.
//
// Semantic authority: the FSM owns emission (receiveWriteResult / receiveReadResult).
// The orchestrator is a bounded executor — it calls I/O, passes results back.
//
// Flow (canonical):
//   FSM buildActions → [{ type: 'EXECUTE_DB_READ', ... }]
//     → CK emits action → subscribeAction('EXECUTE_DB_READ') fires
//     → orchestrator calls readingSubstrate.executeRead()
//     → orchestrator calls fsm.receiveReadResult(action, result)
//       → FSM dispatches DB_READ_COMPLETE through governance
//       → CK routes DB_READ_COMPLETE to FSM handler
//       → FSM handles completion, emits READ_RESULT_AVAILABLE to calling domain

const readingSubstrate = require('../control-plane/governance/domains/reading-substrate');
const writers = require('./writers');

// ── Governance + FSM references — set by wire() ─────────────────────────────
let _governance = null;
let _fsm = null;

/**
 * Wire this orchestrator to the governance kernel and FSM.
 * Registers per-action-type subscribers for persist-telemetry actions.
 * The FSM reference is used to pass raw I/O results back — the FSM owns emission.
 */
function wire(governance, fsm) {
  if (!governance || typeof governance.dispatch !== 'function') {
    throw new Error('[persist-telemetry-orchestrator] wire() requires a governance object with dispatch()');
  }
  if (!fsm || typeof fsm.receiveWriteResult !== 'function' || typeof fsm.receiveReadResult !== 'function') {
    throw new Error('[persist-telemetry-orchestrator] wire() requires an FSM with receiveWriteResult() and receiveReadResult()');
  }
  _governance = governance;
  _fsm = fsm;

  // ── EXECUTE_DB_READ: FSM validated the read, orchestrator executes it ───────
  // FSM (DB_READ_REQUESTED) has already done: whitelist guard, in-flight tracking,
  // pending-read registration. This handler owns the actual read execution and
  // passes the raw result back to the FSM for emission.
  _governance.subscribeAction('EXECUTE_DB_READ', async (action) => {
    const { readDomain, accountId, readId, params } = action;

    if (!readDomain || !accountId || !readId) {
      console.warn('[persist-telemetry-orchestrator] EXECUTE_DB_READ rejected: missing required fields', action);
      return;
    }

    try {
      const result = await readingSubstrate.executeRead(readDomain, params, readId);
      // Pass raw result to FSM — FSM owns emission of DB_READ_COMPLETE
      _fsm.receiveReadResult(action, result);
    } catch (err) {
      // Hard error — reading substrate threw. Pass error result to FSM.
      _fsm.receiveReadResult(action, {
        success: false,
        data: null,
        error: err.message,
        latencyMs: 0,
        cached: false,
      });
    }
  });

  // ── EXECUTE_DB_WRITE: FSM validated the write, orchestrator executes it ─────
  // FSM (DB_WRITE_REQUESTED) has already done: table whitelist, sanity gate,
  // in-flight tracking, idempotency key generation. This handler owns the
  // actual write execution through the domain writer and passes the raw result
  // back to the FSM for emission.
  _governance.subscribeAction('EXECUTE_DB_WRITE', async (action) => {
    const { domain, accountId, intentId, table, operation, rows, idempotencyKey } = action;

    if (!domain || !accountId || !intentId || !table) {
      console.warn('[persist-telemetry-orchestrator] EXECUTE_DB_WRITE rejected: missing required fields', action);
      return;
    }

    const writer = writers.getWriter(domain);

    if (!writer) {
      // No writer registered for this domain — pass failure result to FSM.
      _fsm.receiveWriteResult(action, {
        count: 0,
        error: `no_writer_for_domain: ${domain}`,
      });
      return;
    }

    // Build ctx bridge for writers that call ctx.emit() internally.
    // writer.execute() receives (event, ctx) where ctx.emit → governance.dispatch.
    const ctx = {
      emit: (event) => {
        _governance.dispatch(event);
      },
    };

    try {
      const result = await writer.execute(action, ctx);

      // writer.execute() is responsible for emitting DB_WRITE_COMPLETE or
      // DB_WRITE_FAILED via ctx.emit(). If it returned without emitting (should
      // not happen with current writers), pass the result to the FSM here.
      // This path exists to prevent silent black holes if a writer is misbehaving.
      if (result && !result.emitted) {
        _fsm.receiveWriteResult(action, result);
      }
    } catch (err) {
      // Hard error — writer threw without emitting a completion event.
      _fsm.receiveWriteResult(action, {
        count: 0,
        error: err.message,
      });
    }
  });

  console.log('[persist-telemetry-orchestrator] wired to governance + FSM');
}

module.exports = { wire };
