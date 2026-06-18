// postgres-telemetry-kernel/orchestrator.js
// Persist Telemetry Orchestrator: constitutional coordination membrane.
//
// Owns: routing EXECUTE_DB_READ / EXECUTE_DB_WRITE actions from the FSM
//       through bounded substrates, emitting completion events through CK.
// Does NOT own: FSM state transitions, read/write policy, whitelist validation,
//               backpressure tracking, idempotency key generation.
//
// Constitutional purity: this orchestrator mechanically dispatches reads and
// writes without understanding what the data means. It translates FSM actions
// into bounded I/O and emits normalized outcomes.
//
// Flow (canonical):
//   FSM buildActions → ctx.dispatchGlobal({ type: 'EXECUTE_DB_READ', ... })
//     → CK emits actions → subscribeAction('EXECUTE_DB_READ') fires
//     → orchestrator calls readingSubstrate.executeRead()
//     → orchestrator dispatches DB_READ_COMPLETE through CK
//     → CK routes DB_READ_COMPLETE to persist-telemetry FSM
//     → FSM handles completion, emits READ_RESULT_AVAILABLE to calling domain
//
//   FSM buildActions → ctx.dispatchGlobal({ type: 'EXECUTE_DB_WRITE', ... })
//     → CK emits actions → subscribeAction('EXECUTE_DB_WRITE') fires
//     → orchestrator calls writer.execute(event, ctx)
//     → writer calls ctx.emit(DB_WRITE_COMPLETE / DB_WRITE_FAILED)
//     → orchestrator dispatches completion through CK
//     → CK routes to FSM for state transition

const readingSubstrate = require('../control-plane/governance/domains/reading-substrate');
const writers = require('./writers');

// ── Governance reference — set by wire() ─────────────────────────────────────
let _governance = null;

/**
 * Wire this orchestrator to the governance kernel.
 * Registers per-action-type subscribers for persist-telemetry actions.
 * Called at boot via orchestator.js wire() in control-plane/orchastrator.js.
 */
function wire(governance) {
  if (!governance || typeof governance.dispatch !== 'function') {
    throw new Error('[persist-telemetry-orchestrator] wire() requires a governance object with dispatch()');
  }
  _governance = governance;

  // ── EXECUTE_DB_READ: FSM validated the read, orchestrator executes it ───────
  // FSM (DB_READ_REQUESTED) has already done: whitelist guard, in-flight tracking,
  // pending-read registration. This handler owns the actual read execution.
  _governance.subscribeAction('EXECUTE_DB_READ', async (action) => {
    const { readDomain, accountId, readId, params } = action;

    if (!readDomain || !accountId || !readId) {
      console.warn('[persist-telemetry-orchestrator] EXECUTE_DB_READ rejected: missing required fields', action);
      return;
    }

    try {
      const result = await readingSubstrate.executeRead(readDomain, params, readId);

      // Emit DB_READ_COMPLETE through CK — success and failure both flow through here.
      // CK routes DB_READ_COMPLETE to persist-telemetry FSM for state transition
      // and READ_RESULT_AVAILABLE forwarding to the calling domain.
      _governance.dispatch({
        type: 'DB_READ_COMPLETE',
        readDomain,
        accountId,
        readId,
        success: result.success,
        data: result.data || null,
        error: result.error || null,
        latencyMs: result.latencyMs || 0,
        cached: result.cached || false,
      });
    } catch (err) {
      // Hard error — reading substrate threw. Emit failure through CK.
      _governance.dispatch({
        type: 'DB_READ_COMPLETE',
        readDomain,
        accountId,
        readId,
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
  // actual write execution through the domain writer.
  _governance.subscribeAction('EXECUTE_DB_WRITE', async (action) => {
    const { domain, accountId, intentId, table, operation, rows, idempotencyKey } = action;

    if (!domain || !accountId || !intentId || !table) {
      console.warn('[persist-telemetry-orchestrator] EXECUTE_DB_WRITE rejected: missing required fields', action);
      return;
    }

    const writer = writers.getWriter(domain);

    if (!writer) {
      // No writer registered for this domain — emit failure and return.
      // The FSM's DB_WRITE_REQUESTED guard should have caught unknown domains,
      // but we handle it here as a defensive layer.
      console.warn(`[persist-telemetry-orchestrator] EXECUTE_DB_WRITE: no writer for domain="${domain}"`);
      _governance.dispatch({
        type: 'DB_WRITE_COMPLETE',
        domain,
        accountId,
        intentId,
        table,
        count: 0,
        status: 'failed',
        error: `no_writer_for_domain: ${domain}`,
        authority: 'persist-telemetry-orchestrator',
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
      // not happen with current writers), emit a defensive completion here.
      // This path exists to prevent silent black holes if a writer is misbehaving.
      if (result && !result.emitted) {
        _governance.dispatch({
          type: 'DB_WRITE_COMPLETE',
          domain,
          accountId,
          intentId,
          table,
          count: result.count || 0,
          status: result.success ? 'completed' : 'failed',
          error: result.error || null,
          authority: 'persist-telemetry-orchestrator',
        });
      }
    } catch (err) {
      // Hard error — writer threw without emitting a completion event.
      console.error(`[persist-telemetry-orchestrator] EXECUTE_DB_WRITE threw for ${domain}/${table}:`, err.message);
      _governance.dispatch({
        type: 'DB_WRITE_COMPLETE',
        domain,
        accountId,
        intentId,
        table,
        count: 0,
        status: 'failed',
        error: err.message,
        authority: 'persist-telemetry-orchestrator',
      });
    }
  });

  console.log('[persist-telemetry-orchestrator] wired to governance');
}

module.exports = { wire };
