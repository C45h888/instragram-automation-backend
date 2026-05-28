// control-plane/orchestration/emission-orchestrator.js
// Emission Orchestrator: constitutional coordination membrane.
//
// Owns: routing EVALUATE actions downward through the
//        evaluation → mutation → emission pipeline,
//        forwarding EMISSION_OBSERVATION upward,
//        bridging dedup FSM governance with async evaluation.
// Does NOT own: evaluation policy, publishing rules, intent construction,
//               dedup logic, emission mechanics.
//
// Constitutional purity: this orchestrator mechanically sequences
// evaluator → emitter without understanding what evaluation means.
// It never interprets policy outcomes or intent semantics.
//
// Phase 5: dedup FSM integration — orchestrator dispatches DEDUP_BATCH_BEGIN
// before evaluation, DEDUP_INTENT_MARKED / DEDUP_REPLAY_DETECTED per intent,
// and DEDUP_BATCH_END after evaluation, bridging async substrate work to
// synchronous dedup FSM governance.

const evaluator = require('../runtime/evaluation');
const emitter = require('../runtime/emission');
const dedupSubstrate = require('../../substrates/dedup-substrate');
const mutationSubstrate = require('../mutation-substrate');

const MUTATION_POLICY = {
  scheduled_posts: {
    allowedStatuses: ['publishing'],
    expectedPriorStatuses: ['approved'],
  },
  post_queue: {
    allowedStatuses: ['processing'],
    expectedPriorStatuses: ['pending', 'failed'],
  },
};

function _validateApplyMutationAction(action) {
  const { table, recordId, updates, expectedPriorStatus } = action || {};
  if (!table || !recordId || !updates || typeof updates !== 'object') {
    return { ok: false, reason: 'missing required fields' };
  }
  const policy = MUTATION_POLICY[table];
  if (!policy) {
    return { ok: false, reason: `table "${table}" is not allowed` };
  }
  const keys = Object.keys(updates);
  if (keys.length !== 1 || keys[0] !== 'status') {
    return { ok: false, reason: 'only status-only updates are allowed' };
  }
  if (!policy.allowedStatuses.includes(updates.status)) {
    return { ok: false, reason: `status "${updates.status}" is not allowed for ${table}` };
  }
  if (expectedPriorStatus && !policy.expectedPriorStatuses.includes(expectedPriorStatus)) {
    return { ok: false, reason: `expectedPriorStatus "${expectedPriorStatus}" is not allowed for ${table}` };
  }
  return { ok: true };
}

/**
 * Execute the evaluation → mutation → emission pipeline for a single account.
 * Pure mechanical sequencing — no policy interpretation.
 * After execution, reports EMISSION_OBSERVATION back to governance.
 *
 * @param {object} governance — governance kernel module
 * @param {string} accountId
 * @param {Array} events
 */
async function executeEvaluationPipeline(governance, accountId, events) {
  const startTime = Date.now();

  // Observability: evaluation pipeline state transition
  _emitTransition(accountId, 'IDLE', 'RUNNING');

  // ── Dedup FSM: open batch governance window ────────────────────────────
  governance.dispatch({
    type: 'DEDUP_BATCH_BEGIN',
    accountId,
    eventCount: events.length,
  });

  try {
    const result = await evaluator.evaluate(accountId, events);

    // ── Dedup FSM: report mark counts to governance ──────────────────────
    const { dedup: dedupMeta } = result;
    if (dedupMeta) {
      for (let i = 0; i < dedupMeta.marks; i++) {
        governance.dispatch({
          type: 'DEDUP_INTENT_MARKED',
          accountId,
          isReplay: false,
        });
      }
      for (const replay of dedupMeta.replayDetails) {
        governance.dispatch({
          type: 'DEDUP_REPLAY_DETECTED',
          accountId,
          resourceId: replay.resourceId,
          intentId: replay.intentId,
          previousIntentId: replay.previousIntentId,
        });
      }
    }

    // ── Mechanical: clear dedup identity cache after governance dispatch ──
    dedupSubstrate.clearTick();

    // ── Dedup FSM: close batch governance window ─────────────────────────
    governance.dispatch({
      type: 'DEDUP_BATCH_END',
      accountId,
    });

    for (const mut of result.mutations) {
      await emitter.emitMutation(mut);
    }

    const emitResult = result.intents.length > 0
      ? await emitter.emit(result.intents)
      : { ok: true, error: null };

    const pipelineState = result.intents.length === 0 ? 'EMPTY' : (emitResult.ok ? 'IDLE' : 'ERROR');

    governance.dispatch({
      type: 'EMISSION_OBSERVATION',
      status: result.intents.length === 0 ? 'empty' : (emitResult.ok ? 'ok' : 'error'),
      accountId,
      metadata: {
        intentCount: result.intents.length,
        mutationsApplied: result.mutations.length,
        reason: emitResult.error || null,
        latencyMs: Date.now() - startTime,
      },
    });

    // Observability: evaluation pipeline complete
    _emitTransition(accountId, 'RUNNING', pipelineState);
  } catch (err) {
    console.error(`[emission-orchestrator] Evaluation pipeline error for ${accountId}:`, err.message);

    // ── Dedup FSM: close batch even on error (graceful degradation) ──────
    dedupSubstrate.clearTick();
    governance.dispatch({
      type: 'DEDUP_BATCH_END',
      accountId,
    });

    // Observability: evaluation pipeline error
    _emitTransition(accountId, 'RUNNING', 'ERROR');
    governance.dispatch({
      type: 'EMISSION_OBSERVATION',
      status: 'error',
      accountId,
      metadata: {
        intentCount: 0,
        mutationsApplied: 0,
        reason: err.message,
        latencyMs: Date.now() - startTime,
      },
    });
  }
}

function _emitTransition(accountId, previousState, nextState) {
  try {
    const observability = require('../observability/emitters/transition-emitter');
    observability.transition({
      domain: 'emission',
      entity: 'pipeline',
      entityId: accountId,
      previousState,
      nextState,
      authority: 'emission-orchestrator',
      raw: {},
    });
  } catch (err) {
    console.warn('[emission-orchestrator] Observability transition error:', err.message);
  }
}

/**
 * Wire this orchestrator to the governance kernel.
 * Registers per-action-type subscribers for emission actions.
 *
 * @param {object} governance — governance kernel module
 */
function wire(governance) {
  governance.subscribeAction('EVALUATE', (action) => {
    executeEvaluationPipeline(governance, action.accountId, action.events);
  });

  // ── APPLY_MUTATION: DB scan emitted → mutation substrate ──────────────────
  // Handles DB_SCAN_EMITTED transition's buildActions output.
  // Calls mutation-substrate with idempotent .eq() guards.
  governance.subscribeAction('APPLY_MUTATION', async (action) => {
    const { table, recordId, updates, expectedPriorStatus, reason } = action;
    const validation = _validateApplyMutationAction(action);
    if (!validation.ok) {
      console.warn(`[emission-orchestrator] APPLY_MUTATION rejected: ${validation.reason}`, action);
      return;
    }
    try {
      await mutationSubstrate.applyMutation(table, recordId, updates, expectedPriorStatus, reason);
    } catch (err) {
      console.error('[emission-orchestrator] APPLY_MUTATION error:', err.message);
    }
  });
}

module.exports = { wire };
