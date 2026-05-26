// control-plane/orchestration/emission-orchestrator.js
// Emission Orchestrator: constitutional coordination membrane.
//
// Owns: routing EVALUATE actions downward through the
//        evaluation → mutation → emission pipeline,
//        forwarding EMISSION_OBSERVATION upward.
// Does NOT own: evaluation policy, publishing rules, intent construction,
//               dedup logic, emission mechanics.
//
// Constitutional purity: this orchestrator mechanically sequences
// evaluator → emitter without understanding what evaluation means.
// It never interprets policy outcomes or intent semantics.

const evaluator = require('../runtime/evaluation');
const emitter = require('../runtime/emission');

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

  try {
    const result = await evaluator.evaluate(accountId, events);

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
}

module.exports = { wire };
