// publishing-kernel/orchestrator.js
// Emission Orchestrator: constitutional coordination membrane.
//
// Owns: routing EVALUATE actions downward through the
//        evaluation → mutation → emission pipeline,
//        executing EXECUTE_CONTENT / EXECUTE_ENGAGEMENT via bounded substrates,
//        forwarding PUBLISHING_OBSERVATION upward,
//        bridging dedup FSM governance with async evaluation.
// Does NOT own: evaluation policy, publishing rules, intent construction,
//               dedup logic, emission mechanics, retry policy, circuit breaker.
//
// Constitutional purity: this orchestrator mechanically sequences
// evaluator → emitter without understanding what evaluation means.
// It never interprets policy outcomes or intent semantics.
//
// Phase 5: dedup FSM integration — orchestrator dispatches DEDUP_BATCH_BEGIN
// before evaluation, DEDUP_INTENT_MARKED / DEDUP_REPLAY_DETECTED per intent,
// and DEDUP_BATCH_END after evaluation, bridging async substrate work to
// synchronous dedup FSM governance.

const evaluator = require('../control-plane/runtime/evaluation');
const emitter = require('../control-plane/runtime/emission');
const dedupSubstrate = require('../dedup-kernel/substrates/dedup');
const mutationSubstrate = require('../control-plane/mutation-substrate');
const contentSubstrate = require('./substrates/content');
const engagementSubstrate = require('./substrates/engagement');
const publishErrorParser = require('../retry-cadence-kernel/workers/publish-error-parser');
// NOTE (Step 7): The emission-orchestrator no longer imports the
// credential-resolver. The publish substrates resolve their own
// credentials internally. The orchastrator's role is dispatch
// only — no credential handling.

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
 */
async function executeEvaluationPipeline(governance, accountId, events) {
  const startTime = Date.now();

  _emitTransition(accountId, 'IDLE', 'RUNNING');

  governance.dispatch({
    type: 'DEDUP_BATCH_BEGIN',
    accountId,
    eventCount: events.length,
  });

  try {
    const result = await evaluator.evaluate(accountId, events);

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

    dedupSubstrate.clearTick();

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

    _emitTransition(accountId, 'RUNNING', pipelineState);
  } catch (err) {
    console.error(`[emission-orchestrator] Evaluation pipeline error for ${accountId}:`, err.message);

    dedupSubstrate.clearTick();
    governance.dispatch({ type: 'DEDUP_BATCH_END', accountId });

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

// ═══════════════════════════════════════════════════════════════════════════════
// Bounded publish execution — thin delegate to substrate
// Substrate owns: worker factory, rate limiter, error→CK signal routing.
// Orchestrator owns: credential resolution, mechanical delegation.
// ═══════════════════════════════════════════════════════════════════════════════

function _emitTransition(accountId, previousState, nextState) {
  try {
    const observability = require('../control-plane/observability/emitters/transition-emitter');
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
 */
function wire(governance) {
  governance.subscribeAction('EVALUATE', (action) => {
    executeEvaluationPipeline(governance, action.accountId, action.events);
  });

  // ── GOVERNED_READ: publishing-fsm → CK → persist-telemetry-fsm → reading-substrate ──
  governance.subscribeAction('GOVERNED_READ', (action) => {
    const { readDomain, accountId, readId, params } = action;
    if (!readDomain || !accountId || !readId) {
      console.warn('[emission-orchestrator] GOVERNED_READ rejected: missing required fields', action);
      return;
    }
    governance.dispatch({
      type: 'DB_READ_REQUESTED',
      readDomain,
      accountId,
      readId,
      params: params || { accountId },
    });
  });

  // ── EXECUTE_CONTENT: publishing-fsm → bounded content substrate → IG API
  //    Failure path: dual emission (Step 6).
  //      - WORKER_OUTCOME_REPORTED → engagement-fsm (for retry path)
  //      - PUBLISH_FAILURE → observability subscribers (lineage,
  //        alerting) — does NOT enter a FSM.
  //    The workers' emissions are FSM-only. The orchestrator's
  //    emissions are observability-only. Two emitters, two audiences.
  governance.subscribeAction('EXECUTE_CONTENT', async (action) => {
    const { accountId, items, intentId, domain } = action;
    if (!accountId || !items || !Array.isArray(items)) {
      console.warn('[emission-orchestrator] EXECUTE_CONTENT rejected: missing fields', action);
      return;
    }
    const publishDomain = domain || 'publish:post';
    try {
      // Step 7: substrate resolves its own credentials internally.
      // Orchastrator does not touch credentials.
      const result = await contentSubstrate.execute(accountId, items, governance);
      if (result && !result.success) {
        // Substrate returned a structured failure
        const errorShape = publishErrorParser.parse(result, publishDomain);
        governance.dispatch({
          type: 'WORKER_OUTCOME_REPORTED',
          accountId,
          intentId: intentId || null,
          domain: publishDomain,
          status: 'failed',
          errorShape,
          error: result.error,
        });
        // Observability emission (does not route to a FSM)
        governance.dispatch({
          type: 'PUBLISH_FAILURE',
          accountId,
          domain: publishDomain,
          intentId: intentId || null,
          reason: result.error,
        });
      }
    } catch (err) {
      // Substrate threw — wrap into errorShape
      const errorShape = publishErrorParser.parseError(err, publishDomain);
      governance.dispatch({
        type: 'WORKER_OUTCOME_REPORTED',
        accountId,
        intentId: intentId || null,
        domain: publishDomain,
        status: 'failed',
        errorShape,
        error: err.message,
      });
      // Observability emission
      governance.dispatch({
        type: 'PUBLISH_FAILURE',
        accountId,
        domain: publishDomain,
        intentId: intentId || null,
        reason: err.message,
      });
    }
  });

  // ── EXECUTE_ENGAGEMENT: publishing-fsm → bounded engagement substrate → IG API
  //    Same dual-emission pattern as EXECUTE_CONTENT.
  governance.subscribeAction('EXECUTE_ENGAGEMENT', async (action) => {
    const { accountId, items, intentId, domain } = action;
    if (!accountId || !items || !Array.isArray(items)) {
      console.warn('[emission-orchestrator] EXECUTE_ENGAGEMENT rejected: missing fields', action);
      return;
    }
    const publishDomain = domain || 'publish:comment';
    try {
      // Step 7: substrate resolves its own credentials internally.
      const result = await engagementSubstrate.execute(accountId, items, governance);
      if (result && !result.success) {
        const errorShape = publishErrorParser.parse(result, publishDomain);
        governance.dispatch({
          type: 'WORKER_OUTCOME_REPORTED',
          accountId,
          intentId: intentId || null,
          domain: publishDomain,
          status: 'failed',
          errorShape,
          error: result.error,
        });
        governance.dispatch({
          type: 'PUBLISH_FAILURE',
          accountId,
          domain: publishDomain,
          intentId: intentId || null,
          reason: result.error,
        });
      }
    } catch (err) {
      const errorShape = publishErrorParser.parseError(err, publishDomain);
      governance.dispatch({
        type: 'WORKER_OUTCOME_REPORTED',
        accountId,
        intentId: intentId || null,
        domain: publishDomain,
        status: 'failed',
        errorShape,
        error: err.message,
      });
      governance.dispatch({
        type: 'PUBLISH_FAILURE',
        accountId,
        domain: publishDomain,
        intentId: intentId || null,
        reason: err.message,
      });
    }
  });

  // ── APPLY_MUTATION: DB scan emitted → mutation substrate ──────────────────
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
