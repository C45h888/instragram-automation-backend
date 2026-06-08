// publishing-kernel/orchestrator.js
// Emission Orchestrator: constitutional coordination membrane.
//
// Owns: routing EVALUATE actions downward through the
//        dedup pre-filter → evaluation → mutation → emission pipeline,
//        executing EXECUTE_CONTENT / EXECUTE_ENGAGEMENT via bounded substrates,
//        forwarding PUBLISHING_OBSERVATION upward.
// Does NOT own: evaluation policy, publishing rules, intent construction,
//               dedup logic, emission mechanics, retry policy, circuit breaker.
//
// Constitutional purity: this orchestrator mechanically sequences
// dedup → evaluator → emitter without understanding what any step means.
// It never interprets policy outcomes or intent semantics.
//
// Phase 6 (2026-06-07): Dedup pre-filter added. Events are checked through
// CK → dedup FSM BEFORE evaluation. The FSM is the intelligence layer;
// the orchestrator is a mechanical router.
//
// Flow:
//   raw events → pre-filter (CK → dedup FSM: CHECK_AND_MARK_DEDUP)
//     → clean events → evaluator.evaluate() → intents + mutations
//     → DEDUP_BATCH_END (FSM handles clearTick internally)
//     → emitter.emit(intents) → EMISSION_OBSERVATION

const crypto = require('crypto');
const evaluator = require('../control-plane/runtime/evaluation');
const emitter = require('../control-plane/runtime/emission');
const publishingPolicy = require('../control-plane/policies/publishing');
const mutationSubstrate = require('../control-plane/mutation-substrate');
const contentSubstrate = require('./substrates/content');
const engagementSubstrate = require('./substrates/engagement');
const publishErrorParser = require('../retry-cadence-kernel/workers/publish-error-parser');
// NOTE (Step 7): The emission-orchestrator no longer imports the
// credential-resolver. The publish substrates resolve their own
// credentials internally.
// NOTE (Phase 6): The emission-orchestrator no longer imports the
// dedup substrate. Dedup flows through CK → dedup FSM.

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
 * Pre-filter events through CK → dedup FSM before evaluation.
 * Each event is dispatched as CHECK_AND_MARK_DEDUP to CK.
 * The dedup FSM calls workers, marks in-flight, and returns the result.
 * Blocked events (duplicates) are filtered out.
 * Clean events get pre-allocated intentIds that the evaluator reuses.
 *
 * @returns {Promise<Array<{ table, record, intentId }>>} clean events
 */
async function _preFilterDedup(governance, accountId, events) {
  const clean = [];
  for (const { table, record } of events) {
    const outcome = publishingPolicy.evaluateRecord(table, record);
    if (outcome.action === 'skip') {
      continue;
    }
    if (outcome.action === 'mark_failed') {
      // Mark-failed mutations bypass dedup — they don't create intents.
      // Pass through with null intentId.
      clean.push({ table, record, intentId: null, _markFailed: true });
      continue;
    }
    if (outcome.action !== 'emit') {
      continue;
    }

    const { intent } = outcome;
    const resourceId = record.id;
    const intentId = crypto.randomUUID();
    const actionType = intent.action_type;

    // Dispatch to CK → dedup FSM → check + mark
    const result = await governance.dispatch({
      type: 'CHECK_AND_MARK_DEDUP',
      accountId,
      actionType,
      resourceId,
      intentId,
    });

    // CK.dispatch returns { allowed, actions }. The actions array contains
    // [{ type: 'DEDUP_INTENT_CHECKED', blocked, reason, ... }]
    const checked = result?.actions?.find(a => a.type === 'DEDUP_INTENT_CHECKED');
    if (checked?.blocked) {
      // Duplicate — drop this event
      continue;
    }

    clean.push({ table, record, intentId });
  }
  return clean;
}

/**
 * Execute the evaluation → mutation → emission pipeline for a single account.
 * Phase 6: dedup pre-filter runs BEFORE evaluation.
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
    // ── Phase 6: Pre-filter through dedup FSM ──────────────────────────
    const cleanEvents = await _preFilterDedup(governance, accountId, events);

    // ── Evaluate only clean, non-blocked events ──────────────────────
    const result = await evaluator.evaluate(accountId, cleanEvents);

    // ── Close batch — FSM handles clearTick internally ───────────────
    governance.dispatch({
      type: 'DEDUP_BATCH_END',
      accountId,
    });

    for (const mut of result.mutations) {
      // Phase 8: Mutation dedup gate — check + mark before applying.
      // Belt-and-suspenders: checks Layer 1 (intake) key AND Layer 2 (mutation) key.
      // On duplicate: skip mutation, emit degraded observability.
      const mutDedup = await governance.dispatch({
        type: 'CHECK_MUTATION_DEDUP',
        accountId,
        actionType: mut.table,    // table as actionType for mutation dedup
        resourceId: mut.id,
        intentId: mut.intentId || mut.id,
      });
      const mutBlocked = mutDedup?.actions?.find(a => a.type === 'DEDUP_MUTATION_BLOCKED');
      if (mutBlocked) {
        governance.dispatch({
          type: 'LOG_DEGRADED',
          substate: 'MUTATION_DEDUP_BLOCKED',
          reason: `Mutation dedup blocked ${mut.table}.${mut.id}: ${mutBlocked.reason}`,
          domain: 'mutation',
          intentId: mut.intentId || null,
        });
        continue; // skip the mutation
      }
      await emitter.emitMutation(mut, mut.intentId);
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
        dedupFiltered: events.length - cleanEvents.length,
      },
    });

    _emitTransition(accountId, 'RUNNING', pipelineState);
  } catch (err) {
    console.error(`[emission-orchestrator] Evaluation pipeline error for ${accountId}:`, err.message);

    // Close batch on error — FSM handles clearTick internally
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
  governance.subscribeAction('EXECUTE_CONTENT', async (action) => {
    const { accountId, items, intentId: batchIntentId, domain } = action;
    if (!accountId || !items || !Array.isArray(items)) {
      console.warn('[emission-orchestrator] EXECUTE_CONTENT rejected: missing fields', action);
      return;
    }
    const publishDomain = domain || 'publish:post';

    // Phase 8: Emission dedup gate — check each item before IG API call.
    // Belt-and-suspenders: checks Layer 1 (intake) key AND Layer 3 (emission) key.
    const dedupedItems = [];
    for (const item of items) {
      const { record, actionType, intentId: itemIntentId } = item;
      const emitIntentId = itemIntentId || record?.id;
      const emitActionType = actionType || 'publish_post';
      const emitResourceId = record?.id;

      if (!emitResourceId) { dedupedItems.push(item); continue; }

      const emitDedup = await governance.dispatch({
        type: 'CHECK_EMISSION_DEDUP',
        accountId,
        actionType: emitActionType,
        resourceId: emitResourceId,
        intentId: emitIntentId,
      });
      const emitBlocked = emitDedup?.actions?.find(a => a.type === 'DEDUP_EMISSION_BLOCKED');
      if (emitBlocked) {
        governance.dispatch({
          type: 'LOG_DEGRADED',
          substate: 'EMISSION_DEDUP_BLOCKED',
          reason: `Emission dedup blocked ${emitActionType} ${emitResourceId}: ${emitBlocked.reason}`,
          domain: publishDomain,
          intentId: emitIntentId || null,
        });
        continue; // skip this item
      }
      dedupedItems.push(item);
    }

    if (dedupedItems.length === 0) return;

    try {
      const result = await contentSubstrate.execute(accountId, dedupedItems, governance);
      if (result && !result.success) {
        const errorShape = publishErrorParser.parse(result, publishDomain);
        governance.dispatch({
          type: 'WORKER_OUTCOME_REPORTED',
          accountId,
          intentId: batchIntentId || null,
          domain: publishDomain,
          status: 'failed',
          errorShape,
          error: result.error,
        });
        governance.dispatch({
          type: 'PUBLISH_FAILURE',
          accountId,
          domain: publishDomain,
          intentId: batchIntentId || null,
          reason: result.error,
        });
      }
    } catch (err) {
      const errorShape = publishErrorParser.parseError(err, publishDomain);
      governance.dispatch({
        type: 'WORKER_OUTCOME_REPORTED',
        accountId,
        intentId: batchIntentId || null,
        domain: publishDomain,
        status: 'failed',
        errorShape,
        error: err.message,
      });
      governance.dispatch({
        type: 'PUBLISH_FAILURE',
        accountId,
        domain: publishDomain,
        intentId: batchIntentId || null,
        reason: err.message,
      });
    }
  });

  // ── EXECUTE_ENGAGEMENT: publishing-fsm → bounded engagement substrate → IG API
  governance.subscribeAction('EXECUTE_ENGAGEMENT', async (action) => {
    const { accountId, items, intentId: batchIntentId, domain } = action;
    if (!accountId || !items || !Array.isArray(items)) {
      console.warn('[emission-orchestrator] EXECUTE_ENGAGEMENT rejected: missing fields', action);
      return;
    }
    const publishDomain = domain || 'publish:comment';

    // Phase 8: Emission dedup gate — check each item before IG API call.
    // Belt-and-suspenders: checks Layer 1 (intake) key AND Layer 3 (emission) key.
    const dedupedItems = [];
    for (const item of items) {
      const { record, actionType, intentId: itemIntentId } = item;
      const emitIntentId = itemIntentId || record?.id;
      const emitActionType = actionType || 'reply_comment';
      const emitResourceId = record?.id;

      if (!emitResourceId) { dedupedItems.push(item); continue; }

      const emitDedup = await governance.dispatch({
        type: 'CHECK_EMISSION_DEDUP',
        accountId,
        actionType: emitActionType,
        resourceId: emitResourceId,
        intentId: emitIntentId,
      });
      const emitBlocked = emitDedup?.actions?.find(a => a.type === 'DEDUP_EMISSION_BLOCKED');
      if (emitBlocked) {
        governance.dispatch({
          type: 'LOG_DEGRADED',
          substate: 'EMISSION_DEDUP_BLOCKED',
          reason: `Emission dedup blocked ${emitActionType} ${emitResourceId}: ${emitBlocked.reason}`,
          domain: publishDomain,
          intentId: emitIntentId || null,
        });
        continue; // skip this item
      }
      dedupedItems.push(item);
    }

    if (dedupedItems.length === 0) return;

    try {
      const result = await engagementSubstrate.execute(accountId, dedupedItems, governance);
      if (result && !result.success) {
        const errorShape = publishErrorParser.parse(result, publishDomain);
        governance.dispatch({
          type: 'WORKER_OUTCOME_REPORTED',
          accountId,
          intentId: batchIntentId || null,
          domain: publishDomain,
          status: 'failed',
          errorShape,
          error: result.error,
        });
        governance.dispatch({
          type: 'PUBLISH_FAILURE',
          accountId,
          domain: publishDomain,
          intentId: batchIntentId || null,
          reason: result.error,
        });
      }
    } catch (err) {
      const errorShape = publishErrorParser.parseError(err, publishDomain);
      governance.dispatch({
        type: 'WORKER_OUTCOME_REPORTED',
        accountId,
        intentId: batchIntentId || null,
        domain: publishDomain,
        status: 'failed',
        errorShape,
        error: err.message,
      });
      governance.dispatch({
        type: 'PUBLISH_FAILURE',
        accountId,
        domain: publishDomain,
        intentId: batchIntentId || null,
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
