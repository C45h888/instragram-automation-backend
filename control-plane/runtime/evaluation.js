// control-plane/runtime/evaluation.js
// Evaluation: bounded policy evaluation — pure policy, no dedup.
//
// Owns: applying publishing policy to buffered events, classifying mutations.
// Does NOT own: dedup checking (dedup-kernel/fsm via CK), intent emission,
//               Redis, worker lifecycle, signal intake.
//
// Contract:
//   evaluator.evaluate(accountId, events) → Promise<{ intents: [...], mutations: [...] }>
//
// Dedup is handled by the publishing orchestrator BEFORE evaluation.
// The orchestrator pre-filters events through CK → dedup FSM, then passes
// only clean (non-blocked) events to evaluator.evaluate().
//
// Phase 5 (2026-06-07): Dedup extracted to dedup-kernel FSM.
// evaluation.js is now pure policy evaluation — no dedup substrate import.

const crypto = require('crypto');
const publishingPolicy = require('../policies/publishing');

// ── Observability state tracking ────────────────────────────────────────────

let _evalState = 'IDLE';

/**
 * Evaluates a batch of events for one account.
 * Async — policy evaluation is synchronous but observability emissions are
 * fire-and-forget.
 *
 * @param {string} accountId — non-empty string
 * @param {Array<{table: string, record: object, intentId?: string}>} events — pre-filtered DB events (dedup already checked)
 *        Optional intentId: if provided, reused; otherwise generated fresh.
 * @returns {Promise<{ intents: Array<object>, mutations: Array<object> }>}
 * @throws {Error} if accountId is not a string or events is not an array
 */
async function evaluate(accountId, events) {
  if (typeof accountId !== 'string' || !accountId) {
    throw new Error(`[evaluation] accountId must be a non-empty string, got ${typeof accountId}`);
  }
  if (!Array.isArray(events)) {
    throw new Error(`[evaluation] events must be an array, got ${typeof events}`);
  }

  // Emit EVALUATING transition when evaluation begins
  if (_evalState === 'IDLE') {
    _evalState = 'EVALUATING';
    try {
      const observability = require('../observability/emitters/transition-emitter');
      observability.transition({
        domain: 'evaluation',
        entity: 'evaluator',
        entityId: accountId,
        previousState: 'IDLE',
        nextState: 'EVALUATING',
        authority: 'evaluation',
        raw: { eventCount: events.length },
      });
    } catch (_) {}
  }

  const intents = [];
  const mutations = [];

  for (const { table, record, intentId } of events) {
    const outcome = publishingPolicy.evaluateRecord(table, record);

    if (outcome.action === 'skip') {
      continue;
    }

    if (outcome.action === 'mark_failed') {
      mutations.push({
        table,
        id: record.id,
        updates: outcome.updates,
        reason: outcome.reason,
        intentId: intentId || null,
      });
      continue;
    }

    if (outcome.action === 'emit') {
      const { intent } = outcome;
      const resourceId = record.id;
      // intentId is pre-allocated by _preFilterDedup (on the outer event object
      // from the loop variable { table, record, intentId }). Reuse it to ensure
      // the same intentId used at intake dedup check is used in the intent.
      // If missing (non-orchestrator caller), generate fresh.
      const intentId_out = intentId || crypto.randomUUID();

      // Dedup already checked by publishing orchestrator via CK → dedup FSM.
      // Events reaching here have passed dedup. No isInFlight/markInFlight.

      intents.push({
        intent_id: intentId_out,
        account_id: accountId,
        action_type: intent.action_type,
        resource_id: resourceId,
        payload: intent.payload,
        queue_row_id: intent.queue_row_id || null,
        scheduled_post_id: intent.scheduled_post_id || null,
        intent_type: table === 'scheduled_posts' ? 'scheduled_post' : 'post_queue',
      });
    }
  }

  // Emit IDLE transition when evaluation completes
  if (_evalState !== 'IDLE') {
    _evalState = 'IDLE';
    try {
      const observability = require('../observability/emitters/transition-emitter');
      observability.transition({
        domain: 'evaluation',
        entity: 'evaluator',
        entityId: accountId,
        previousState: 'EVALUATING',
        nextState: 'IDLE',
        authority: 'evaluation',
        raw: { intentsEmitted: intents.length, mutationsApplied: mutations.length },
      });
    } catch (_) {}
  }

  return { intents, mutations };
}

module.exports = { evaluate };
