// control-plane/runtime/evaluation.js
// Evaluation: bounded policy evaluation with dedup and mutation classification.
//
// Owns: applying publishing policy to buffered events, dedup checking,
//        classifying mutations (mark_failed).
// Does NOT own: intent emission, Redis, worker lifecycle, signal intake.
//
// Contract:
//   evaluator.evaluate(accountId, events) → Promise<{ intents: [...], mutations: [...], dedup: { marks, replays, duplicates } }>
//
// Dedup is Redis-backed — must be async to check/set Redis keys.
// Caller handles emission, mutation execution, and governance dispatch.
//
// Phase 4a: lineage-aware dedup — each intent carries a unique intentId.
// Dedup now distinguishes duplicate (same intentId, block) from replay
// (different intentId touching same resource, allow with observability signal).
//
// Phase 5: dedup FSM governance — evaluation returns dedup metadata for the
// orchestrator to dispatch DEDUP_BATCH_BEGIN / DEDUP_INTENT_MARKED /
// DEDUP_REPLAY_DETECTED / DEDUP_BATCH_END through the constitutional kernel.
// clearTick() is now called by the orchestrator after governance dispatch.

const crypto = require('crypto');
const publishingPolicy = require('../policies/publishing');
const dedupSubstrate = require('../../substrates/dedup-substrate');

// ── Observability state tracking ────────────────────────────────────────────

let _evalState = 'IDLE';

/**
 * Evaluates a batch of events for one account.
 * Async — dedup checks and marks require Redis-backed idempotency.
 * Always returns a valid shape even for empty input.
 *
 * @param {string} accountId — non-empty string
 * @param {Array<{table: string, record: object}>} events — array of DB events
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

  // ── Dedup governance metadata — collected for orchestrator dispatch ─────
  const dedupMeta = {
    marks: 0,
    replays: 0,
    duplicates: 0,
    replayDetails: [], // [{ resourceId, intentId, previousIntentId }]
  };

  for (const { table, record } of events) {
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
      });
      continue;
    }

    if (outcome.action === 'emit') {
      const { intent } = outcome;
      const resourceId = record.id;
      const intentId = crypto.randomUUID();

      // Lineage-aware dedup check — structured result distinguishes duplicate from replay
      const dedupResult = await dedupSubstrate.isInFlight(accountId, intent.action_type, resourceId, intentId);

      if (dedupResult.blocked && dedupResult.reason === 'duplicate') {
        dedupMeta.duplicates++;
        continue;
      }

      // Substrate marks in-flight (mechanical operation — governance recorded by orchestrator)
      await dedupSubstrate.markInFlight(accountId, intent.action_type, resourceId, { intentId });
      dedupMeta.marks++;

      if (dedupResult.reason === 'replay') {
        dedupMeta.replays++;
        dedupMeta.replayDetails.push({
          resourceId,
          intentId,
          previousIntentId: dedupResult.existingIntentId,
        });
      }

      intents.push({
        intent_id: intentId,
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

  // clearTick() is now called by the orchestrator after governance dispatch

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

  return { intents, mutations, dedup: dedupMeta };
}

module.exports = { evaluate };
