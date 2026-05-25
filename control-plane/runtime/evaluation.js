// control-plane/runtime/evaluation.js
// Evaluation: bounded policy evaluation with dedup and mutation classification.
//
// Owns: applying publishing policy to buffered events, dedup checking,
//        classifying mutations (mark_failed).
// Does NOT own: intent emission, Redis, worker lifecycle, signal intake.
//
// Contract:
//   evaluator.evaluate(accountId, events) → Promise<{ intents: [...], mutations: [...] }>
//
// Dedup is Redis-backed — must be async to check/set Redis keys.
// Caller handles emission and mutation execution.

const publishingPolicy = require('../policies/publishing');
const dedupSubstrate = require('../../substrates/dedup-substrate');

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

  const intents = [];
  const mutations = [];

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

      if (await dedupSubstrate.isInFlight(accountId, intent.action_type, resourceId)) {
        continue;
      }
      await dedupSubstrate.markInFlight(accountId, intent.action_type, resourceId);

      intents.push({
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

  dedupSubstrate.clearTick();
  return { intents, mutations };
}

module.exports = { evaluate };
