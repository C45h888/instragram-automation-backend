// control-plane/policies/publishing.js
// Publishing policy: deterministic evaluation rules for publishing intents.
//
// Owns: business-domain logic (status checks, retry window checks).
// Does NOT own: persistence, Redis, orchestration, telemetry.
//
// The evaluator calls evaluateRecord() for each buffered event.
// Returns an outcome object describing what the evaluator should do.

const VALID_MEDIA_TYPES = ['IMAGE', 'VIDEO', 'REELS', 'CAROUSEL'];

/**
 * Evaluates a scheduled_posts record.
 * @param {object} record - raw DB row from scheduled_posts
 * @returns {{ action: 'emit'|'skip'|'mark_failed', intent?: object, reason?: string }}
 */
function evaluateScheduledPost(record) {
  // Only process approved posts
  if (record.status !== 'approved') {
    return { action: 'skip', reason: `status=${record.status}` };
  }

  // Must have an asset_id
  if (!record.asset_id) {
    return { action: 'mark_failed', reason: 'missing asset_id', updates: { status: 'failed' } };
  }

  return {
    action: 'emit',
    intent: {
      action_type: 'publish_post',
      payload: {
        asset_id: record.asset_id,
        scheduled_post_id: record.id,
      },
    },
  };
}

/**
 * Evaluates a post_queue record.
 * @param {object} record - raw DB row from post_queue
 * @returns {{ action: 'emit'|'skip', intent?: object, reason?: string }}
 */
function evaluatePostQueue(record) {
  // Only process pending or failed rows
  if (!['pending', 'failed'].includes(record.status)) {
    return { action: 'skip', reason: `status=${record.status}` };
  }

  // Skip if retry window not elapsed
  if (record.next_retry_at && new Date(record.next_retry_at) > new Date()) {
    return { action: 'skip', reason: 'retry window not elapsed' };
  }

  return {
    action: 'emit',
    intent: {
      action_type: record.action_type,
      payload: record.payload || {},
      queue_row_id: record.id,
      scheduled_post_id: record.payload?.scheduled_post_id || null,
    },
  };
}

/**
 * Main entry point — routes to the appropriate evaluator.
 * @param {string} table - 'scheduled_posts' | 'post_queue'
 * @param {object} record - DB row
 * @returns {{ action: 'emit'|'skip'|'mark_failed', intent?: object, reason?: string, updates?: object }}
 */
function evaluateRecord(table, record) {
  if (table === 'scheduled_posts') {
    return evaluateScheduledPost(record);
  }
  if (table === 'post_queue') {
    return evaluatePostQueue(record);
  }
  return { action: 'skip', reason: `unknown table: ${table}` };
}

module.exports = { evaluateRecord };
