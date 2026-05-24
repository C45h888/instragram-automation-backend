// control-plane/mutation-substrate.js
// Persistence mutation substrate: deterministic state transitions.
//
// Owns: writing state transitions to Supabase (status updates, etc.)
// Does NOT own: evaluation logic, orchestration, intent emission.
//
// Called by evaluator when a policy outcome requires a state mutation
// (e.g., marking a scheduled_post as 'failed' when asset is missing).
//
// All mutations are idempotent — uses .eq() clauses to ensure
// only the intended row is updated.

const { getSupabaseAdmin } = require('../config/supabase');
const { logWithDomain } = require('../substrates/telemetry');

/**
 * Applies a state mutation to a single database row.
 *
 * @param {string} table - e.g. 'scheduled_posts', 'post_queue'
 * @param {string} recordId - UUID of the row
 * @param {object} updates - columns to update, e.g. { status: 'failed' }
 * @param {string} reason - human-readable reason for observability
 */
async function applyMutation(table, recordId, updates, reason) {
  const supabase = getSupabaseAdmin();
  if (!supabase) {
    console.warn(`[mutation-substrate] Supabase unavailable — skipping ${table}.${recordId} mutation: ${reason}`);
    return;
  }

  const { error } = await supabase
    .from(table)
    .update(updates)
    .eq('id', recordId);

  if (error) {
    console.error(`[mutation-substrate] Failed to mutate ${table}.${recordId}:`, error.message);
    await logWithDomain('publish', {
      endpoint: '/mutation-substrate', method: 'SYSTEM',
      success: false, error: error.message,
      details: { table, recordId, updates, reason },
    });
    return;
  }

  console.log(`[mutation-substrate] ${table}.${recordId} → ${JSON.stringify(updates)} (${reason})`);
}

module.exports = { applyMutation };
