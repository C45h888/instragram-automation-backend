// substrates/reconciliation/orphan-message-repair.js
// Orphan Message Repair: subscription-based background repair worker.
//
// Owns: async, idempotent repair of instagram_dm_messages rows where
//        conversation_id IS NULL after a message batch write completes.
// Does NOT own: governance policy, normalization, fetch, orchestration.
//
// Triggered by: DB_WRITE_COMPLETE where table === 'instagram_dm_messages'.
// Runs asynchronously — does NOT block writes.
// Idempotent: only updates rows where conversation_id IS NULL.
//
// Phase 3C: extracted from messages-writer.js inline repair block.

const { getSupabaseAdmin } = require('../../config/supabase');

let _started = false;

/**
 * Start the orphan repair worker.
 * Subscribes to DB_WRITE_COMPLETE events on instagram_dm_messages.
 *
 * @param {object} governance — CK module (must have subscribeAction)
 */
function start(governance) {
  if (_started) return;
  _started = true;

  governance.subscribeAction('DB_WRITE_COMPLETE', async (action) => {
    if (action.table !== 'instagram_dm_messages') return;
    if (!action.count || action.count === 0) return;
    if (action.error) return;

    const supabase = getSupabaseAdmin();
    if (!supabase) return;

    try {
      // Find orphaned messages for this account: messages with null conversation_id
      // that share instagram_message_ids with messages that DO have conversation_id.
      // Strategy: resolve conversation_id by matching instagram_message_id against
      // rows that were just upserted (have a conversation_id set).
      //
      // This is a convergent repair: run it after every messages write, and any
      // messages that were inserted before their conversation existed will get fixed.

      const { data: orphans } = await supabase
        .from('instagram_dm_messages')
        .select('instagram_message_id')
        .eq('business_account_id', action.accountId)
        .is('conversation_id', null)
        .limit(200);

      if (!orphans || orphans.length === 0) return;

      // For each orphan, find a matching message that HAS conversation_id set
      const orphanIds = orphans.map(o => o.instagram_message_id);

      const { data: resolved } = await supabase
        .from('instagram_dm_messages')
        .select('instagram_message_id, conversation_id')
        .in('instagram_message_id', orphanIds)
        .not('conversation_id', 'is', null)
        .limit(200);

      if (!resolved || resolved.length === 0) return;

      // Group by conversation_id for batch repair
      const byConv = {};
      for (const row of resolved) {
        if (!byConv[row.conversation_id]) byConv[row.conversation_id] = [];
        byConv[row.conversation_id].push(row.instagram_message_id);
      }

      // Batch-repair per conversation
      for (const [convId, msgIds] of Object.entries(byConv)) {
        await supabase
          .from('instagram_dm_messages')
          .update({ conversation_id: convId, business_account_id: action.accountId })
          .in('instagram_message_id', msgIds)
          .is('conversation_id', null);
      }
    } catch (_) {
      // Fire-and-forget — failure is non-blocking
    }
  });
}

module.exports = { start };
