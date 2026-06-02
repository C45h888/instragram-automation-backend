// substrates/db/writers/messages-writer.js
// Messages writer: instagram_dm_messages batch upsert.
//
// Owns: Supabase upsert for instagram_dm_messages table.
// Does NOT own: governance, normalization, fetch, orchestration.

const { getSupabaseAdmin } = require('../../../config/supabase');

async function execute(params, governance) {
  const { domain, accountId, intentId, table, rows } = params;
  const supabase = getSupabaseAdmin();
  if (!supabase) {
    governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId, table, count: 0, error: 'supabase_unavailable' });
    return;
  }

  try {
    const { error } = await supabase
      .from(table)
      .upsert(rows, { onConflict: 'instagram_message_id', ignoreDuplicates: true });

    if (error) throw error;

    // Orphan repair: fix null conversation_id for messages matched by message_id
    const messageIdsByConv = {};
    for (const r of rows) {
      if (!r.conversation_id) continue;
      if (!messageIdsByConv[r.conversation_id]) messageIdsByConv[r.conversation_id] = [];
      messageIdsByConv[r.conversation_id].push(r.instagram_message_id);
    }

    const repairPromises = Object.entries(messageIdsByConv).map(([convUUID, msgIds]) =>
      supabase
        .from('instagram_dm_messages')
        .update({ conversation_id: convUUID, business_account_id: accountId })
        .in('instagram_message_id', msgIds)
        .is('conversation_id', null)
    );
    await Promise.allSettled(repairPromises);

    governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId, table, count: rows.length, error: null });
  } catch (err) {
    governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId, table, count: 0, error: err.message });
  }
}

module.exports = { execute };
