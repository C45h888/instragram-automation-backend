// substrates/db/writers/conversations-writer.js
// Conversations writer: instagram_dm_conversations batch upsert.
//
// Owns: Supabase upsert for instagram_dm_conversations table.
// Does NOT own: governance, normalization, fetch, orchestration,
//               customer_user_id resolution (Phase 3A: hydrator).

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
      .upsert(rows, { onConflict: 'instagram_thread_id', ignoreDuplicates: false });

    if (error) throw error;

    governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId, table, count: rows.length, error: null });
  } catch (err) {
    governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId, table, count: 0, error: err.message });
  }
}

module.exports = { execute };
