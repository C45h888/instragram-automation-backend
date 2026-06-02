// substrates/db/writers/conversations-writer.js
// Conversations writer: instagram_dm_conversations batch upsert.
//
// Owns: Supabase upsert for instagram_dm_conversations table including
//        customer_user_id resolution.
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
    // Batch-resolve customer_user_id from instagram_business_accounts
    const igIds = rows.map(r => r.customer_instagram_id).filter(Boolean);
    if (igIds.length > 0) {
      const { data: knownAccounts } = await supabase
        .from('instagram_business_accounts')
        .select('instagram_business_id, user_id')
        .in('instagram_business_id', igIds);
      const igIdToUserId = {};
      for (const a of knownAccounts || []) igIdToUserId[a.instagram_business_id] = a.user_id;
      for (const r of rows) {
        if (r.customer_instagram_id) r.customer_user_id = igIdToUserId[r.customer_instagram_id] || null;
      }
    }

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
