// substrates/db/writers/comments-writer.js
// Comments writer: instagram_comments batch upsert.
//
// Owns: Supabase upsert for instagram_comments table including
//        media UUID resolution for FK integrity.
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
    // Resolve media UUIDs for FK integrity
    const mediaIds = [...new Set(rows.map(r => r.media_id).filter(Boolean))];
    const { data: existing } = await supabase
      .from('instagram_media')
      .select('id, instagram_media_id')
      .in('instagram_media_id', mediaIds);

    const mediaUUIDMap = {};
    for (const row of existing || []) mediaUUIDMap[row.instagram_media_id] = row.id;

    const missingIds = mediaIds.filter(id => !mediaUUIDMap[id]);
    if (missingIds.length > 0) {
      const stubs = missingIds.map(id => ({ instagram_media_id: id, business_account_id: accountId }));
      const { data: created } = await supabase
        .from('instagram_media')
        .upsert(stubs, { onConflict: 'instagram_media_id' })
        .select('id, instagram_media_id');
      for (const row of created || []) mediaUUIDMap[row.instagram_media_id] = row.id;
    }

    for (const r of rows) {
      if (r.media_id && mediaUUIDMap[r.media_id]) r.media_id = mediaUUIDMap[r.media_id];
    }

    const { error } = await supabase
      .from(table)
      .upsert(rows, { onConflict: 'instagram_comment_id', ignoreDuplicates: false });

    if (error) throw error;

    governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId, table, count: rows.length, error: null });
  } catch (err) {
    governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId, table, count: 0, error: err.message });
  }
}

module.exports = { execute };
