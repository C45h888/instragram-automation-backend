// postgres-telemetry-kernel/writers/comments-writer.js
// Comments writer: instagram_comments batch upsert (parent comments + replies).
//
// Owns: Supabase upsert for instagram_comments table.
// Does NOT own: governance, normalization, fetch, orchestration,
//               media UUID resolution (Phase 3B: media-hydrator),
//               failure classification (persistence-failure-substrate),
//               retry policy (retry-cadence-kernel).
//
// Operation dispatch:
//   batch_upsert_comments       → upsert on instagram_comment_id (parent comments)
//   batch_upsert_comment_replies→ upsert on instagram_comment_id (replies have parent_comment_id)

const { getSupabaseAdmin } = require('../../config/supabase');

// Map of operation name → primary key field
const _OPERATION_PK = {
  batch_upsert_comments:        'instagram_comment_id',
  batch_upsert_comment_replies: 'instagram_comment_id',
};

async function execute(params, governance) {
  const { domain, accountId, intentId, table, rows, operation } = params;
  const supabase = getSupabaseAdmin();
  const onConflict = _OPERATION_PK[operation] || 'instagram_comment_id';
  const workerName = 'comments-writer';

  if (!supabase) {
    governance?.dispatch({
      type: 'DB_WRITE_FAILED', domain, accountId, intentId, table,
      count: 0, rows: rows || [], error: 'supabase_unavailable',
      rawError: { message: 'supabase_unavailable' }, workerName,
      lineageId: intentId, primaryKeyField: onConflict,
      primaryKeyValue: rows?.[0]?.instagram_comment_id,
      attemptN: 1, operation: 'write', source: 'supabase',
    });
    return;
  }

  try {
    const { error } = await supabase
      .from(table)
      .upsert(rows, { onConflict, ignoreDuplicates: false });

    if (error) throw error;

    governance?.dispatch({
      type: 'DB_WRITE_COMPLETE', domain, accountId, intentId, table,
      count: rows.length, error: null,
    });
  } catch (err) {
    governance?.dispatch({
      type: 'DB_WRITE_FAILED', domain, accountId, intentId, table,
      count: 0, rows: rows || [], error: err.message, rawError: err,
      workerName, lineageId: intentId, primaryKeyField: onConflict,
      primaryKeyValue: rows?.[0]?.instagram_comment_id,
      attemptN: 1, operation: 'write', source: 'supabase',
    });
  }
}

module.exports = { execute, _OPERATION_PK };
