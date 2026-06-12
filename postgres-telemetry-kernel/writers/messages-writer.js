// postgres-telemetry-kernel/writers/messages-writer.js
// Messages writer: instagram_dm_messages batch upsert (new messages + read receipts).
//
// Owns: Supabase upsert for instagram_dm_messages table.
// Does NOT own: governance, normalization, fetch, orchestration,
//               orphan repair (Phase 3C: orphan-message-repair.js),
//               failure classification (persistence-failure-substrate),
//               retry policy (retry-cadence-kernel).
//
// Operation dispatch:
//   batch_upsert_messages → upsert on instagram_message_id
//   batch_upsert_dm_seen  → update seen_at watermark on existing message rows

const { getSupabaseAdmin } = require('../../config/supabase');

async function execute(params, governance) {
  const { domain, accountId, intentId, table, rows, operation } = params;
  const supabase = getSupabaseAdmin();
  const workerName = 'messages-writer';

  if (!supabase) {
    governance?.dispatch({
      type: 'DB_WRITE_FAILED', domain, accountId, intentId, table,
      count: 0, rows: rows || [], error: 'supabase_unavailable',
      rawError: { message: 'supabase_unavailable' }, workerName,
      lineageId: intentId, primaryKeyField: 'instagram_message_id',
      primaryKeyValue: rows?.[0]?.instagram_message_id,
      attemptN: 1, operation: 'write', source: 'supabase',
    });
    return;
  }

  try {
    let result;

    if (operation === 'batch_upsert_dm_seen') {
      // Seen receipts: update existing message rows, do not insert new ones.
      // Each row should have instagram_message_id + seen_at + reader_id.
      if (!rows || rows.length === 0) {
        result = { error: null };
      } else {
        const updates = await Promise.all(
          rows.map(r =>
            supabase
              .from(table)
              .update({
                seen_at: r.seenAt || new Date().toISOString(),
                seen_by: r.senderId || null,
              })
              .eq('instagram_message_id', r.messageId || r.instagram_message_id)
          )
        );
        const firstError = updates.find(u => u.error)?.error;
        if (firstError) throw firstError;
        result = { error: null };
      }
    } else {
      // batch_upsert_messages: standard upsert on PK
      result = await supabase
        .from(table)
        .upsert(rows, { onConflict: 'instagram_message_id', ignoreDuplicates: true });
    }

    if (result.error) throw result.error;

    governance?.dispatch({
      type: 'DB_WRITE_COMPLETE', domain, accountId, intentId, table,
      count: rows.length, error: null,
    });
  } catch (err) {
    governance?.dispatch({
      type: 'DB_WRITE_FAILED', domain, accountId, intentId, table,
      count: 0, rows: rows || [], error: err.message, rawError: err,
      workerName, lineageId: intentId, primaryKeyField: 'instagram_message_id',
      primaryKeyValue: rows?.[0]?.instagram_message_id,
      attemptN: 1, operation: 'write', source: 'supabase',
    });
  }
}

module.exports = { execute };
