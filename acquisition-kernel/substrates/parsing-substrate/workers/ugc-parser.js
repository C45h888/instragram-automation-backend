// substrates/parsing/workers/ugc-worker.js
// UGC parsing worker: build rows → CK(DB_WRITE_REQUESTED).
//
// Owns: transforming raw UGC media into normalized rows,
//        emitting through CK for governed DB write.
// Does NOT own: Supabase, governance policy, fetch, orchestration.

const { mapRawPostToUgcContent } = require('../../ugc-content-substrate/ugc-normalizer');

async function execute(rawData, accountId, intentId, extra = {}, governance) {
  if (!rawData.records || rawData.records.length === 0) return { count: 0 };

  const source = rawData.cleanHashtag ? 'hashtag' : 'tagged';
  const rows = rawData.records
    .filter(p => p.id)
    .map(p => mapRawPostToUgcContent(p, accountId, source, rawData.cleanHashtag || null));

  if (rows.length === 0) return { count: 0 };

  if (governance) {
    governance.dispatch({
      type: 'DB_WRITE_REQUESTED',
      domain: 'ugc',
      accountId, intentId,
      table: 'ugc_content',
      operation: 'batch_upsert_ugc',
      rows,
    });
  }

  return { count: 0 };
}

module.exports = { execute };
