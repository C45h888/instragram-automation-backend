// substrates/engagement/hydrators/conversation-hydrator.js
// Conversation Hydrator: resolves customer_user_id from instagram_business_accounts.
//
// Owns: pre-flight hydration of conversation records before writer dispatch.
// Does NOT own: DB reads (delegates to governedRead), normalization, upserts.
//
// Constitutional flow:
//   engagement/index.js persist() → hydrator.hydrate(records)
//     → governance.governedRead('db.accounts', { query: 'igIdToUserId', igIds })
//     → fills customer_user_id on each record
//     → returns hydrated records ready for writer dispatch

/**
 * Hydrate conversation records with customer_user_id resolved from
 * instagram_business_accounts.instagram_business_id → user_id mapping.
 *
 * @param {Array<{id: string, customer_instagram_id?: string, ...}>} records — parsed conversation records
 * @param {object} governance — CK module (must have governedRead)
 * @param {number} [timeoutMs=10000] — max wait for governedRead
 * @returns {Promise<Array>} records with customer_user_id filled (or null)
 */
async function hydrate(records, governance, timeoutMs = 10000) {
  if (!records || records.length === 0) return records;

  // Collect unique igIds that need resolution
  const igIds = [...new Set(
    records.map(r => r.customer_instagram_id).filter(Boolean)
  )];

  if (igIds.length === 0) return records;

  // Constitutional read gate
  const result = await governance.governedRead('db.accounts', {
    query: 'igIdToUserId',
    igIds,
  }, timeoutMs);

  // Build lookup map on success; leave customer_user_id null on failure
  const igIdToUserId = {};
  if (result.success && Array.isArray(result.data)) {
    for (const row of result.data) {
      igIdToUserId[row.instagram_business_id] = row.user_id;
    }
  }

  // Fill customer_user_id on each record
  for (const r of records) {
    if (r.customer_instagram_id) {
      r.customer_user_id = igIdToUserId[r.customer_instagram_id] || null;
    }
  }

  return records;
}

module.exports = { hydrate };
