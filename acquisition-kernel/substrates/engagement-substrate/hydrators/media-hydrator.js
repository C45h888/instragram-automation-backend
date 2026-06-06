// substrates/engagement/hydrators/media-hydrator.js
// Media Hydrator: resolves instagram_media_id → UUID for FK integrity.
//
// Owns: pre-flight hydration of media IDs before comment writer dispatch.
// Does NOT own: DB reads (delegates to governedRead), normalization, upserts.
//
// Constitutional flow:
//   engagement/index.js persist() → hydrator.hydrate(accountId, mediaIds, governance)
//     → governance.governedRead('db.media', { accountId, query: 'igIdToUuid', mediaIds })
//     → returns { resolved: Map<igId, uuid>, missing: Set<igId> }

/**
 * Hydrate Instagram media IDs to DB UUIDs.
 * Returns a Map of resolved IDs and a Set of missing IDs (need stubs).
 *
 * @param {string} accountId
 * @param {string[]} mediaIds — Instagram media IDs to resolve
 * @param {object} governance — CK module (must have governedRead)
 * @param {number} [timeoutMs=10000] — max wait for governedRead
 * @returns {Promise<{resolved: Map<string, string>, missing: Set<string>}>}
 */
async function hydrate(accountId, mediaIds, governance, timeoutMs = 10000) {
  const unique = [...new Set(mediaIds || [])];

  if (unique.length === 0) {
    return { resolved: new Map(), missing: new Set() };
  }

  // Constitutional read gate
  const result = await governance.governedRead('db.media', {
    accountId,
    query: 'igIdToUuid',
    mediaIds: unique,
  }, timeoutMs);

  const resolved = new Map();
  const missing = new Set(unique);

  if (result.success && Array.isArray(result.data)) {
    for (const row of result.data) {
      resolved.set(row.instagram_media_id, row.id);
      missing.delete(row.instagram_media_id);
    }
  }

  return { resolved, missing };
}

module.exports = { hydrate };
