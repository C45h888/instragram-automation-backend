// acquisition-kernel/substrates/content/hashtag-sync.js
// Hashtag Sync: extracts hashtags from captions, upserts into ugc_monitored_hashtags.
//
// Owns: parsing hashtags from captions, upserting to monitored hashtags table.
// Does NOT own: IG API transport, content fetch, orchestration.
//
// Extracted from helpers/agent-helpers.js (decomposed).

/**
 * Extracts hashtags from brand post captions and upserts into ugc_monitored_hashtags.
 * Auto-populates the table the agent reads at the start of every UGC discovery cycle.
 *
 * @param {object} supabase - Supabase admin client
 * @param {string} businessAccountId
 * @param {string[]} captions
 */
async function syncHashtagsFromCaptions(supabase, businessAccountId, captions) {
  const tagSet = new Set();
  const hashtagRegex = /#(\\w+)/g;
  for (const caption of captions) {
    if (!caption) continue;
    let match;
    while ((match = hashtagRegex.exec(caption)) !== null) {
      tagSet.add(match[1].toLowerCase());
    }
  }
  if (tagSet.size === 0) return;
  const records = [...tagSet].map(tag => ({
    business_account_id: businessAccountId,
    hashtag: tag,
    is_active: true,
  }));
  const { error } = await supabase
    .from('ugc_monitored_hashtags')
    .upsert(records, { onConflict: 'business_account_id,hashtag', ignoreDuplicates: true });
  if (error) console.warn('⚠️ Hashtag sync failed:', error.message);
}

module.exports = { syncHashtagsFromCaptions };
