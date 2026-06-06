// substrates/ugc/parser.js
// UGC parser: validates raw UGC media shapes from IG API.
//
// Owns: extracting structured data from raw hashtag/tagged API responses.
// Does NOT own: API transport, DB writes, schema normalization, orchestration.

/**
 * Parse raw UGC media records from hashtag search or tagged endpoints.
 *
 * @param {Array} rawMedia — [{ id, media_type, media_url, thumbnail_url, caption, timestamp, username, like_count, comments_count, owner, owner_id }]
 * @returns {Array}
 */
function parseUgcMedia(rawMedia) {
  if (!Array.isArray(rawMedia)) return [];
  return rawMedia
    .filter(m => m && typeof m.id === 'string' && m.id.length > 0)
    .map(m => ({
      id: m.id,
      media_type: m.media_type || null,
      media_url: m.media_url || m.thumbnail_url || null,
      thumbnail_url: m.thumbnail_url || null,
      caption: m.caption || null,
      timestamp: m.timestamp || null,
      username: m.username || null,
      like_count: typeof m.like_count === 'number' ? m.like_count : 0,
      comments_count: typeof m.comments_count === 'number' ? m.comments_count : 0,
      permalink: m.permalink || null,
      owner_id: m.owner?.id || m.owner_id || null,
    }));
}

module.exports = { parseUgcMedia };
