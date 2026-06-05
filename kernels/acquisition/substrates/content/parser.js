// substrates/content/parser.js
// Content parser: validates raw business post shapes from IG API.
//
// Owns: extracting structured data from raw post API responses.
// Does NOT own: API transport, DB writes, schema normalization, orchestration.

/**
 * Parse raw business post records from IG API.
 *
 * @param {Array} posts — raw posts from fetchPosts [{ id, media_type, caption, media_url, thumbnail_url, permalink, timestamp, like_count, comments_count }]
 * @returns {Array}
 */
function parsePosts(posts) {
  if (!Array.isArray(posts)) return [];
  return posts
    .filter(p => p && typeof p.id === 'string' && p.id.length > 0)
    .map(p => ({
      id: p.id,
      media_type: p.media_type || null,
      caption: p.caption || null,
      media_url: p.media_url || null,
      thumbnail_url: p.thumbnail_url || null,
      permalink: p.permalink || null,
      timestamp: p.timestamp || null,
      like_count: typeof p.like_count === 'number' ? p.like_count : 0,
      comments_count: typeof p.comments_count === 'number' ? p.comments_count : 0,
    }));
}

module.exports = { parsePosts };
