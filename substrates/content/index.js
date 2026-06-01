// substrates/content/index.js
// Content substrate: full acquisition pipeline for business media posts.
//
// Owns: fetch → parse → normalize → persist for content domain.
// Does NOT own: retry decisions, governance, orchestration.

const transport = require('./transport');
const parser = require('./parser');
const normalizer = require('./normalizer');
const persistence = require('../persistence');

/**
 * Execute a full acquisition cycle for the content domain.
 *
 * @param {string} accountId
 * @param {string} domain — 'media'
 * @param {object} params — intent payload { limit, since, until }
 * @param {object} credentials — pre-resolved
 * @returns {Promise<{status: string, count: number, error: string|null, _usagePct: number|null}>}
 */
async function acquire(accountId, domain, params, credentials) {
  const timeWindow = (params.since || params.until)
    ? { since: params.since, until: params.until }
    : null;

  const raw = await transport.fetchPosts(accountId, params.limit, credentials, timeWindow);

  if (!raw.success) {
    return { status: 'failed', count: 0, error: raw.error, _usagePct: raw._usagePct || null };
  }

  const parsed = parser.parsePosts(raw.posts);
  if (parsed.length === 0) {
    return { status: 'completed', count: 0, error: null, _usagePct: raw._usagePct };
  }

  const records = parsed.map(p => normalizer.normalizeBusinessPost(p, accountId));
  const stored = await persistence.storeBusinessPosts(accountId, records);
  return { status: 'completed', count: stored.count, error: null, _usagePct: raw._usagePct };
}

module.exports = { acquire };
