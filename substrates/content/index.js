// substrates/content/index.js
// Content substrate: full pipeline for business media posts.
//
// Owns: fetch → parse → normalize → persist for content domain.
// Does NOT own: retry logic, error classification, orchestration, credential resolution.

const transport = require('./transport');
const persistence = require('../persistence');

/**
 * Fetch raw data from Instagram API for content domain.
 * Pure transport — no parsing, no persistence.
 */
async function fetch(accountId, params, credentials) {
  return transport.fetchPosts(accountId, params.limit || 50, credentials);
}

/**
 * Persist raw business post data to Supabase.
 * Handles normalization internally.
 */
async function persist(accountId, rawData) {
  if (!rawData.posts || rawData.posts.length === 0) return { count: 0 };
  return persistence.storeBusinessPosts(accountId, rawData.posts);
}

module.exports = { fetch, persist };
