// substrates/publishing/index.js
// Publishing bounded-substrate routing entry.
//
// Owns: mapping worker names to bounded substrates.
// Does NOT own: transport, credentials, retry, orchestration, DB mutations.
//
// Workers are instantiated by the publishing orchestrator (emission-orchestrator.js)
// via EXECUTE_CONTENT / EXECUTE_ENGAGEMENT subscribers.

const content = require('./content');
const engagement = require('./engagement');

const WORKER_MAP = {
  // ── Content substrate ──────────────────────────────────────
  posts:    content,
  stories:  content,

  // ── Engagement substrate ───────────────────────────────────
  comments: engagement,
  messages: engagement,
};

/**
 * Resolves a worker name to its bounded substrate module.
 * @param {string} worker — 'posts'|'stories'|'comments'|'messages'
 * @returns {object|null} substrate with execute* methods
 */
function resolve(worker) {
  return WORKER_MAP[worker] || null;
}

module.exports = { resolve, content, engagement };
