// substrates/engagement/index.js
// Engagement substrate: full acquisition pipeline for comments + messages.
//
// Owns: fetch → parse → normalize → persist for engagement domains.
// Does NOT own: retry decisions, governance, circuit breakers, orchestration.

const transport = require('./transport');
const parser = require('./parser');
const persistence = require('../persistence');

/**
 * Execute a full acquisition cycle for an engagement domain.
 *
 * @param {string} accountId
 * @param {string} domain — 'comments' | 'messages'
 * @param {object} params — intent payload
 * @param {object} credentials — pre-resolved { igUserId, pageToken, userId, pageId }
 * @returns {Promise<{status: string, count: number, error: string|null, _usagePct: number|null}>}
 */
async function acquire(accountId, domain, params, credentials) {
  // ── Comments ──────────────────────────────────────────────────────────
  if (domain === 'comments') {
    let raw;
    if (params.media_id) {
      raw = await transport.fetchComments(accountId, params.media_id, params.limit, credentials);
    } else {
      raw = await transport.fetchComments(accountId, null, params.limit, credentials);
    }

    if (!raw.success) {
      return { status: 'failed', count: 0, error: raw.error, _usagePct: raw._usagePct || null };
    }

    const parsed = parser.parseComments(raw.records);
    if (parsed.length === 0) {
      return { status: 'completed', count: 0, error: null, _usagePct: raw._usagePct };
    }

    const batch = { mediaId: params.media_id || 'direct', comments: parsed };
    const stored = await persistence.storeCommentBatches(accountId, [batch]);
    return { status: 'completed', count: stored.count, error: null, _usagePct: raw._usagePct };
  }

  // ── Conversations ─────────────────────────────────────────────────────
  if (domain === 'messages' && !params.conversation_id) {
    const raw = await transport.fetchConversations(accountId, params.convLimit || params.limit, credentials);

    if (!raw.success) {
      return { status: 'failed', count: 0, error: raw.error, _usagePct: raw._usagePct || null };
    }

    const stored = await persistence.storeConversationBatches(
      accountId, raw.rawConversations, raw.igUserId, raw.pageId
    );
    return { status: 'completed', count: stored.count, error: null, _usagePct: raw._usagePct };
  }

  // ── Messages (single conversation) ────────────────────────────────────
  if (domain === 'messages') {
    const raw = await transport.fetchMessages(accountId, params.conversation_id, params.limit, credentials);

    if (!raw.success) {
      return { status: 'failed', count: 0, error: raw.error, _usagePct: raw._usagePct || null };
    }

    const parsed = parser.parseMessages(raw.rawMessages);
    if (parsed.length === 0) {
      return { status: 'completed', count: 0, error: null, _usagePct: raw._usagePct };
    }

    const stored = await persistence.storeMessageBatches(
      accountId,
      [{ conversationId: params.conversation_id, rawMessages: parsed }],
      raw.igUserId, raw.pageId, { pageToken: raw.pageToken, igUserId: raw.igUserId, pageId: raw.pageId }
    );
    return { status: 'completed', count: stored.count, error: null, _usagePct: raw._usagePct };
  }

  return { status: 'failed', count: 0, error: `unknown engagement domain: ${domain}`, _usagePct: null };
}

module.exports = { acquire };
