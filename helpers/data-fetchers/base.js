// helpers/data-fetchers/base.js
// Thin re-export layer — all functions now originate from bounded substrates.
// Data-fetcher domain files import from here without changes.
// Under the hood: substrates/transport/, substrates/persistence/,
// substrates/normalization/, substrates/telemetry/, substrates/quota/.

const axios = require('axios');
const { getSupabaseAdmin } = require('../../config/supabase');
const {
  resolveAccountCredentials,
  categorizeIgError,
  ensureMediaRecord,
  ensureConversationRows,
  syncHashtagsFromCaptions,
  GRAPH_API_BASE,
} = require('../agent-helpers');
const { mapRawPostToUgcContent } = require('../ugc-field-map');

// ── Re-exports from substrates ───────────────────────────────────────────────
const { logWithDomain } = require('../../substrates/telemetry');
const { transformMessage, normalizeComment, normalizeBusinessPost, normalizeMediaInsight } = require('../../substrates/normalization');
const { storeCommentBatches, storeConversationBatches, storeMessageBatches, storeUgcContentBatch } = require('../../substrates/persistence');
const { parseUsageHeader } = require('../../substrates/quota');

module.exports = {
  axios,
  getSupabaseAdmin,
  resolveAccountCredentials,
  categorizeIgError,
  ensureMediaRecord,
  ensureConversationRows,
  syncHashtagsFromCaptions,
  mapRawPostToUgcContent,
  GRAPH_API_BASE,
  logWithDomain,
  transformMessage,
  normalizeComment,
  normalizeBusinessPost,
  normalizeMediaInsight,
  storeCommentBatches,
  storeConversationBatches,
  storeMessageBatches,
  storeUgcContentBatch,
  parseUsageHeader,
};
