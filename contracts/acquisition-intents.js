// backend/contracts/acquisition-intents.js
// Schema validation for AcquisitionIntent payloads arriving from the agent.
//
// The agent's SyncLayer (sync_layer.py) emits intents in this shape:
//   { intent_id, account_id, domains, fetch_type, priority,
//     issued_at, dedupe_key, status, result_key, result_ttl_sec }
//
// This module validates the shape before the backend worker processes it.

const VALID_FETCH_TYPES = [
  'account_insights',
  'media_insights',
  'ugc_discovery',
  'post_comments',
  'publish_media',      // publish_post — scheduled posts / own media publishing
  'publish_ugc',       // repost_ugc — UGC content reposting after permission
  'publish_messaging', // reply_comment, reply_dm, send_dm — outbound messaging actions
];

const VALID_PRIORITIES = ['low', 'normal', 'high', 'critical'];

const REQUIRED_FIELDS = ['intent_id', 'account_id', 'fetch_type'];

/**
 * Validates an AcquisitionIntent payload against the contract schema.
 *
 * @param {object} payload - Raw JSON from Redis queue
 * @returns {{ valid: true } | { valid: false, error: string }}
 */
function validateIntent(payload) {
  if (!payload || typeof payload !== 'object') {
    return { valid: false, error: 'payload is not an object' };
  }

  for (const field of REQUIRED_FIELDS) {
    if (!payload[field]) {
      return { valid: false, error: `missing required field: ${field}` };
    }
  }

  if (typeof payload.intent_id !== 'string' || payload.intent_id.length === 0) {
    return { valid: false, error: 'intent_id must be a non-empty string' };
  }

  if (typeof payload.account_id !== 'string' || payload.account_id.length === 0) {
    return { valid: false, error: 'account_id must be a non-empty string' };
  }

  if (!VALID_FETCH_TYPES.includes(payload.fetch_type)) {
    return {
      valid: false,
      error: `unknown fetch_type: ${payload.fetch_type}. valid: ${VALID_FETCH_TYPES.join(', ')}`,
    };
  }

  if (payload.priority && !VALID_PRIORITIES.includes(payload.priority)) {
    return {
      valid: false,
      error: `invalid priority: ${payload.priority}. valid: ${VALID_PRIORITIES.join(', ')}`,
    };
  }

  return { valid: true };
}

module.exports = { validateIntent, VALID_FETCH_TYPES, VALID_PRIORITIES };
