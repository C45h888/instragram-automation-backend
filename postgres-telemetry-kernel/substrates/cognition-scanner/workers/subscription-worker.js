// postgres-telemetry-kernel/substrates/cognition-scanner/workers/subscription-worker.js
// Subscription Worker: owns per-account Realtime channel lifecycle.
//
// Owns: channel map + lifecycle. Supabase Realtime access via bedrock.
// Does NOT own: event processing, queue state, FSM dispatch decisions.

const bedrock = require('../../../bedrock');

/** @type {Map<string, object>} */
const _channels = new Map(); // channelName → channel object

function _channelName(accountId) {
  return `cognition:${accountId}`;
}

/**
 * Subscribe to cognition events for one account.
 */
function subscribe(accountId, onEvent) {
  unsubscribe(accountId); // clean up existing

  const channel = bedrock.realtime.subscribeToTable('scheduled_posts', {
    event: 'UPDATE',
    filter: `status=eq.approved`,
  });

  if (!channel) return null;

  channel
    .on('postgres_changes', {
      event: 'UPDATE',
      schema: 'public',
      table: 'scheduled_posts',
      filter: 'status=eq.approved',
    }, (payload) => {
      onEvent(accountId, 'scheduled_posts', payload.new);
    })
    .on('postgres_changes', {
      event: 'UPDATE',
      schema: 'public',
      table: 'post_queue',
      filter: 'status=eq.pending',
    }, (payload) => {
      onEvent(accountId, 'post_queue', payload.new);
    })
    .on('postgres_changes', {
      event: 'INSERT',
      schema: 'public',
      table: 'post_queue',
    }, (payload) => {
      onEvent(accountId, 'post_queue', payload.new);
    })
    .subscribe((status) => {
      if (status === 'SUBSCRIBED') {
        console.log(`Cognition channel subscribed for account ${accountId}`);
      }
    });

  _channels.set(_channelName(accountId), channel);
  return channel;
}

/**
 * Unsubscribe from cognition events for one account.
 */
function unsubscribe(accountId) {
  const name = _channelName(accountId);
  const existing = _channels.get(name);
  if (existing) {
    try { existing.unsubscribe(); } catch (_) {}
    _channels.delete(name);
  }
}

/**
 * Get active channel count.
 */
function getActiveChannelCount() {
  return _channels.size;
}

module.exports = { subscribe, unsubscribe, getActiveChannelCount };
