// postgres-telemetry-kernel/substrates/cognition-scanner/workers/subscription-worker.js
// Subscription Worker: owns per-account Realtime channel lifecycle.
// Bound to: one Supabase Realtime channel per accountId.
// Does NOT own: event processing, queue state, FSM dispatch decisions.

const { getSupabaseAdmin } = require('../../../config/supabase');

/** @type {Map<string, import('@supabase/supabase-js').RealtimeChannel>} */
const _channels = new Map(); // channelName → RealtimeChannel

function _channelName(accountId) {
  return `cognition:${accountId}`;
}

/**
 * Subscribe to cognition events for one account.
 *
 * @param {string} accountId
 * @param {(accountId: string, table: string, record: object) => void} onEvent
 * @returns {import('@supabase/supabase-js').RealtimeChannel | null}
 */
function subscribe(accountId, onEvent) {
  const admin = getSupabaseAdmin();
  if (!admin) return null;

  const name = _channelName(accountId);
  unsubscribe(accountId); // clean up any existing

  const channel = admin.channel(name, {
    config: {
      broadcast: { self: false },
      postgres: { filter: `business_account_id=eq.${accountId}` },
    },
  });

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
    .subscribe((status) => {
      if (status === 'SUBSCRIBED') {
        console.log(`[subscription-worker] Subscribed for account ${accountId}`);
      } else if (status === 'CHANNEL_ERROR') {
        console.error(`[subscription-worker] Channel error for account ${accountId}`);
      } else if (status === 'TIMED_OUT') {
        console.warn(`[subscription-worker] Subscription timed out for account ${accountId}`);
      }
    });

  _channels.set(name, channel);
  return channel;
}

/**
 * Unsubscribe and remove channel for one account.
 *
 * @param {string} accountId
 */
function unsubscribe(accountId) {
  const admin = getSupabaseAdmin();
  if (!admin) return;

  const name = _channelName(accountId);
  const existing = _channels.get(name);
  if (existing) {
    admin.removeChannel(existing);
    _channels.delete(name);
  }
}

/**
 * Remove all channels. Call on substrate stop.
 */
function unsubscribeAll() {
  const admin = getSupabaseAdmin();
  if (!admin) return;

  for (const [, channel] of _channels) {
    admin.removeChannel(channel);
  }
  _channels.clear();
}

/**
 * Returns current subscribed account count.
 */
function activeCount() {
  return _channels.size;
}

/**
 * Returns set of subscribed account IDs.
 */
function getSubscribedAccountIds() {
  return new Set([..._channels.keys()].map(k => k.replace('cognition:', '')));
}

module.exports = {
  subscribe,
  unsubscribe,
  unsubscribeAll,
  activeCount,
};