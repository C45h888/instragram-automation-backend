// substrates/realtime.js
// Bounded substrate: Supabase Realtime subscription management.
//
// Owns: establishing Realtime channel subscriptions on relevant tables,
//        forwarding insert/update events to the evaluator buffer.
// Does NOT own: evaluation logic, condition checking, orchestration.
//
// Tables subscribed:
//   scheduled_posts     — INSERT (new approved post ready to publish)
//   post_queue          — INSERT (new publish action queued by routes)
//
// Each subscription is per-account channel, forwarding raw DB row
// to evaluator._buffer() for accumulation until next evaluation tick.

const { getSupabaseAdmin } = require('../config/supabase');

// ── Module state ─────────────────────────────────────────────────────────────

/**
 * @type {Map<string, import('@supabase/supabase-js').RealtimeChannel>}
 *  channel name → active channel
 */
const _channels = new Map();

/**
 * @type {Function|null}
 * Set by control-plane/evaluator.js to receive buffered events.
 * Signature: (accountId: string, table: string, record: object) => void
 */
let _bufferCallback = null;

// ── Channel factory ───────────────────────────────────────────────────────────

/**
 * Builds the filter clause for a per-account Realtime channel.
 * Uses Supabase's PostgREST `eq()` filter in the channel's broadcast config.
 */
function _buildFilter(businessAccountId) {
  return `business_account_id=eq.${business_accountId}`;
}

/**
 * Subscribes to INSERT events on scheduled_posts and post_queue for one account.
 * Calls _bufferCallback with every new row.
 */
function _subscribeAccount(accountId) {
  const admin = getSupabaseAdmin();
  if (!admin) return null;

  const channelName = `publish:${accountId}`;

  // Unsubscribe any existing channel for this account (idempotent)
  _unsubscribeAccount(accountId);

  const channel = admin.channel(channelName, {
    config: { broadcast: { self: false }, postgres: { filter: _buildFilter(accountId) } }
  });

  channel
    .on('postgres_changes', {
      event: 'INSERT',
      schema: 'public',
      table: 'scheduled_posts',
      filter: `business_account_id=eq.${accountId}`,
    }, (payload) => {
      if (_bufferCallback) {
        _bufferCallback(accountId, 'scheduled_posts', payload.new);
      }
    })
    .on('postgres_changes', {
      event: 'INSERT',
      schema: 'public',
      table: 'post_queue',
      filter: `business_account_id=eq.${accountId}`,
    }, (payload) => {
      if (_bufferCallback) {
        _bufferCallback(accountId, 'post_queue', payload.new);
      }
    })
    .subscribe((status) => {
      if (status === 'SUBSCRIBED') {
        console.log(`[realtime] Subscribed to publish events for account ${accountId}`);
      } else if (status === 'CHANNEL_ERROR') {
        console.error(`[realtime] Channel error for account ${accountId}`);
      }
    });

  _channels.set(channelName, channel);
  return channel;
}

function _unsubscribeAccount(accountId) {
  const admin = getSupabaseAdmin();
  if (!admin) return;

  const channelName = `publish:${accountId}`;
  const existing = _channels.get(channelName);
  if (existing) {
    admin.removeChannel(existing);
    _channels.delete(channelName);
  }
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Registers the buffer callback from the evaluator.
 * Must be called before startRealtime() so the callback is set before any events fire.
 * @param {Function} callback - (accountId, table, record) => void
 */
function registerBufferCallback(callback) {
  _bufferCallback = callback;
}

/**
 * Starts Realtime subscriptions for all currently active accounts.
 * Called once by lifecycle after the initial account pool is loaded.
 * @param {Array<{id: string}>} accounts - active accounts from persistence
 */
async function startRealtime(accounts) {
  for (const account of accounts) {
    _subscribeAccount(account.id);
  }
}

/**
 * Refreshes subscriptions: unsubscribes removed accounts, subscribes new ones.
 * Called by lifecycle's periodic refresh loop.
 * @param {Array<{id: string}>} currentAccounts
 */
async function refreshRealtime(currentAccounts) {
  const currentIds = new Set(currentAccounts.map(a => a.id));
  const subscribedIds = new Set(
    [..._channels.keys()].map(k => k.replace('publish:', ''))
  );

  // Subscribe new accounts
  for (const id of currentIds) {
    if (!subscribedIds.has(id)) {
      _subscribeAccount(id);
    }
  }

  // Unsubscribe removed accounts
  for (const [channelName, channel] of _channels) {
    const accountId = channelName.replace('publish:', '');
    if (!currentIds.has(accountId)) {
      _unsubscribeAccount(accountId);
    }
  }
}

/**
 * Stops all Realtime subscriptions and clears the channel map.
 */
async function stopRealtime() {
  const admin = getSupabaseAdmin();
  if (!admin) return;

  for (const [name, channel] of _channels) {
    admin.removeChannel(channel);
  }
  _channels.clear();
}

module.exports = {
  registerBufferCallback,
  startRealtime,
  refreshRealtime,
  stopRealtime,
};
