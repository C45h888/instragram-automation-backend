// substrates/realtime.js
// Bounded substrate: Supabase Realtime subscription management.
//
// Owns: establishing Realtime channel subscriptions on relevant tables,
//        emitting insert events to the signal bus.
// Does NOT own: evaluation logic, condition checking, orchestration,
//               signal bus topology.
//
// Signal bus topology: realtime emits to signalBus; evaluator subscribes.
// This decouples the realtime substrate from evaluator topology.
//
// Tables subscribed:
//   scheduled_posts — INSERT (new approved post ready to publish)
//   post_queue      — INSERT (new publish action queued by routes)

const { getSupabaseAdmin } = require('../config/supabase');
const signalBus = require('../control-plane/signal-bus');

// ── Module state ─────────────────────────────────────────────────────────────

/**
 * @type {Map<string, import('@supabase/supabase-js').RealtimeChannel>}
 *  channel name → active channel
 */
const _channels = new Map();

// ── Channel factory ───────────────────────────────────────────────────────────

function _buildFilter(businessAccountId) {
  return `business_account_id=eq.${business_accountId}`;
}

function _onInsert(accountId, table, record) {
  // Emit to signal bus — realtime substrate has no knowledge of evaluator
  signalBus.emit('db:insert', { accountId, table, record });
}

function _subscribeAccount(accountId) {
  const admin = getSupabaseAdmin();
  if (!admin) return null;

  const channelName = `publish:${accountId}`;
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
      _onInsert(accountId, 'scheduled_posts', payload.new);
    })
    .on('postgres_changes', {
      event: 'INSERT',
      schema: 'public',
      table: 'post_queue',
      filter: `business_account_id=eq.${accountId}`,
    }, (payload) => {
      _onInsert(accountId, 'post_queue', payload.new);
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
 * Starts Realtime subscriptions for all currently active accounts.
 * @param {Array<{id: string}>} accounts - active accounts from persistence
 */
async function startRealtime(accounts) {
  for (const account of accounts) {
    _subscribeAccount(account.id);
  }
}

/**
 * Refreshes subscriptions: unsubscribes removed accounts, subscribes new ones.
 * @param {Array<{id: string}>} currentAccounts
 */
async function refreshRealtime(currentAccounts) {
  const currentIds = new Set(currentAccounts.map(a => a.id));
  const subscribedIds = new Set(
    [..._channels.keys()].map(k => k.replace('publish:', ''))
  );

  for (const id of currentIds) {
    if (!subscribedIds.has(id)) {
      _subscribeAccount(id);
    }
  }

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

  for (const [, channel] of _channels) {
    admin.removeChannel(channel);
  }
  _channels.clear();
}

module.exports = {
  startRealtime,
  refreshRealtime,
  stopRealtime,
};
