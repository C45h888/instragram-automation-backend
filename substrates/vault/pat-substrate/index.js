// substrates/vault/pat-substrate/index.js
// PAT substrate façade: factory-creates workers, owns pre-flight + orchestration.
// Does NOT do I/O — workers do.
//
// Architectural invariant:
//   Substrate = mutation plane (state, pre-flight, factory, signal dispatch)
//   Worker    = executor plane (one bounded I/O call, no state)

const ExchangeWorker = require('./workers/exchange-worker');
const StoreWorker = require('./workers/store-worker');
const RetrieveWorker = require('./workers/retrieve-worker');

/**
 * Exchange a user access token for a page access token + IG business account discovery.
 * One bounded /me/accounts call. No state.
 * @param {{ userAccessToken: string }} input
 */
async function exchange({ userAccessToken }) {
  if (!userAccessToken) {
    return { success: false, error: 'userAccessToken is required' };
  }
  const worker = new ExchangeWorker();
  return worker.execute({ userAccessToken });
}

/**
 * Store a page access token: provision vault key, upsert business account, encrypt, upsert credential.
 * On success, the substrate is responsible for emitting trigger events to the graph-capability plane.
 * @param {{ userId: string, igBusinessAccountId: string, pageAccessToken: string, pageId: string, pageName: string, scope?: string[], triggerBridge?: object, businessAccountId?: string, userId_?: string }} input
 */
async function store(input) {
  const { triggerBridge, businessAccountId, userId_, ...workerInput } = input;
  if (!workerInput.userId || !workerInput.igBusinessAccountId || !workerInput.pageAccessToken || !workerInput.pageName) {
    return { success: false, error: 'userId, igBusinessAccountId, pageAccessToken, pageName are required' };
  }
  const worker = new StoreWorker();
  const result = await worker.execute(workerInput);

  // On success, emit NEW_ACCOUNT_CONNECTED so the capability FSM can evaluate.
  // Substrate is the mutation plane — it owns the signal dispatch.
  if (result.success && triggerBridge) {
    try {
      triggerBridge.emitNewAccountConnected({
        businessAccountId: result.businessAccountId || businessAccountId,
        userId: workerInput.userId || userId_,
      });
    } catch (emitErr) {
      console.warn('⚠️ trigger-bridge emitNewAccountConnected failed:', emitErr.message);
    }
  }
  return result;
}

/**
 * Retrieve and decrypt a page access token.
 * Throws on failure (matches legacy contract).
 * @param {{ userId: string, businessAccountId: string }} input
 */
async function retrieve({ userId, businessAccountId }) {
  if (!userId || !businessAccountId) {
    throw new Error('userId and businessAccountId are required');
  }
  const worker = new RetrieveWorker();
  return worker.execute({ userId, businessAccountId });
}

module.exports = { exchange, store, retrieve };
