// dedup-kernel/substrates/repair/conversation-repair.js
// Conversation Repair Substrate: mutation plane for missing conversations.
//
// Owns: CK subscription (EXECUTE_CONVERSATION_REPAIR), dedup via dedup substrate,
//        factory-create worker, completion signal.
// Does NOT own: Graph API calls (worker), DB writes (delegates to writers),
//               governance policy (CK + FSM).
//
// Constitutional flow:
//   CK(REPAIR_CONVERSATION) → dedup-fsm → EXECUTE_CONVERSATION_REPAIR
//   → conversation-repair substrate → worker → REPAIR_CONVERSATION_COMPLETE
//
// Phase 5: canonical repair path — dedup via dedup-kernel substrate,
//          DB writes through dispatchWrite → CK → persist-telemetry-fsm.

const ConversationRepairWorker = require('./workers/conversation-repair-worker');
const dedupSubstrate = require('../dedup');

let _started = false;
let _governance = null;

/**
 * Start the repair substrate. Subscribes to EXECUTE_CONVERSATION_REPAIR events.
 *
 * @param {object} governance — CK module (subscribeAction, dispatch, governedRead)
 */
function start(governance) {
  if (_started) return;
  _started = true;
  _governance = governance;

  governance.subscribeAction('EXECUTE_CONVERSATION_REPAIR', async (action) => {
    const { threadId, accountId, igUserId, pageToken, pageId } = action;
    if (!threadId || !accountId) return;

    // ── Dedup: skip if already repairing this threadId ──────────────────
    const { blocked } = await dedupSubstrate.isInFlight(
      accountId, 'repair_conversation', threadId
    );
    if (blocked) return;
    await dedupSubstrate.markInFlight(
      accountId, 'repair_conversation', threadId
    );

    // ── Factory-create worker, execute ──────────────────────────────────
    try {
      const worker = new ConversationRepairWorker();
      const result = await worker.execute({
        threadId, accountId, igUserId, pageToken, pageId,
      }, governance);

      // ── Completion signal ────────────────────────────────────────────
      if (_governance) {
        _governance.dispatch({
          type: 'REPAIR_CONVERSATION_COMPLETE',
          threadId,
          accountId,
          uuid: result.uuid,
          recovered: result.recovered,
        });
      }
    } catch (err) {
      console.warn(`[conversation-repair] Repair failed for ${threadId}:`, err.message);
    }
  });
}

module.exports = { start };
