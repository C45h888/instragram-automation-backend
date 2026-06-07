// dedup-kernel/substrates/repair/conversation-repair.js
// Conversation Repair Substrate: mutation plane for missing conversations.
//
// Owns: CK subscription (EXECUTE_CONVERSATION_REPAIR), factory-create worker,
//        completion signal dispatch.
// Does NOT own: Graph API calls (worker), DB writes (delegates to CK/writers),
//               governance policy (CK + FSM), dedup gating (FSM).
//
// Constitutional flow:
//   messages-parser → CK(REPAIR_CONVERSATION) → dedup-fsm (dedup gate + emit)
//     → CK(EXECUTE_CONVERSATION_REPAIR)
//     → conversation-repair substrate → worker → REPAIR_CONVERSATION_COMPLETE
//
// Phase 7 (2026-06-07): Dedup gating moved to dedup FSM. The REPAIR_CONVERSATION
// event is now routed through CK → dedup FSM. The FSM checks dedup before
// emitting EXECUTE_CONVERSATION_REPAIR. This substrate no longer imports
// the dedup substrate — the FSM is the intelligence layer.

const ConversationRepairWorker = require('./workers/conversation-repair-worker');

let _started = false;
let _governance = null;

/**
 * Start the repair substrate. Subscribes to EXECUTE_CONVERSATION_REPAIR events.
 * Dedup gating is handled by the dedup FSM BEFORE this action is emitted.
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

    // ── Dedup already checked by dedup FSM — no isInFlight/markInFlight here ──

    // ── Factory-create worker, execute ──────────────────────────────────
    try {
      const worker = new ConversationRepairWorker();
      const result = await worker.execute({
        threadId, accountId, igUserId, pageToken, pageId,
      }, governance);

      // ── Completion signal → CK → dedup FSM ──────────────────────────
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
      // Report failure to FSM
      if (_governance) {
        _governance.dispatch({
          type: 'REPAIR_CONVERSATION_COMPLETE',
          threadId,
          accountId,
          uuid: null,
          recovered: 0,
          error: err.message,
        });
      }
    }
  });
}

module.exports = { start };
