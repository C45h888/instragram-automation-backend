// graph-capability-kernel/substrates/workers/evaluation-worker.js
// Evaluation Worker: closes the CAPABILITY_EVALUATION_STARTED loop.
//
// Constitutional role:
//   The FSM emits CAPABILITY_EVALUATION_STARTED when vault state changes
//   (CAPABILITY_EVALUATE transitions cred to UNKNOWN). This worker
//   subscribes to that signal and triggers re-inference by dispatching a
//   CAPABILITY_OBSERVATION with a minimal envelope.
//
//   Why a minimal envelope? The real observation data already exists in the
//   FSM's evidence store — the workers that triggered the state change
//   already called signalDispatch.emitEnvelope() with their findings. The
//   EvalWorker just needs to kick the inference engine so the FSM can
//   re-evaluate the merged evidence and transition to the correct state.
//
//   The FSM's _mergeAndInfer merges the new (minimal) envelope into the
//   existing evidence, preserving existing slot data (pat, uat, detection,
//   scope) and only updating the observedAt timestamp. Then inferStateFrom-
//   Envelope re-evaluates all populated slots and returns the correct state.
//
// Wiring:
//   Orchestrator's wire(governance) calls evaluationWorker.start(governance).
//   The orchestrator owns the semantic authority — the worker is passed through
//   it, not independently registered. This avoids semantic contamination of
//   the orchestrator's CAPABILITY_CHECK concern.

const fsm = require('../../fsm');

// ── Evaluation Worker ──────────────────────────────────────────────────────

class EvaluationWorker {
  constructor() {
    this._started = false;
    this._governance = null;
  }

  /**
   * Subscribe to CAPABILITY_EVALUATION_STARTED via the governance action fabric.
   * Called by the orchestrator's wire(governance) — the orchestrator owns
   * the semantic authority for evaluation lifecycle.
   */
  start(governance) {
    if (this._started) return;
    this._started = true;
    this._governance = governance;

    governance.subscribeAction('CAPABILITY_EVALUATION_STARTED', (action) => {
      const { businessAccountId, source } = action;
      if (!businessAccountId) {
        console.warn('[evaluation-worker] CAPABILITY_EVALUATION_STARTED rejected: missing businessAccountId');
        return;
      }

      console.log(`[evaluation-worker] Re-evaluating capability for account ${businessAccountId} (source: ${source || 'unknown'})`);

      try {
        // Build a minimal envelope — the FSM's _mergeAndInfer will merge
        // this into the existing evidence and re-infer state from all
        // populated slots (pat, uat, detection, scope). Existing data from
        // previous worker runs (emitEnvelope) is preserved and re-evaluated.
        const envelope = fsm.newEnvelope({ businessAccountId });
        envelope.detection = {
          source: source || 'evaluation',
          evaluatedAt: Date.now(),
        };

        this._governance.dispatch({
          type: 'CAPABILITY_OBSERVATION',
          envelope,
          businessAccountId,
        });
      } catch (err) {
        console.error(`[evaluation-worker] Re-evaluation failed for ${businessAccountId}:`, err.message);
      }
    });

    console.log('[evaluation-worker] Wired — subscribed to CAPABILITY_EVALUATION_STARTED');
  }

  stop() {
    if (!this._started) return;
    this._started = false;
  }

  isStarted() {
    return this._started;
  }
}

// ── Singleton ──────────────────────────────────────────────────────────────

const instance = new EvaluationWorker();
module.exports = instance;
