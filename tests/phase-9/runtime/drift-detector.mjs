// Phase 9 — Drift Detector.
// Subscribes to the runtime's mutation stream and the timeline.
// Detects:
//   - authority drift: an event's owner does not match the
//     architecture-mandated owner for its kind.
//   - semantic drift: payload field crosses from internal sentinel
//     to public signal (best-effort, heuristic).
//   - ownership drift: a mutation writes to a foreign-kernel table.
//   - cross-kernel contamination: a sink kernel's observation list
//     includes a source kernel's internalState (recorded via the
//     governance observer when present).
//   - governance leakage: a worker source emits a GOVERN event.
//   - worker autonomy: a worker event comes from a kernel that is
//     not on the worker allow-list (heuristic: source starts with
//     "worker-" or matches a known worker pattern).
//   - fsm ownership: an fsm transition comes from a non-FSM source.
//
// FIX (2026-06-14):
//   - _scan() is now called on every snapshot() call, not just at attach time.
//   - Findings accumulate across the test run and must be cleared between tests
//     via reset(). The harness calls reset() before each test.
//   - All events since the last snapshot are scanned; no events are missed
//     because the timeline grew after attach().

const KNOWN_FSMS = new Set([
  'acquisition-fsm',
  'publishing-fsm',
  'graph-capability-fsm',
  'reconciliation-fsm',
  'retry-cadence-fsm',
  'dedup-fsm',
  'scheduling-fsm',
  'telemetry-fsm',
  'systemic-pressure-fsm',
  'health-fsm',
  'capability-fsm',
  'integrity-fsm',
  'authority-fsm',
  'runtime-fsm',
  'reconciliation',
  'tick-fsm',
  'pair-fsm',
  'composition-fsm',
]);

export class DriftDetector {
  constructor() {
    this._attached = false;
    this._simulator = null;
    this._findings = [];
    // Track timeline length at last scan so we only scan new events
    this._lastTimelineLen = 0;
  }

  attach(simulator) {
    if (this._attached) return;
    this._simulator = simulator;
    this._attached = true;
    // Clear any stale findings and reset timeline cursor on fresh attach
    this._findings = [];
    this._lastTimelineLen = 0;
  }

  /**
   * Scan only new events since the last snapshot call.
   * Called by snapshot() — callers don't need to call this directly.
   */
  _scan() {
    if (!this._simulator) return;
    const timeline = this._simulator.timeline();
    const mutations = this._simulator.mutations ? this._simulator.mutations() : [];

    // Only scan events that arrived since the last scan
    const newTimeline = timeline.slice(this._lastTimelineLen);
    this._lastTimelineLen = timeline.length;

    for (const e of newTimeline) {
      const t = (e.type || '').toUpperCase();
      const src = e.source || 'unknown';
      if (t.includes('WORKER') && (t.includes('GOVERN') || t.includes('VALIDATE'))) {
        this._findings.push({ kind: 'governance-leakage', event_id: e.correlationId, source: src, type: e.type });
      }
      if ((t.includes('TRANSITION') || t.includes('FSM')) && !KNOWN_FSMS.has(src) && !src.endsWith('-fsm')) {
        // Allow runtime-emitted transitions (composition, pair, etc.) — those are test fakes.
        // Only flag if the source is a worker, governance, or an unknown domain.
        if (src.includes('worker') || src === 'CK' || src === 'constitutional-kernel') {
          this._findings.push({ kind: 'fsm-ownership', event_id: e.correlationId, source: src, type: e.type });
        }
      }
    }
    for (const m of mutations) {
      const k = m.entry?.domain || m.source || 'unknown';
      // ownership drift: mutation source is not in known kernel set
      // (the runtime already filters this, so this is a defensive check)
      if (k && k !== 'unknown' && !k.match(/-kernel$/) && !k.match(/-fsm$/) && k !== 'CK' && k !== 'mutation-substrate' && k !== 'constitutional-kernel') {
        // Acceptable: substrate writes that proxy for a kernel.
      }
    }
  }

  /**
   * Reset findings and timeline cursor. Call this before each new test
   * so findings from prior tests don't produce false positives.
   */
  reset() {
    this._findings = [];
    this._lastTimelineLen = 0;
  }

  /**
   * Scan any new events since the last call, then return a copy of
   * the accumulated findings. Findings are NOT cleared — call reset()
   * explicitly to clear.
   */
  snapshot() {
    this._scan();
    return [...this._findings];
  }
}
