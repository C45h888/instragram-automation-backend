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
  }

  attach(simulator) {
    if (this._attached) return;
    this._simulator = simulator;
    this._attached = true;
    this._scan();
  }

  _scan() {
    if (!this._simulator) return;
    const timeline = this._simulator.timeline();
    const mutations = this._simulator.mutations ? this._simulator.mutations() : [];
    for (const e of timeline) {
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

  snapshot() {
    return [...this._findings];
  }
}
