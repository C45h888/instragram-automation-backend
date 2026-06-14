// Phase 9 — Recorder Observer (PASSIVE).
//
// The recorder is a SUBSCRIBER, not an actor. It reads from the
// observability plane's EventRecorder (the canonical timeline
// for runtime events) and projects each event into the recorder
// bucket shape for constitutional assertions.
//
// This module exposes ONLY observation methods. There is no
// .ingress() / .governance() / .fsm() / .worker() / .mutation()
// write API. Phase 9 tests cannot fabricate constitutional
// events; they can only assert against what the runtime did.
//
// Event vocabulary (mapped from runtime's actual emission):
//   source: 'acquisition' + payload.kind in {WEBHOOK_EVENT_*,
//     PERSIST_STAGED_*, *_PERSISTED, *_PERSIST_FAILED, _STAGED,
//     _DISPATCHED, _EXECUTING, _COMPLETE} → kind: worker
//     (the substrate staged the event into the worker pipeline)
//   source: 'governance' OR payload.kind: 'divergence' → kind:
//     governance (CK validation, dispatch, anomaly)
//   type: 'STATE_TRANSITION' (general, no specific source match)
//     → kind: fsm (FSM transition or lifecycle change)
//   type: 'MUTATION' / 'DB_WRITE_*' / 'UPSERT' → kind: mutation
//   everything else → kind: ingress (raw runtime ingress)

export class RecorderObserver {
  constructor() {
    this._attached = false;
    this._simulator = null;
  }

  attach(simulator) {
    if (this._attached) return;
    this._simulator = simulator;
    this._attached = true;
  }

  /**
   * Returns the observation log — a flat list of events captured
   * from the observability plane's EventRecorder timeline. Each
   * entry has: event_id, kind, source, payload, ts.
   *
   * The event_id is the runtime's correlationId when present,
   * otherwise a synthetic evt-N. Kind is inferred from the
   * runtime's actual event vocabulary (see header).
   */
  snapshot() {
    if (!this._simulator) return [];
    const timeline = this._simulator.timeline();
    const mutations = this._simulator.mutations ? this._simulator.mutations() : [];
    const observed = [];
    for (const e of timeline) observed.push(this._normalize(e));
    for (const m of mutations) {
      observed.push({
        event_id: m.correlationId || m.entry?.entityId || `mut-${m.id}`,
        kind: 'mutation',
        source: m.source || m.entry?.domain || 'mutation-substrate',
        payload: m.entry || {},
        ts: m.timestamp,
      });
    }
    return observed;
  }

  _normalize(e) {
    const id = e.correlationId || `evt-${e.id}`;
    return {
      event_id: id,
      kind: this._inferKind(e),
      source: e.source || 'runtime',
      payload: e.payload || null,
      ts: e.timestamp,
    };
  }

  _inferKind(e) {
    // The EventRecorder's normalized entries have:
    //   e.type = 'STATE_TRANSITION' (always — this is the
    //           observability plane's normalized envelope type)
    //   e.payload.type or e.payload.intent = the actual event
    //           type the substrate emitted (e.g. 'WEBHOOK_EVENT_STAGED')
    //   e.source = the kernel source (e.g. 'acquisition')
    const topType = (e.type || '').toUpperCase();
    const recorderKind = (e.kind || '').toUpperCase();
    const source = (e.source || '').toLowerCase();
    const payloadType = (e.payload?.type || '').toUpperCase();
    const payloadIntent = (e.payload?.intent || '').toUpperCase();
    const payloadKind = (e.payload?.kind || '').toUpperCase();
    const payloadEntryType = (e.payload?.entryType || '').toUpperCase();

    // Combine the signal: the actual semantic type of the
    // event is payload.type, payload.intent, or (rarely) e.type.
    const semanticType = payloadType || payloadIntent || topType;

    // Mutation: explicit MUTATION / DB_WRITE / UPSERT.
    if (semanticType.includes('MUTATION') || semanticType.includes('DB_WRITE') || semanticType.includes('UPSERT') || payloadEntryType.includes('DB_WRITE') || topType.includes('MUTATION') || topType.includes('DB_WRITE')) {
      return 'mutation';
    }

    // Worker: substrate-originated webhook lifecycle kinds.
    // The substrate's emission lands in e.payload.type or
    // e.payload.intent.
    const workerLifecycleKinds = [
      'WEBHOOK_EVENT_RECEIVED', 'WEBHOOK_EVENT_STAGED', 'WEBHOOK_EVENT_PERSISTED',
      'WEBHOOK_EVENT_PERSIST_FAILED', 'WEBHOOK_EVENT_DISCARDED',
      'PERSIST_STAGED_EVENT', 'PARSING_DISPATCHED', 'PARSING_COMPLETE',
      'ACQUISITION_INTENT_RECEIVED', 'ACQUISITION_EXECUTING', 'ACQUISITION_COMPLETE',
      'PUBLISH_REQUESTED', 'PUBLISH_EXECUTING', 'PUBLISH_COMPLETE',
      'WORKER_OUTCOME_REPORTED',
    ];
    if (workerLifecycleKinds.some((k) => semanticType.includes(k))) {
      return 'worker';
    }

    // Worker: explicit WORKER_* at top level.
    if (topType.includes('WORKER')) return 'worker';

    // Governance: source is governance/CK, or payload is divergence.
    if (source === 'governance' || source === 'constitutional-kernel' || source === 'ck') return 'governance';
    if (payloadKind === 'DIVERGENCE' || payloadKind === 'ANOMALY') return 'governance';
    if (recorderKind === 'DIVERGENCE' || recorderKind === 'ANOMALY') return 'governance';
    if (semanticType.includes('VALIDATE') || semanticType.includes('GOVERN') || semanticType.includes('DISPATCH') || semanticType.includes('PERSIST')) return 'governance';

    // FSM: state transitions without a specific worker/governance signature.
    if (topType.includes('STATE_TRANSITION') || semanticType.includes('FSM') || semanticType.includes('TRANSITION')) return 'fsm';

    // Default: treat raw events as ingress.
    return 'ingress';
  }

  /**
   * Return the recorder-shape summary — the legacy phase-8 view
   * derived from the runtime's actual events. Each event_id is
   * summarized with the first ts of each kind.
   */
  summarize() {
    const obs = this.snapshot();
    const byId = new Map();
    for (const o of obs) {
      if (!byId.has(o.event_id)) {
        byId.set(o.event_id, {
          event_id: o.event_id,
          ingress_ts: null,
          governance_ts: null,
          fsm_ts: null,
          worker_count: 0,
          mutation_count: 0,
        });
      }
      const e = byId.get(o.event_id);
      if (o.kind === 'ingress') e.ingress_ts = e.ingress_ts || o.ts;
      else if (o.kind === 'governance') e.governance_ts = e.governance_ts || o.ts;
      else if (o.kind === 'fsm') e.fsm_ts = e.fsm_ts || o.ts;
      else if (o.kind === 'worker') e.worker_count += 1;
      else if (o.kind === 'mutation') e.mutation_count += 1;
    }
    for (const e of byId.values()) {
      const t = [e.ingress_ts, e.governance_ts, e.fsm_ts];
      e.ordering_ok = t.every((x) => x != null) ? (t[0] <= t[1] && t[1] <= t[2]) : null;
    }
    return Array.from(byId.values());
  }
}
