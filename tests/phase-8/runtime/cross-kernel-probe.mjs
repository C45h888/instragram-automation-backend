// Phase 8 — Cross-Kernel Probe.
// Harness that drives a (source_kernel → sink_kernel) pair and
// asserts the constitutional signal passed across the boundary
// contains only the projected public surface — no internal
// token tables, no private state, no leaked authority.
//
// 20 directed pairs are exercised: 5 kernels × 4 sinks each.

export class CrossKernelProbe {
  constructor() {
    this.sentinelSecret = `phase8_sentinel_${Date.now()}_${Math.random().toString(36).slice(2)}`;
  }

  buildPacket({ source, sink, publicSignal, internalState = {} }) {
    const internal = { ...internalState, __sentinel: this.sentinelSecret };
    return {
      packet_id: `pkt_${source}_${sink}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
      source_kernel: source,
      sink_kernel: sink,
      public_signal: publicSignal,
      internal_state: internal,
      produced_at: Date.now(),
    };
  }

  assertSentinelIsolated(sinkObservedPayloads) {
    const leaked = sinkObservedPayloads.filter((p) => {
      const text = JSON.stringify(p);
      return text.includes(this.sentinelSecret);
    });
    return {
      ok: leaked.length === 0,
      sentinel: this.sentinelSecret,
      leak_count: leaked.length,
      leaked_samples: leaked.slice(0, 3),
    };
  }

  static PAIRS() {
    const kernels = ['capability', 'acquisition', 'publishing', 'recovery', 'insights'];
    const pairs = [];
    for (const src of kernels) {
      for (const sink of kernels) {
        if (src === sink) continue;
        pairs.push({ source: src, sink });
      }
    }
    return pairs;
  }
}

const singleton = new CrossKernelProbe();

export const probe = singleton;
export default singleton;
