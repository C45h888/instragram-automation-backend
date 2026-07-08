// control-plane/governance/probes/webview-transition.probe.js
// Probe for WebView-origin transitions (Pass 7 / S5 consumer-side).
//
// Reads-only the producer's XADD payload (see runtime/src-tauri/
// src/redis/commands.rs:147-166) and validates:
//   1. The `transition_id` field is present + non-empty (producer
//      contract).
//   2. The `domain` field is a known DomainId (13 closed values
//      per runtime/src-tauri/lib/fsm/contracts/domain.ts).
//   3. The (from_state, event, to_state) triple is in the rules
//      table for that domain.
//
// The rules table is FORWARD-PORTED (frozen copy) from the
// WebView FSM's `RULES` arrays. The forward-port is hashed with
// SHA-256; EXPORTED_HASH is the CI-side drift check (per spec D5).
// A vitest in tests/phase-X-webview-probe-hash.test.js asserts
// this hash against a recomputed value of the WebView-side
// rules table on CI; drift = stale probe = reject-everything
// until the port is updated.
//
// Per spec Pass 7 invariant I35: the probe MUST NOT import from
// the WebView repo. The forward-port is the legal read surface.

// ── DomainId closed set (forward-ported from contracts/domain.ts) ──
const DOMAIN_IDS = Object.freeze([
  'analytics-reports',
  'scheduled-posts',
  'alerts',
  'activity-feed',
  'attribution',
  'queue-monitor',
  'health',
  'consent',
  'privacy',
  'business-accounts',
  'auth',
  'content',
  'chat',
]);
const DOMAIN_SET = new Set(DOMAIN_IDS);

// ── Rules table (forward-port from state/*.ts RULES arrays) ────────
// Each domain has a Map: `<from>\x1f<event>` → `to`.
// The compound key is a NUL-byte-separated pair to avoid collisions
// on (from='IDLE_m', event='ount') vs (from='IDLE', event='mount').
// Key collisions on real domain IDs are statistically zero.

const RULES_TABLE = Object.freeze({
  'analytics-reports': {
    'IDLE\x1fmount': 'POLLING',
    'POLLING\x1ffetch-success': 'READY',
    'POLLING\x1ffetch-error': 'ERROR',
    'READY\x1frefetch': 'POLLING',
    'READY\x1fheartbeat-down': 'DEGRADED',
    'POLLING\x1fheartbeat-down': 'DEGRADED',
  },
  'scheduled-posts': {
    'IDLE\x1fmount': 'FETCHING',
    'FETCHING\x1ffetch-success': 'READY',
    'FETCHING\x1ffetch-error': 'ERROR',
    'READY\x1frefetch': 'FETCHING',
    'READY\x1fschedule-post': 'SUBMITTING',
    'SUBMITTING\x1fsubmit-success': 'READY',
    'SUBMITTING\x1fsubmit-error': 'ERROR',
    'ERROR\x1fretry': 'FETCHING',
    'READY\x1fheartbeat-down': 'DEGRADED',
    'FETCHING\x1fheartbeat-down': 'DEGRADED',
    'SUBMITTING\x1fheartbeat-down': 'DEGRADED',
    'ERROR\x1fheartbeat-down': 'DEGRADED',
  },
  'alerts': {
    'IDLE\x1fmount': 'LOADING',
    'LOADING\x1ffetch-success': 'READY',
    'LOADING\x1ffetch-error': 'ERROR',
    'READY\x1fheartbeat-down': 'DEGRADED',
    'LOADING\x1fheartbeat-down': 'DEGRADED',
  },
  'activity-feed': {
    'IDLE\x1fmount': 'LOADING',
    'LOADING\x1ffetch-success': 'READY',
    'LOADING\x1ffetch-error': 'ERROR',
    'READY\x1frefetch': 'LOADING',
    'READY\x1fheartbeat-down': 'DEGRADED',
    'LOADING\x1fheartbeat-down': 'DEGRADED',
  },
  'attribution': {
    'IDLE\x1fmount': 'LOADING_QUEUE',
    'LOADING_QUEUE\x1fqueue-success': 'LOADING_MODEL',
    'LOADING_MODEL\x1fmodel-success': 'COMPUTING',
    'COMPUTING\x1fcompute-success': 'READY',
    'LOADING_QUEUE\x1fqueue-error': 'ERROR',
    'LOADING_MODEL\x1fmodel-error': 'ERROR',
    'COMPUTING\x1fcompute-error': 'ERROR',
    'READY\x1frefetch': 'LOADING_QUEUE',
    'ERROR\x1fretry': 'LOADING_QUEUE',
    'READY\x1fheartbeat-down': 'DEGRADED',
    'LOADING_QUEUE\x1fheartbeat-down': 'DEGRADED',
    'LOADING_MODEL\x1fheartbeat-down': 'DEGRADED',
    'COMPUTING\x1fheartbeat-down': 'DEGRADED',
    'ERROR\x1fheartbeat-down': 'DEGRADED',
  },
  'queue-monitor': {
    'IDLE\x1fmount': 'POLLING',
    'POLLING\x1ffetch-success': 'READY',
    'POLLING\x1ffetch-error': 'ERROR',
    'READY\x1fretry-start': 'POLLING',
    'READY\x1fheartbeat-down': 'DEGRADED',
    'POLLING\x1fheartbeat-down': 'DEGRADED',
  },
  'health': {
    'IDLE\x1fmount': 'POLLING',
    'POLLING\x1fcheck-success': 'LIVE',
    'POLLING\x1fcheck-error': 'ERROR',
    'LIVE\x1ftick': 'POLLING',
    'LIVE\x1fdegraded-signal': 'DEGRADED',
    'DEGRADED\x1ftick': 'POLLING',
    'DEGRADED\x1frecovered': 'LIVE',
    'LIVE\x1fheartbeat-down': 'DEGRADED',
    'POLLING\x1fheartbeat-down': 'DEGRADED',
    'DEGRADED\x1fheartbeat-down': 'DEGRADED',
  },
  'consent': {
    'IDLE\x1fconsent-check-start': 'CHECKING',
    'CHECKING\x1fconsent-check-ok': 'COMPLIANT',
    'CHECKING\x1fconsent-check-fail': 'NON_COMPLIANT',
    'COMPLIANT\x1frevoke': 'REVOKED',
    'NON_COMPLIANT\x1fgrant': 'COMPLIANT',
    'COMPLIANT\x1fheartbeat-down': 'DEGRADED',
    'CHECKING\x1fheartbeat-down': 'DEGRADED',
    'NON_COMPLIANT\x1fheartbeat-down': 'DEGRADED',
    'REVOKED\x1fheartbeat-down': 'DEGRADED',
  },
  'privacy': {
    'IDLE\x1fheartbeat-down': 'DEGRADED',
    'DELETING\x1fheartbeat-down': 'DEGRADED',
    'IDLE\x1fdelete-start': 'DELETING',
    'DELETING\x1fdelete-success': 'IDLE',
    'DELETING\x1fdelete-error': 'ERROR',
  },
  'business-accounts': {
    'IDLE\x1fmount': 'LOADING',
    'LOADING\x1ffetch-success': 'READY',
    'LOADING\x1ffetch-error': 'ERROR',
    'READY\x1fselect': 'READY',
    'READY\x1frefetch': 'LOADING',
    'READY\x1fheartbeat-down': 'DEGRADED',
    'LOADING\x1fheartbeat-down': 'DEGRADED',
  },
  'auth': {
    'IDLE\x1fsignin-start': 'AUTHENTICATING',
    'AUTHENTICATING\x1fsignin-success': 'AUTHENTICATED',
    'AUTHENTICATING\x1fsignin-error': 'IDLE',
    'AUTHENTICATED\x1fsignout-start': 'SIGNING_OUT',
    'SIGNING_OUT\x1fsignout-success': 'IDLE',
    'AUTHENTICATED\x1ftoken-refresh': 'AUTHENTICATING',
    'AUTHENTICATED\x1fheartbeat-down': 'DEGRADED',
    'AUTHENTICATING\x1fheartbeat-down': 'DEGRADED',
  },
  'content': {
    'IDLE\x1ffetch-start': 'LOADING',
    'LOADING\x1ffetch-success': 'READY',
    'LOADING\x1ffetch-error': 'ERROR',
    'ERROR\x1frefetch': 'LOADING',
    'READY\x1frefetch': 'LOADING',
    'READY\x1fheartbeat-down': 'DEGRADED',
    'LOADING\x1fheartbeat-down': 'DEGRADED',
  },
  'chat': {
    'IDLE\x1fpersist-user-msg': 'SENDING',
    'SENDING\x1fopen-stream': 'STREAMING',
    'STREAMING\x1fstream-done': 'DONE',
    'STREAMING\x1fstream-error': 'ERROR',
    'DONE\x1freset': 'IDLE',
    'ERROR\x1freset': 'IDLE',
    'IDLE\x1fheartbeat-down': 'DEGRADED',
    'SENDING\x1fheartbeat-down': 'DEGRADED',
    'STREAMING\x1fheartbeat-down': 'DEGRADED',
    'DONE\x1fheartbeat-down': 'DEGRADED',
    'ERROR\x1fheartbeat-down': 'DEGRADED',
  },
});

const K = '\x1f'; // separator for compound rule keys

// ── SHA-256 of the forward-port (D5 drift detection) ───────────────
// crypto is a builtin; no new dependency.
const crypto = require('crypto');
function _hashRules() {
  // Stable serialisation: keys sorted, values joined by newline.
  const domains = Object.keys(RULES_TABLE).sort();
  const parts = [];
  for (const d of domains) {
    const ruleKeys = Object.keys(RULES_TABLE[d]).sort();
    for (const k of ruleKeys) {
      const to = RULES_TABLE[d][k];
      const [from, event] = k.split(K);
      // Canonical form: domain|from|event|to  (one per line)
      parts.push([d, from, event, to].join('|'));
    }
  }
  const text = parts.join('\n');
  return crypto.createHash('sha256').update(text, 'utf8').digest('hex');
}

const EXPORTED_HASH = _hashRules();

// ── Probe result type (per the consumer-side convention) ──────────
function probeSuccess(data, queriedAtEpochMs) {
  return { ok: true, data, error: null, queriedAtEpochMs };
}
function probeFailure(error, queriedAtEpochMs) {
  return { ok: false, data: null, error, queriedAtEpochMs };
}

/**
 * probeWebviewTransition — the read-only oracle for the FSM guard.
 *
 * @param {object} transition — the parsed XADD entry shape
 *   { transition_id, correlation_id, domain, from_state,
 *     to_state, event, occurred_at_epoch_ms }
 * @returns {ProbeResult<{...}>}
 */
function probeWebviewTransition(transition) {
  const queriedAtEpochMs = Date.now();

  if (!transition || typeof transition !== 'object') {
    return probeFailure('webview probe: missing transition object', queriedAtEpochMs);
  }

  // 1. transition_id contract
  if (typeof transition.transition_id !== 'string' || transition.transition_id.length === 0) {
    return probeFailure('webview probe: missing or empty transition_id',
      queriedAtEpochMs);
  }

  // 2. DomainId closed-set check
  if (typeof transition.domain !== 'string' || !DOMAIN_SET.has(transition.domain)) {
    return probeFailure(`webview probe: unknown domain '${transition.domain}'`,
      queriedAtEpochMs);
  }

  // 3. (from, event, to) legality check
  const domainRules = RULES_TABLE[transition.domain];
  const from = transition.from_state;
  const event = transition.event;
  const to = transition.to_state;

  if (typeof from !== 'string' || typeof event !== 'string' || typeof to !== 'string') {
    return probeFailure('webview probe: missing from_state/event/to_state',
      queriedAtEpochMs);
  }

  // heartbeat-down is a universal rule: any state can transition to
  // DEGRADED on heartbeat-down. The forward-port encodes this via
  // explicit entries; if a domain's rules-table doesn't include the
  // pair, we still accept heartbeat-down → DEGRADED as a safety net.
  let isLegal = false;
  let ruleFingerprint;

  const key = from + K + event;
  const expectedTo = domainRules[key];
  if (expectedTo !== undefined) {
    if (expectedTo === to) {
      isLegal = true;
      ruleFingerprint = `${transition.domain}:${from}:${event}:${to}`;
    }
  } else if (event === 'heartbeat-down' && to === 'DEGRADED') {
    // Universal fallback — every envelope accepts a heartbeat-down.
    isLegal = true;
    ruleFingerprint = `${transition.domain}:HEARTBEAT_DOWN:DEGRADED`;
  }

  if (!isLegal) {
    return probeFailure(
      `webview probe: illegal transition ${from}→${to} via ${event} for domain ${transition.domain}`,
      queriedAtEpochMs);
  }

  // 4. occurred_at_epoch_ms sanity (parseable, not zero)
  if (typeof transition.occurred_at_epoch_ms !== 'number'
      || !Number.isFinite(transition.occurred_at_epoch_ms)
      || transition.occurred_at_epoch_ms <= 0) {
    return probeFailure('webview probe: invalid occurred_at_epoch_ms',
      queriedAtEpochMs);
  }

  return probeSuccess({
    transition_id: transition.transition_id,
    correlation_id: transition.correlation_id ?? null,
    domain: transition.domain,
    from_state: from,
    to_state: to,
    event,
    occurred_at_epoch_ms: transition.occurred_at_epoch_ms,
    isKnownDomain: true,
    isLegalTransition: true,
    ruleFingerprint,
    // Exposed for downstream DRIFT checks; the FSM forward-ports
    // and trusts this hash until CI disagrees.
    rulesTableHash: EXPORTED_HASH,
  }, queriedAtEpochMs);
}

module.exports = {
  probeWebviewTransition,
  // Exported for tests + D5 CI hash check.
  DOMAIN_IDS,
  DOMAIN_SET,
  RULES_TABLE,
  EXPORTED_HASH,
  K_SEPARATOR: K,
  _hashRules,
};
