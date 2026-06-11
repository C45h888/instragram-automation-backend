// graph-capability-kernel/substrates/workers/escalation-worker.js
// Escalation Worker — handles unrecoverable conditions after automated
// recovery pathways have been exhausted.
//
// Consumes: ig-reliability-substrate §15 recommendations (ESCALATE_*),
//           §16 severity (CRITICAL), repeated failure counters from
//           other workers
//
// Owns:
//   - Escalation threshold checking:
//     3 consecutive token failures → escalate
//     5 permission failures → escalate
//     Quota lockout > 1h → escalate
//     Webhook FAILED > 30m → escalate
//     Dependency CIRCUIT_OPEN > 1h → escalate
//   - Operator alert generation (writes to system_alerts table)
//   - Account marking for manual intervention
//   - Escalation history tracking per account
//
// Does NOT own:
//   - Recovery attempts (domain workers own)
//   - Policy decisions on WHEN to escalate (FSM owns)
//   - Alert delivery (writes to DB, external notification system reads)
//
// States: PASSIVE → ESCALATING → ESCALATED → ACKNOWLEDGED
//
// Never: escalates unless upstream worker explicitly reports exhaustion.
//
// Membrane interface: start(governance), stop(), isStarted()

const fsm = require('../../fsm');
const signalDispatch = require('../vault/signal-dispatch');

// ── Escalation thresholds ─────────────────────────────────────────────────

const THRESHOLDS = {
  token_failure:         { maxConsecutive: 3,  windowMs: 24 * 60 * 60 * 1000 },
  permission_failure:    { maxConsecutive: 5,  windowMs: 24 * 60 * 60 * 1000 },
  quota_lockout:         { maxDurationMs:    60 * 60 * 1000 },  // 1h
  webhook_failed:        { maxDurationMs:    30 * 60 * 1000 },  // 30m
  dependency_circuit:    { maxDurationMs:    60 * 60 * 1000 },  // 1h
  account_restriction:   { maxConsecutive: 1,  windowMs: 0 },   // immediate
  publishing_failure:    { maxConsecutive: 3,  windowMs: 60 * 60 * 1000 },
};

// ── Escalation Worker ─────────────────────────────────────────────────────

class EscalationWorker {
  constructor() {
    this._started = false;
    this._governance = null;

    // Escalation state per account: baId → { state, lastEscalatedAt, escalatedCount, activeEscalations }
    this._escalationState = new Map();

    // Failure counters per account per type: baId:type → { count, firstAt, lastAt }
    this._failureCounters = new Map();
  }

  // ── Membrane interface ──────────────────────────────────────────────────

  start(governance) {
    if (this._started) return;
    this._started = true;
    this._governance = governance;

    governance.subscribeAction('ESCALATION_RECEIVED', (action) => {
      this.receiveEscalation(action).catch(err => {
        console.error('[escalation] ESCALATION_RECEIVED failed:', err.message);
      });
    });

    governance.subscribeAction('ESCALATION_ACKNOWLEDGED', (action) => {
      this.acknowledgeEscalation(action).catch(err => {
        console.error('[escalation] ESCALATION_ACKNOWLEDGED failed:', err.message);
      });
    });

    console.log('[escalation] Membrane wired — subscribed to ESCALATION_RECEIVED, ESCALATION_ACKNOWLEDGED');
  }

  stop() {
    this._started = false;
  }

  isStarted() {
    return this._started;
  }

  // ── Public: receive escalation from another worker ──────────────────────

  /**
   * Receive an escalation event from a domain worker. Checks thresholds,
   * increments counters, and if thresholds are exceeded, generates an
   * operator alert and transitions to ESCALATED.
   *
   * @param {{ businessAccountId: string, userId: string, escalationType: string,
   *           reason: string, details: object }} action
   */
  async receiveEscalation(action = {}) {
    const { businessAccountId, userId, escalationType, reason, details } = action;

    if (!businessAccountId || !escalationType) {
      console.warn('[escalation] Received escalation without businessAccountId or escalationType');
      return { escalated: false, reason: 'missing_fields' };
    }

    // Check thresholds
    const threshold = THRESHOLDS[escalationType];
    if (!threshold) {
      console.warn(`[escalation] Unknown escalation type: ${escalationType}`);
      return { escalated: false, reason: `unknown_type:${escalationType}` };
    }

    // Increment failure counter
    const counterKey = `${businessAccountId}:${escalationType}`;
    const counter = this._failureCounters.get(counterKey) || {
      count: 0,
      firstAt: Date.now(),
      lastAt: Date.now(),
      windowMs: threshold.windowMs,
    };

    // Reset if window expired
    if (counter.windowMs > 0 && (Date.now() - counter.firstAt) > counter.windowMs) {
      counter.count = 0;
      counter.firstAt = Date.now();
    }

    counter.count++;
    counter.lastAt = Date.now();
    this._failureCounters.set(counterKey, counter);

    // Check if threshold exceeded
    let shouldEscalate = false;

    if (threshold.maxConsecutive && counter.count >= threshold.maxConsecutive) {
      shouldEscalate = true;
    }

    if (threshold.maxDurationMs && details?.durationMs && details.durationMs >= threshold.maxDurationMs) {
      shouldEscalate = true;
    }

    // Immediate escalation types (account_restriction, publishing_failure)
    if (escalationType === 'account_restriction') {
      shouldEscalate = true;
    }

    if (!shouldEscalate) {
      return { escalated: false, reason: 'below_threshold', currentCount: counter.count };
    }

    // Escalate
    const acctState = this._escalationState.get(businessAccountId) || {
      state: 'PASSIVE',
      lastEscalatedAt: null,
      escalatedCount: 0,
      activeEscalations: [],
    };

    acctState.state = 'ESCALATING';
    acctState.lastEscalatedAt = new Date().toISOString();
    acctState.escalatedCount++;
    acctState.activeEscalations.push({
      type: escalationType,
      reason,
      details,
      escalatedAt: new Date().toISOString(),
      acknowledged: false,
    });
    this._escalationState.set(businessAccountId, acctState);

    // Write alert to system_alerts (escalation marker)
    fsm.requestDBWrite({
      table: 'system_alerts',
      operation: 'insert_alert',
      accountId: businessAccountId,
      rows: [{
        alert_type: `escalation_${escalationType}`,
        business_account_id: businessAccountId,
        message: `ESCALATION: ${escalationType} — ${reason}. Manual intervention required.`,
        details: {
          escalationType,
          reason,
          failureCount: counter.count,
          threshold: threshold.maxConsecutive || threshold.maxDurationMs,
          escalationCount: acctState.escalatedCount,
          details,
        },
        resolved: false,
      }],
    });

    // Emit envelope — account requires intervention
    const env = fsm.newEnvelope({ businessAccountId, userId });
    env.detection = {
      isValid: false,
      reason: `escalated:${escalationType}`,
      details: { escalationType, reason, count: counter.count },
    };
    signalDispatch.emitEnvelope({ envelope: env });

    acctState.state = 'ESCALATED';
    this._escalationState.set(businessAccountId, acctState);

    console.warn(`[escalation] ESCALATED: ${businessAccountId} — ${escalationType} — ${reason}`);

    return {
      escalated: true,
      escalationType,
      businessAccountId,
      failureCount: counter.count,
      state: acctState.state,
    };
  }

  // ── Public: acknowledge escalation ──────────────────────────────────────

  async acknowledgeEscalation(action = {}) {
    const { businessAccountId, escalationType } = action;

    if (!businessAccountId) return { acknowledged: false };

    const acctState = this._escalationState.get(businessAccountId);
    if (!acctState) return { acknowledged: false, reason: 'no_escalation_state' };

    // Mark matching escalation as acknowledged
    for (const esc of acctState.activeEscalations) {
      if (!escalationType || esc.type === escalationType) {
        esc.acknowledged = true;
        esc.acknowledgedAt = new Date().toISOString();
      }
    }

    // If all are acknowledged, reset to PASSIVE
    const allAcknowledged = acctState.activeEscalations.every(e => e.acknowledged);
    if (allAcknowledged) {
      acctState.state = 'ACKNOWLEDGED';
    }

    this._escalationState.set(businessAccountId, acctState);

    // Reset failure counters for resolved escalation types
    const counterKey = `${businessAccountId}:${escalationType}`;
    this._failureCounters.delete(counterKey);

    return { acknowledged: true, state: acctState.state };
  }
}

module.exports = EscalationWorker;
