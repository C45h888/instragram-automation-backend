// backend.api/services/tokens/index.js
// Zero-logic re-export shim.
// All existing require('../services/instagram-tokens') can be updated to
// require('../services/tokens') and continue working unchanged.
//
// logAudit is re-exported from config/supabase for backward compat.
// Routes should migrate to importing logAudit from config/supabase directly (Task 11).
//
// ── DEPRECATION NOTICE (2026-06-04) ──────────────────────────────────────────
// This module is the legacy capability governance surface. It is being decomposed
// into the Graph Capability Plane (read-only observers) + Vault Plane (credential
// materialization) per the Graph Capability Plane Constitutional Decomposition Contract.
//
// Current status:
//   Phase 1+2 COMPLETE — graph-capability-fsm.js + workers exist as observer scaffolding.
//   Phase 3 PENDING    — substrate I/O (axios/vault/DB) not yet wired.
//   Phase 4 PENDING    — consumer migration (media.js, ugc.js, agent-helpers.js).
//   Phase 5 PENDING    — deletion of this module.
//
// Until Phase 4 completes, this module remains the active capability surface.
// New code MUST consume capability state via:
//   const fsm = require('../../control-plane/governance/domains/graph-capability-fsm');
//   const verdict = fsm.getCapabilityVerdict();
//
// Do NOT add new capability logic here. It belongs in substrates/graph-capability/.

const { logAudit } = require('../../config/supabase');

module.exports = {
  ...require('./detection'),
  ...require('./pat'),
  ...require('./uat'),
  ...require('./scope'),
  ...require('./base'),
  logAudit,
};
