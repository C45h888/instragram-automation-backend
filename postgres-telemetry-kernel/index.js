// postgres-telemetry-kernel/index.js
// Postgres Telemetry Kernel — domain data write plane.
//
// Constitutional position:
//   Execution layer beneath the CK (Constitutional Kernel). This kernel
//   owns domain-specific Postgres writers for data acquired by the
//   acquisition kernel. Writers are semantically blind — they execute
//   deterministic writes and emit completion/failure events. The FSM
//   owns state transitions. The CK owns routing.
//
// Chain:
//   acquisition-kernel → parser → governance.dispatch(DB_WRITE_REQUESTED)
//       → CK routes by domain → this kernel's writer
//       → writer.execute(event, ctx) → Postgres
//       → ctx.emit(DB_WRITE_COMPLETE / DB_WRITE_FAILED)
//       → CK transitions FSM
//
// Does NOT own:
//   - CK routing (CK owns)
//   - State transitions (FSM owns)
//   - Error classification (bedrock owns)
//   - Data normalization (normalizer owns)
//   - Retry decisions (retry-cadence-kernel owns)

const writers = require('./writers');
const bedrock = require('./bedrock');

module.exports = {
    writers,
    getWriter: writers.getWriter,
    hasWriter: writers.hasWriter,
    getRegisteredDomains: writers.getRegisteredDomains,

    // Canonical persistence abstraction — the ONLY surface workers should touch
    // for all Supabase operations. Domain facades: ugc, publishing, insights, token.
    bedrock,
};
