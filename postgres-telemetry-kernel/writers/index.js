// postgres-telemetry-kernel/writers/index.js
// Domain writer registry — maps domain names to bounded Postgres writers.
//
// Each writer is a semantically blind execution module. Writers receive
// DB_WRITE_REQUESTED events from the CK and execute deterministic Postgres
// writes. They emit DB_WRITE_COMPLETE or DB_WRITE_FAILED back to the CK.
//
// Pattern:
//   CK receives DB_WRITE_REQUESTED { domain: 'insights', ... }
//   CK looks up writer via getWriter(event.domain)
//   CK invokes writer.execute(event, ctx)
//   Writer writes → emits completion via ctx.emit(completionEvent)
//   CK transitions FSM: WRITER_DISPATCHED → DB_WRITE_COMPLETE → IDLE

const insightsDomainWriter = require('./insights-domain-writer');

// Domain → writer map. Add new domains here as writers are built.
const DOMAIN_WRITER_MAP = {
    insights: insightsDomainWriter,
};

/**
 * Get the bounded writer for a domain.
 * @param {string} domain — 'insights' | future: 'comments' | 'messages' | 'ugc'
 * @returns {object|null} writer module with execute(event, ctx)
 */
function getWriter(domain) {
    return DOMAIN_WRITER_MAP[domain] || null;
}

/**
 * Check if a writer is registered for a domain.
 * @param {string} domain
 * @returns {boolean}
 */
function hasWriter(domain) {
    return domain in DOMAIN_WRITER_MAP;
}

/**
 * Get all registered domain names.
 * @returns {string[]}
 */
function getRegisteredDomains() {
    return Object.keys(DOMAIN_WRITER_MAP);
}

module.exports = {
    getWriter,
    hasWriter,
    getRegisteredDomains,
    DOMAIN_WRITER_MAP,
};
