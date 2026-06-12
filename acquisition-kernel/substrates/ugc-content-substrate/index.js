// acquisition-kernel/substrates/ugc-content-substrate/index.js
//
// DEPRECATED SUBSTRATE — polling/fetch path retired.
// UGC data is now sourced exclusively from the webhook acquisition path
// (substrates/webhook-acquisition-substrate). All fetchers, transports,
// parsers, and normalizers in this folder have been removed.
//
// This stub remains so that any stale `require('./substrates/ugc-content-substrate')`
// in the kernel fails with an explicit error rather than silently importing
// a no-op. New code MUST NOT use this substrate.

function _removed(name) {
  throw new Error(
    `[ugc-content-substrate] ${name} has been removed. ` +
    `UGC data is now sourced from the webhook acquisition path ` +
    `(acquisition-kernel/substrates/webhook-acquisition-substrate).`
  );
}

module.exports = {
  fetch:        () => _removed('fetch'),
  persistUgc:   () => _removed('persistUgc'),
  persistContent: () => _removed('persistContent'),
};
