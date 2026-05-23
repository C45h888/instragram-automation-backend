// backend.api/helpers/data-fetchers/index.js
// Zero-breakage re-export shim.
//
// Allows existing callers of require('../helpers/data-fetchers') to continue
// working without changes during migration. Node.js resolves a folder require
// to this index.js automatically.
//
// Once all callers have been updated to import from their specific domain file
// (messaging-fetchers, ugc-fetchers, etc.), this file can be removed.

module.exports = {
  ...require('./messaging-fetchers'),
  ...require('./ugc-fetchers'),
  ...require('./media-fetchers'),
  ...require('./account-fetchers'),
};
