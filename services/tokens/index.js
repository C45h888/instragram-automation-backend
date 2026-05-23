// backend.api/services/tokens/index.js
// Zero-logic re-export shim.
// All existing require('../services/instagram-tokens') can be updated to
// require('../services/tokens') and continue working unchanged.
//
// logAudit is re-exported from config/supabase for backward compat.
// Routes should migrate to importing logAudit from config/supabase directly (Task 11).

const { logAudit } = require('../../config/supabase');

module.exports = {
  ...require('./detection'),
  ...require('./pat'),
  ...require('./uat'),
  ...require('./scope'),
  ...require('./base'),
  logAudit,
};
