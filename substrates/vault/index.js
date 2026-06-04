// substrates/vault/index.js
// Vault root façade. The only public entry point consumers should use.
//
// Pattern:
//   const vault = require('./substrates/vault');
//   await vault.pat.exchange({ userAccessToken });
//   await vault.pat.store({ ... });
//   await vault.uat.refresh({ userId, businessAccountId, triggerBridge });
//   await vault.scope.detectDynamic({ token, supabase });
//
// Architecture:
//   vault.<domain> = substrate façade for that credential domain
//   Each façade owns: pre-flight, factory, signal dispatch (e.g. NEW_ACCOUNT_CONNECTED)
//   Each worker owns: one bounded I/O call (axios, supabase, vault RPC)
//   No I/O at this level — workers only.

const pat = require('./pat-substrate');
const uat = require('./uat-substrate');
const scope = require('./scope-substrate');

module.exports = {
  pat,
  uat,
  scope,
};
