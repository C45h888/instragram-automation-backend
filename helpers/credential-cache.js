// backend.api/helpers/credential-cache.js
// Singleton in-memory credential cache.
// Extracted from agent-helpers.js to break the circular dependency:
//   vault substrate workers → agent-helpers → vault substrate workers
// Both helpers/agent-helpers.js and substrates/vault/**/workers/* import from here.

const _credentialCache = new Map();
const CREDENTIAL_TTL_MS = 5 * 60 * 1000; // 5 minutes

function clearCredentialCache(businessAccountId) {
  _credentialCache.delete(businessAccountId);
}

function getFromCache(businessAccountId) {
  const entry = _credentialCache.get(businessAccountId);
  if (!entry) return null;
  if ((Date.now() - entry.ts) >= CREDENTIAL_TTL_MS) {
    _credentialCache.delete(businessAccountId);
    return null;
  }
  return entry.value;
}

function setInCache(businessAccountId, value) {
  _credentialCache.set(businessAccountId, { value, ts: Date.now() });
}

module.exports = { clearCredentialCache, getFromCache, setInCache };
