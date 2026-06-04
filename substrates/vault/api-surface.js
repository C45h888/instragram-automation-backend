// substrates/vault/api-surface.js
// Vault API surface constants. Singleton: shared axios instance + Graph API endpoints.
// Migrated from services/tokens/base.js.

const axios = require('axios');

const GRAPH_API_VERSION = 'v23.0';
const GRAPH_API_BASE = `https://graph.facebook.com/${GRAPH_API_VERSION}`;

module.exports = {
  axios,
  GRAPH_API_VERSION,
  GRAPH_API_BASE,
};
