// substrates/db/readers/index.js
// DB Readers — re-export surface.

const media = require('./media');
module.exports = { ...media };
