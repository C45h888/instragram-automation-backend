const { createTransitionWriter } = require('./base-transition-writer');
const writer = createTransitionWriter('acquisition');
module.exports = { start: writer.start, stop: writer.stop, getHealth: writer.getHealth, awaitPendingWrite: writer.awaitPendingWrite };
