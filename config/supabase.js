
/**
 * LEGACY RE-EXPORT — All logic moved to lib/supabase/
 *
 * This file re-exports everything from lib/supabase/index.js to maintain
 * 100% backward compatibility with all 29 importing files.
 *
 * New code should import directly from lib/supabase/ if using internal modules.
 * Existing imports from this file work unchanged.
 */

module.exports = require('../lib/supabase/index');
