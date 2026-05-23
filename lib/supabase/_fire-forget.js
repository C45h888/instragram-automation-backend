/**
 * Wraps a PostgrestBuilder in a real Promise so that error handling works as expected.
 * Without this, `await query.catch()` does not work because PostgrestBuilder resolves
 * to a plain object {error, data} rather than rejecting.
 *
 * Use for insert/update/upsert calls where failure is non-fatal and should be logged
 * but not thrown.
 *
 * @param {PostgrestBuilder} builder — any supabase chain (insert/update/upsert)
 * @returns {Promise<{error: object|null, data: any}>}
 */
function fireAndForgetInsert(builder) {
  return new Promise((resolve) => {
    builder
      .then(({ error, data }) => {
        if (error) console.warn('[fireAndForgetInsert] DB error:', error.message);
        resolve({ error, data });
      })
      .catch((err) => {
        // Network-level failure (DNS, connection refused, etc.)
        console.error('[fireAndForgetInsert] Promise rejected:', err);
        resolve({ error: err, data: null });
      });
  });
}

module.exports = { fireAndForgetInsert };
