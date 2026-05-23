/**
 * Miscellaneous helper functions — currently just supabaseHelpers.deleteUserData.
 * Extracted from config/supabase.js supabaseHelpers object.
 */

const { getSupabaseAdmin } = require('./_client');
const { logAudit } = require('./_logging');

const supabaseHelpers = {
  /**
   * Deletes all user data across all tables for a given user.
   * Tables are deleted in sequence — if a table is missing, it logs and continues.
   * @param {string} userId — Supabase user UUID
   * @returns {{ success: boolean, results: Array, error?: string }}
   */
  async deleteUserData(userId) {
    try {
      const admin = getSupabaseAdmin();
      if (!admin) throw new Error('Database not connected');

      const results = [];
      const tables = [
        'workflow_executions',
        'automation_workflows',
        'instagram_comments',
        'instagram_media',
        'daily_analytics',
        'instagram_credentials',
        'instagram_business_accounts',
        'notifications',
        'api_usage',
        'user_profiles',
      ];

      for (const table of tables) {
        const { error } = await admin.from(table).delete().eq('user_id', userId);
        results.push({ table, success: !error, error: error?.message });
      }

      await logAudit('user_data_deletion', userId, {
        action: 'delete_all',
        resource_type: 'user_data',
        details: { tables_affected: tables, results },
        success: true,
      });

      return { success: true, results };
    } catch (error) {
      console.error('Error deleting user data:', error);

      await logAudit('user_data_deletion', userId, {
        action: 'delete_all',
        resource_type: 'user_data',
        details: { error: error.message },
        success: false,
      });

      return { success: false, error: error.message };
    }
  },
};

module.exports = { supabaseHelpers };
