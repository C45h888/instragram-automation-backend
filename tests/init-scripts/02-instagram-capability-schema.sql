-- ============================================
-- PostgreSQL Test Init Script
-- Application Domain Tables (Instagram Capability Substrate)
-- ============================================
-- Purpose: Create the application-domain tables that the
-- postgres-telemetry-kernel workers read/write at runtime.
-- The governance-lineage schema (01-governance-schema.sql) is
-- a separate concern owned by the constitutional kernel; this
-- file covers the Instagram capability substrate tables.
--
-- Why this file exists:
--   The Phase 7 runtime-validation test
--   (tests/phase-7/kernels/capability-uat-refresh-runtime.test.js)
--   exposed that the test-postgres container's schema was missing
--   the tables the workers actually hit. Without these tables:
--     - read-credential-worker fails with "relation instagram_credentials does not exist"
--     - read-alerts-worker fails with "relation system_alerts does not exist"
--     - write-alert-worker fails with "relation system_alerts does not exist"
--     - write-lifecycle-event-worker fails with "relation token_lifecycle_events does not exist"
--   (Phase 7 Findings, B2)
--
-- Schema shape mirrors the column set the production workers
-- select/insert/update. Test rows are seeded by the test harness
-- (no production data is replicated here).
-- ============================================

-- ----------------------------------------
-- Instagram Credentials (Phase 7 Findings B2)
-- ----------------------------------------
-- Owned by the graph-capability substrate. read-credential-worker
-- selects {id, user_id, business_account_id, debug_token_checked_at,
-- issued_at, expires_at, data_access_expires_at} filtered by
-- token_type and is_active. update-credential-status-worker writes
-- {is_active, debug_token_checked_at, updated_at} by id.
CREATE TABLE IF NOT EXISTS instagram_credentials (
    id                          TEXT PRIMARY KEY,
    user_id                     TEXT NOT NULL,
    business_account_id         TEXT,
    token_type                  TEXT NOT NULL CHECK (token_type IN ('user', 'page')),
    is_active                   BOOLEAN NOT NULL DEFAULT true,
    issued_at                   TIMESTAMPTZ,
    expires_at                  TIMESTAMPTZ,
    data_access_expires_at      TIMESTAMPTZ,
    debug_token_checked_at      TIMESTAMPTZ,
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_instagram_creds_user_ba_type
    ON instagram_credentials(user_id, business_account_id, token_type);
CREATE INDEX idx_instagram_creds_active_expiring
    ON instagram_credentials(token_type, is_active, expires_at)
    WHERE expires_at IS NOT NULL;
CREATE INDEX idx_instagram_creds_active_data_access
    ON instagram_credentials(token_type, is_active, data_access_expires_at)
    WHERE data_access_expires_at IS NOT NULL;

-- ----------------------------------------
-- System Alerts (Phase 7 Findings B2)
-- ----------------------------------------
-- Owned by the alerts substrate. read-alerts-worker selects
-- (filtered by business_account_id, alert_type, resolved) and
-- write-alert-worker inserts (alert_type, business_account_id,
-- message, details, resolved).
CREATE TABLE IF NOT EXISTS system_alerts (
    id                          TEXT PRIMARY KEY,
    business_account_id         TEXT NOT NULL,
    alert_type                  TEXT NOT NULL,
    message                     TEXT NOT NULL,
    details                     JSONB NOT NULL DEFAULT '{}'::jsonb,
    resolved                    BOOLEAN NOT NULL DEFAULT false,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_system_alerts_ba_type_unresolved
    ON system_alerts(business_account_id, alert_type, resolved)
    WHERE resolved = false;
CREATE INDEX idx_system_alerts_created_at
    ON system_alerts(created_at DESC);

-- ----------------------------------------
-- Token Lifecycle Events (Phase 7 Findings B2)
-- ----------------------------------------
-- Owned by the alerts substrate. write-lifecycle-event-worker
-- inserts (credential_id, business_account_id, event_type,
-- token_age_days, details). Used for observability of
-- refresh / recovery / validation events.
CREATE TABLE IF NOT EXISTS token_lifecycle_events (
    id                          TEXT PRIMARY KEY,
    credential_id               TEXT,
    business_account_id         TEXT,
    event_type                  TEXT NOT NULL,
    token_age_days              INTEGER,
    details                     JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_lifecycle_credential
    ON token_lifecycle_events(credential_id, created_at DESC);
CREATE INDEX idx_lifecycle_ba
    ON token_lifecycle_events(business_account_id, created_at DESC);
CREATE INDEX idx_lifecycle_event_type
    ON token_lifecycle_events(event_type, created_at DESC);
