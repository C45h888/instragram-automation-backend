-- ============================================
-- PostgreSQL Test Init Script
-- Governance Lineage Schema
-- ============================================
-- Purpose: Initialize ephemeral PostgreSQL with
-- governance lineage tables, telemetry projections,
-- and execution state storage for test runtime.
-- ============================================

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ----------------------------------------
-- Governance Lineage Tables
-- ----------------------------------------

-- Canonical lineage event log (event-sourced truth)
CREATE TABLE IF NOT EXISTS governance_lineage (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sequence_number BIGSERIAL NOT NULL UNIQUE,
    event_type VARCHAR(128) NOT NULL,
    event_payload JSONB NOT NULL,
    domain VARCHAR(64) NOT NULL,
    emitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    causal_vector JSONB,
    metadata JSONB DEFAULT '{}'
);

CREATE INDEX idx_lineage_sequence ON governance_lineage(sequence_number);
CREATE INDEX idx_lineage_event_type ON governance_lineage(event_type);
CREATE INDEX idx_lineage_domain ON governance_lineage(domain);
CREATE INDEX idx_lineage_emitted_at ON governance_lineage(emitted_at);

-- ----------------------------------------
-- Telemetry Projection Tables
-- ----------------------------------------

-- Systemic health projections
CREATE TABLE IF NOT EXISTS telemetry_health (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    projection_name VARCHAR(128) NOT NULL,
    health_status VARCHAR(32) NOT NULL,
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB DEFAULT '{}',
    UNIQUE(projection_name)
);

-- Engagement telemetry projections
CREATE TABLE IF NOT EXISTS telemetry_engagement (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_id VARCHAR(128) NOT NULL,
    metric_type VARCHAR(64) NOT NULL,
    metric_value NUMERIC NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lineage_sequence BIGINT REFERENCES governance_lineage(sequence_number),
    metadata JSONB DEFAULT '{}'
);

CREATE INDEX idx_engagement_account ON telemetry_engagement(account_id, recorded_at);
CREATE INDEX idx_engagement_metric ON telemetry_engagement(metric_type, recorded_at);

-- ----------------------------------------
-- Execution State Tables
-- ----------------------------------------

-- FSM execution state
CREATE TABLE IF NOT EXISTS fsm_execution_state (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    fsm_id VARCHAR(128) NOT NULL,
    state VARCHAR(64) NOT NULL,
    context JSONB NOT NULL DEFAULT '{}',
    transitioned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lineage_sequence BIGINT REFERENCES governance_lineage(sequence_number),
    UNIQUE(fsm_id)
);

-- Worker execution registry
CREATE TABLE IF NOT EXISTS worker_registry (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    worker_id VARCHAR(128) NOT NULL,
    worker_type VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'idle',
    last_ping TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    capabilities JSONB DEFAULT '[]',
    metadata JSONB DEFAULT '{}',
    UNIQUE(worker_id)
);

-- ----------------------------------------
-- Test Isolation Tables
-- ----------------------------------------

CREATE TABLE IF NOT EXISTS test_markers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    marker_key VARCHAR(256) NOT NULL,
    marker_value JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(marker_key)
);

-- ----------------------------------------
-- Bootstrap: Initial Governance State
-- ----------------------------------------

INSERT INTO governance_lineage (event_type, event_payload, domain)
VALUES (
    'CONSTITUTIONAL_KERNEL_BOOTSTRAP',
    jsonb_build_object('kernel', 'instagram-governance', 'version', '2.0.0', 'bootstrap_time', NOW()),
    'KERNEL'
) ON CONFLICT DO NOTHING;

INSERT INTO telemetry_health (projection_name, health_status)
VALUES ('kernel', 'healthy')
ON CONFLICT (projection_name) DO NOTHING;
