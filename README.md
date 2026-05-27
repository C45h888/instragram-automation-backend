# Instagram Automation Backend

**Hierarchical Deterministic Governance Runtime** — constitutional orchestration for Instagram automation built on bounded federated domains, event-sourced lineage, and reconciliation-driven equilibrium.

## Architecture Overview

This platform is not a conventional SaaS backend. It is a constitutional governance runtime in which a single **Constitutional Kernel** acts as the sole legality authority, **Domain FSMs** (acquisition, publishing, scheduling) own bounded lifecycle transitions, **Orchestrators** route governance authority without interpreting runtime meaning, and **Substrates** perform infrastructure mechanics (retry, dedup, metrics, persistence, telemetry) while remaining semantically blind. State transitions flow exclusively through the kernel, lineage is the canonical source of truth, and all layers are vertically isolated by constitutional jurisdiction. The system prioritizes deterministic replay, reconciliation over reactive sync, and explicit contracts over implicit assumptions.

## Directory Structure

```
├── config/              Environment configuration & Supabase client initialization
├── contracts/           Runtime contracts & API boundary definitions
├── control-plane/       Constitutional kernel, domain FSMs, orchestrator, telemetry workers
├── helpers/             Utility helpers
├── lib/                 Shared libraries
├── middleware/          Express middleware (auth, rate-limiting, etc.)
├── routes/              HTTP API route handlers
├── services/            Business-logic services
├── substrates/          Semantically blind infrastructure (retry, dedup, metrics, persistence, etc.)
├── tests/               Test files
├── workers/             Bounded execution workers
├── server.js            Express application entry point
├── Dockerfile           Container build definition
└── nginx.conf           Reverse-proxy configuration
```

## Prerequisites

- **Node.js** >= 16.0.0
- **Supabase** project (Postgres + realtime)
- **Redis** instance (for queuing, caching, and lineage persistence)
- **Instagram API** credentials (Meta developer app)

## Setup

```bash
# 1. Install dependencies
npm install

# 2. Configure environment
cp .env.example .env   # or edit .env directly
# Required: SUPABASE_URL, SUPABASE_SERVICE_KEY, REDIS_URL, INSTAGRAM_* credentials

# 3. Verify connectivity
npm run test:connection
```

## Available Scripts

| Script | Description |
|--------|-------------|
| `npm run dev` | Start with nodemon (development, hot-reload) |
| `npm start` | Start in production mode |
| `npm run start:pm2` | Start via PM2 process manager |
| `npm test` | Run test suite |
| `npm run test:connection` | Verify Supabase connectivity |
| `npm run health` | Check `/health` endpoint |
| `npm run health:db` | Check `/health/database` endpoint |
| `npm run logs` | Tail all logs |
| `npm run logs:error` | Tail error logs only |

## Key Architectural Rules

1. **Substrates are semantically blind** — they perform mechanical work (retry, dedup, persistence) without interpreting constitutional meaning.
2. **Governance owns all policy** — the Constitutional Kernel is the sole interpreter of runtime legality; no subordinate layer governs independently.
3. **Orchestrators are constitutionally passive** — they route and coordinate; they do not decide degradation, retry, or recovery semantics.
4. **Lineage is canonical truth** — state is a materialized projection of append-only event lineage, enabling deterministic replay and crash recovery.
5. **FSM domains are bounded** — each domain FSM owns its lifecycle absolutely but remains subordinate to constitutional verification.
6. **Execution layers are non-agentic** — workers execute deterministic tasks only; they never self-govern or reinterpret policy.

## Further Reading

- [FEDERATED-GOVERNANCE-ARCHITECTURE.md](./FEDERATED-GOVERNANCE-ARCHITECTURE.md) — full architectural philosophy, phase progression, and audit findings
- [DEVELOPMENT-CONTRACT.md](./DEVELOPMENT-CONTRACT.md) — development conventions and constraints
- [.factory/AGENTS.md](./.factory/AGENTS.md) — constitutional runtime philosophy for AI coding agents
