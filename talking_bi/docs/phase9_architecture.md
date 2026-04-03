# Phase 9 — Productization Architecture Design
## TalkingBI | Finalized Decisions Document

---

## What Already Exists (Baseline Audit)

Before choosing anything, audit what you actually have:

| Component | Status | Notes |
|---|---|---|
| FastAPI app | **EXISTS** — `main.py` | Routers for upload, query, run, intelligence |
| Session manager | **EXISTS** — `services/session_manager.py` | In-memory dict + TTL + APScheduler cleanup |
| Upload endpoint | **EXISTS** — `api/upload.py` | CSV → DataFrame → session_id |
| Query endpoint | **EXISTS** — `api/query.py` | `POST /query/{session_id}` |
| Execution engine | **EXISTS** — `graph/adaptive_executor.py` | FULL_RUN (LangGraph) + PARTIAL_RUN (Pandas) |
| Evaluator | **EXISTS** — `services/evaluator.py` | Metrics, failure classification, regression |
| Orchestration | **MISSING** | Logic is embedded directly in `api/query.py` |
| Metrics endpoint | **MISSING** | Evaluator not exposed via HTTP |
| UI | **MISSING** | No frontend at all |
| ExecutionEngine abstraction | **MISSING** | Pandas path and LangGraph hardcoded |
| Output schema | **INCONSISTENT** | query.py and adaptive_executor return different shapes |

> The system is ~60% of a product already. Phase 9 fills specific gaps — it does not rebuild.

---

## Architecture Decision Record

### Decision 1 — API Layer

**Chosen: FastAPI (already in use)**

Do not change frameworks. FastAPI is already the right choice.

What to change:
- Add `GET /metrics` endpoint (expose evaluator)
- Add `DELETE /session/{id}` (currently no explicit reset)
- Add `GET /session/{id}/status` (query health of session state)
- Standardize all response shapes (see Decision 6)

What NOT to do:
- Do not add GraphQL
- Do not add gRPC
- Do not split into microservices

---

### Decision 2 — Orchestrator Layer

**Chosen: Extract `QueryOrchestrator` from `api/query.py`**

Currently `api/query.py` does too much:
- Validates session
- Calls intent parser
- Calls schema mapper
- Calls semantic interpreter
- Calls resolver
- Calls planner
- Calls executor
- Records to evaluator
- Formats response

This must be split:

```
api/query.py          → HTTP boundary only (validate input, call orchestrator, return HTTP response)
services/orchestrator.py  → Pipeline coordination (session → engine → evaluator → result)
```

The API layer should never know about intents, KPIs, or execution plans.
The orchestrator should never know about HTTP.

**Target interface:**

```python
class QueryOrchestrator:
    def handle(
        self,
        query: str,
        session_id: str,
    ) -> OrchestratorResult:
        ...
```

**OrchestratorResult is the product's canonical output format** (see Decision 6).

---

### Decision 3 — Session Management

**Chosen: Keep in-memory, add execution state into existing session**

The current `session_manager.py` stores `{df, metadata, created_at, expires_at}`.

The 6D `ExecutionStateStore` is a *separate* in-memory dict.

These must be unified. Every session should hold:

```python
{
    "df":              pd.DataFrame,        # raw uploaded data
    "metadata":        UploadedDataset,
    "execution_state": ExecutionState,      # 6D cache (filtered_df, last_result, last_intent)
    "conversation":    List[Dict],          # turn history for 6C resolver
    "dashboard_plan":  Dict,                # generated once, reused every turn
    "created_at":      datetime,
    "expires_at":      datetime,
}
```

Do NOT switch to Redis yet. Redis adds a serialization problem: DataFrames and ExecutionState contain pandas objects that don't serialize cleanly to JSON. Fix the architecture first. Redis should only be considered when you have a second server process that needs shared state.

---

### Decision 4 — Execution Engine Abstraction

**Chosen: Introduce `ExecutionBackend` protocol, keep PandasBackend as only implementation**

The current `adaptive_executor.py` already has a clean two-path split:
- `FULL_RUN` → LangGraph
- `PARTIAL_RUN` → Pure Pandas

What's missing is the interface abstraction. Add it without changing any logic:

```python
# services/execution_backend.py

from typing import Protocol

class ExecutionBackend(Protocol):
    def execute(
        self,
        plan: ExecutionPlan,
        intent: Dict,
        dashboard_plan: Dict,
        df: pd.DataFrame,
        prev_state: Optional[ExecutionState],
        session_id: str,
    ) -> AdaptiveResult:
        ...
```

Then rename `adaptive_executor.adaptive_execute()` to `PandasBackend.execute()`.

The orchestrator calls `backend.execute(...)` — it never imports from `adaptive_executor` directly.

When PostgresBackend is needed (Phase 9B), add a second class that implements the same protocol and generates SQL. The orchestrator does not change.

> **Rule**: ExecutionPlan must remain backend-agnostic. It already is — do not add pandas-specific fields to it.

---

### Decision 5 — Data Layer

**Chosen: CSV/Pandas for Phase 9A. Design for Postgres in 9B.**

You already have SQLAlchemy and psycopg2 in `requirements.txt`. This tells me someone planned Postgres. Good.

For Phase 9A: keep Pandas. The evaluation showed 4ms average latency on datasets up to 20k rows. That's fast enough.

For Phase 9B, the migration path is:
1. `PandasBackend` stays for datasets < 50k rows or CSV uploads
2. `PostgresBackend` takes over when a dataset is registered to a DB table
3. Planner output (ExecutionPlan) translates to SQL via a `SQLGenerator` class

The threshold routing would live in the orchestrator:

```python
backend = PandasBackend() if df is not None else PostgresBackend(conn)
result = backend.execute(plan, ...)
```

Do not implement this yet. Design for it.

---

### Decision 6 — Output Schema (CRITICAL — Do This First)

**Chosen: Standardize `OrchestratorResult` as the single product output**

Current problem: `api/query.py` and `adaptive_executor.py` return different keys. The UI and evaluator both have to guess what's in the response.

Define one canonical output — used everywhere:

```python
@dataclass
class OrchestratorResult:
    status: str           # RESOLVED | INCOMPLETE | UNKNOWN | ERROR
    query: str
    session_id: str

    # What the system understood
    intent: Dict          # resolved intent (kpi, dimension, filter, intent_type)
    semantic_meta: Dict   # Phase 7 output (applied, confidence, mapped_to)

    # What the system computed
    data: List[Dict]      # prepared_data (timeseries or scalar per KPI)
    charts: List[Dict]    # chart specs (type, kpi, values)
    insights: List[Dict]  # insight objects

    # How the system computed it
    plan: Dict            # execution plan (mode, reuse, ops, reason)
    latency_ms: float

    # What went wrong (if anything)
    warnings: List[str]
    errors: List[str]
```

This maps directly to the JSON response the UI will consume:

```json
{
  "status": "RESOLVED",
  "query": "show revenue by region",
  "intent": { "intent": "SEGMENT_BY", "kpi": "Revenue", "dimension": "region" },
  "semantic_meta": { "applied": false },
  "data": [{ "kpi": "Revenue", "type": "timeseries", "data": [...] }],
  "charts": [{ "type": "bar", "kpi": "Revenue", "x": "region", "y": "value" }],
  "insights": [{ "kpi": "Revenue", "type": "range", "details": {...} }],
  "plan": { "mode": "PARTIAL_RUN", "reuse": "filtered_df", "reason": "kpi_changed" },
  "latency_ms": 4.3,
  "warnings": [],
  "errors": []
}
```

---

### Decision 7 — Observability / Metrics Endpoint

**Chosen: `GET /metrics` backed by module-level Evaluator singleton**

The `Evaluator` already supports `compute_metrics()`. Wire it to an HTTP endpoint:

```python
@router.get("/metrics")
async def get_metrics():
    return evaluator_singleton.compute_metrics()
```

No dashboard yet. The JSON response is enough for Phase 9A.

What the endpoint returns:
```json
{
  "total_queries": 108,
  "success_rate": 0.935,
  "avg_latency_ms": 4.12,
  "p95_latency_ms": 20.95,
  "semantic_usage_rate": 0.083,
  "partial_execution_rate": 0.475,
  "failure_breakdown": { "NONE": 101, "AMBIGUOUS_QUERY": 6, "CONTEXT_MISSING": 1 }
}
```

---

### Decision 8 — UI Layer

**Chosen: Minimal chat UI (Phase 9A). Analyst UI (Phase 9B).**

Phase 9A requirements:
- Single-page HTML/CSS/JS (no framework needed at this scale)
- Three components: input box, response panel, chart renderer
- Chart library: **Plotly.js** (already aligns with chart_specs format, CDN, zero build step)
- No React, no build pipeline — this adds complexity you don't need yet

Phase 9A layout:
```
┌────────────────────────────────────────────┐
│  TalkingBI                         [reset] │
├──────────────────────────────────────────── │
│                                            │
│  [Chart or table renders here]             │
│                                            │
│  Insight: Revenue peaked in Q3 at $1.2M   │
│                                            │
│──────────────────────────────────────────── │
│  > show revenue by region          [Send]  │
└────────────────────────────────────────────┘
```

What the UI calls:
```
POST /upload        → get session_id
POST /query/{id}    → get OrchestratorResult
DELETE /session/{id} → reset
GET /metrics        → system health
```

---

### Decision 9 — Caching

**Chosen: Use 6D ExecutionState (already built). Do not add a query cache yet.**

The 6D partial execution system IS the cache. It gives:
- 47.5% of queries served without re-running computation
- avg 4ms latency already

A hash-based query cache (`(hash(query), dataset) → result`) would add:
- Cache invalidation complexity
- Memory risk on large datasets
- Marginal gain on non-repeated queries

Add it in Phase 9B only if profiling shows repeated identical queries are a bottleneck.

---

### Decision 10 — Security / Validation

**Chosen: Add three lightweight guards at the API boundary**

```python
# 1. Query length limit
MAX_QUERY_LENGTH = 500
if len(query) > MAX_QUERY_LENGTH:
    raise HTTPException(400, "Query too long")

# 2. Session access (already exists — extend)
if not session_store.has(session_id):
    raise HTTPException(404, "Session not found or expired")

# 3. Dataset row limit (Phase 9A safety valve)
MAX_ROWS = 100_000
if session.df.shape[0] > MAX_ROWS:
    raise HTTPException(413, f"Dataset too large: {session.df.shape[0]} rows (max {MAX_ROWS})")
```

Do not add auth/JWT yet — single-user product. Add when multi-tenancy is needed.

---

## Phase 9A Implementation Scope

These are the **6 concrete deliverables** for Phase 9A, in order:

| # | Deliverable | File(s) | Dependency |
|---|---|---|---|
| 1 | `OrchestratorResult` dataclass + serializer | `models/contracts.py` | None — do first |
| 2 | `QueryOrchestrator` class | `services/orchestrator.py` | Needs #1 |
| 3 | `ExecutionBackend` protocol + `PandasBackend` | `services/execution_backend.py` | Needs #2 |
| 4 | Unified session schema | `services/session_manager.py` | Needs #2 |
| 5 | `GET /metrics` + slim `api/query.py` | `api/query.py`, `api/metrics.py` | Needs #2 |
| 6 | Chat UI | `static/index.html` | Needs #5 |

> Implement strictly in this order. Each step is independently testable.

---

## Phase 9B Scope (Do Not Start Yet)

- `PostgresBackend` — SQL generation from ExecutionPlan  
- Redis session store — when multi-process deployment needed  
- Query result cache — hash-based, keyed by `(query_hash, session_id)`  
- Analyst UI — query history, explain-plan view, debug mode  
- Auth layer — JWT or API key gating  

---

## Architecture Diagram

```
┌─────────────────────────┐
│      Browser / Client   │
│   static/index.html     │
└────────────┬────────────┘
             │ HTTP
┌────────────▼────────────┐
│      FastAPI (main.py)  │
│  POST /upload           │
│  POST /query/{id}       │
│  DELETE /session/{id}   │
│  GET  /metrics          │
└────────────┬────────────┘
             │
┌────────────▼────────────────────────────────┐
│         QueryOrchestrator                   │
│  services/orchestrator.py                   │
│                                             │
│  1. get_session(session_id)                 │
│  2. 6E normalize → 6G override → 6B parse  │
│  3. 7 semantic interpret                    │
│  4. 6F schema map → 6C resolve              │
│  5. 6D plan → backend.execute()             │
│  6. evaluator.record()                      │
│  7. return OrchestratorResult               │
└──┬──────────────┬───────────────────────────┘
   │              │
┌──▼──────┐  ┌───▼───────────────────────────┐
│Session  │  │    ExecutionBackend (Protocol) │
│Manager  │  │                               │
│{df,     │  │  PandasBackend (Phase 9A)     │
│ exec_   │  │    adaptive_executor.py        │
│ state,  │  │    ├── FULL_RUN → LangGraph   │
│ convo,  │  │    └── PARTIAL_RUN → Pandas   │
│ plan}   │  │                               │
└─────────┘  │  PostgresBackend (Phase 9B)  │
             │    SQLGenerator(plan) → SQL   │
             └───────────────────────────────┘
                          │
             ┌────────────▼────────────┐
             │   Evaluator Singleton   │
             │   services/evaluator.py │
             │   (metrics, failures)   │
             └─────────────────────────┘
```

---

## Critical Rules (Enforced By Design)

| Rule | Enforcement |
|---|---|
| Engine never touches HTTP | Orchestrator is the only bridge |
| Execution must be backend-agnostic | ExecutionPlan has no Pandas/SQL-specific fields |
| State is external to engine | Session holds all state; engine is stateless |
| All outputs are structured | OrchestratorResult is the only return type |
| No LLM in deterministic layers | 6E, 6F, 6G, 7, 6D — zero LLM calls |

---

## What to Say Next

When ready to implement:

```
"implement phase 9 deliverable 1: OrchestratorResult"
```

Then continue sequentially through the 6 deliverables. Do not skip steps.
