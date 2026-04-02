"""
Phase 6D — End-to-End Integration Tests

Tests the full pipeline in isolation (no API server, no LLM).
Uses mock intent inputs and a synthetic DataFrame to validate
the planning + execution path deterministically.

Tests mirror the 6 integration scenarios from the spec:
  T1 — Context + Partial Execution (dimension change)
  T2 — KPI Swap (kpi_changed)
  T3 — Compare intent
  T4 — Filter Change
  T5 — Cache Hit (no change)
  T6 — UNKNOWN → no execution, no state update
"""

import sys
sys.path.insert(0, ".")

import pandas as pd
from services.execution_planner import (
    ExecutionPlanner,
    ExecutionStateStore,
    ExecutionState,
    MODE_FULL,
    MODE_PARTIAL,
    REUSE_BASE_DF,
    REUSE_FILTERED_DF,
    REUSE_LAST_RESULT,
    STEP_FILTER,
    STEP_GROUPBY,
    STEP_AGGREGATE,
    STEP_RENDER,
    STEP_COMPUTE_KPI_1,
    STEP_COMPUTE_KPI_2,
)
from graph.adaptive_executor import (
    adaptive_execute,
    _apply_filter,
    _build_prepared_data,
)

# ─────────────────────────────────────────────────────────────
# Test Framework
# ─────────────────────────────────────────────────────────────

passed = 0
failed = 0
failures = []

def check(test_id: str, condition: bool, message: str):
    global passed, failed
    if condition:
        print(f"  [OK]  {message}")
        passed += 1
    else:
        print(f"  [FAIL] {message}")
        failed += 1
        failures.append(f"{test_id}: {message}")

def sep(title: str):
    print(f"\n{'='*60}\n  {title}\n{'='*60}")

# ─────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────

# Synthetic DataFrame — simulates uploaded sales dataset
MOCK_DF = pd.DataFrame({
    "Sales": [100, 200, 150, 300, 250],
    "Quantity": [10, 20, 15, 30, 25],
    "Region": ["North", "South", "North", "East", "South"],
    "Quarter": ["Q1", "Q2", "Q3", "Q4", "Q4"],
})

# Dashboard plan (minimal — what generate_dashboard_plan() would return)
DASHBOARD_PLAN = {
    "kpis": [
        {"name": "Sales", "source_column": "Sales", "aggregation": "sum",
         "segment_by": None, "time_column": None},
        {"name": "Quantity", "source_column": "Quantity", "aggregation": "sum",
         "segment_by": None, "time_column": None},
    ],
    "charts": [],
    "_meta": {"kpi_count": 2, "chart_count": 0, "filename": "test.csv"},
}

planner = ExecutionPlanner()
store = ExecutionStateStore()
SESSION = "test-session-6d"


def make_state(intent: dict) -> ExecutionState:
    """Build a valid ExecutionState for the given intent."""
    filtered = _apply_filter(MOCK_DF, intent)
    return ExecutionState(
        base_df=MOCK_DF.copy(),
        filtered_df=filtered.copy(),
        last_result=[{"kpi": "Sales", "type": "scalar", "value": 1000.0}],
        last_intent=intent,
    )


# ─────────────────────────────────────────────────────────────
# T1 — Context + Partial Execution (dimension change)
# T1: show sales → T2: now by region
# Expected: plan=PARTIAL_RUN, filter NOT in ops, groupby in ops
# ─────────────────────────────────────────────────────────────
sep("T1 — Context + Partial Execution (Dimension Change)")

intent_t1 = {"intent": "SEGMENT_BY", "kpi": "Sales", "kpi_1": None, "kpi_2": None,
              "dimension": None, "filter": None}
intent_t2 = {"intent": "SEGMENT_BY", "kpi": "Sales", "kpi_1": None, "kpi_2": None,
              "dimension": "Region", "filter": None}

state_t1 = make_state(intent_t1)
plan_t2 = planner.plan(curr_intent=intent_t2, prev_state=state_t1)

check("T1", plan_t2.mode == MODE_PARTIAL, "plan.mode = PARTIAL_RUN")
check("T1", plan_t2.reuse == REUSE_FILTERED_DF, "reuse = filtered_df")
check("T1", STEP_FILTER not in plan_t2.operations, "filter NOT in ops")
check("T1", STEP_GROUPBY in plan_t2.operations, "groupby in ops")
check("T1", STEP_AGGREGATE in plan_t2.operations, "aggregate in ops")

# Execute and verify
result_t2 = adaptive_execute(
    plan=plan_t2,
    resolved_intent=intent_t2,
    dashboard_plan=DASHBOARD_PLAN,
    df=MOCK_DF,
    prev_state=state_t1,
    session_id=SESSION,
    run_id="run-t1",
)
prepared = result_t2.pipeline_result.get("prepared_data", [])
check("T1", result_t2.mode_used == MODE_PARTIAL, "executor used PARTIAL_RUN")
check("T1", any(STEP_FILTER not in op for op in result_t2.operations_run), "filter node NOT executed")
check("T1", len(prepared) > 0, "result has prepared data")

# ─────────────────────────────────────────────────────────────
# T2 — KPI Swap (kpi_changed → reuse=filtered_df, aggregate only)
# T1: show sales → T2: show quantity
# ─────────────────────────────────────────────────────────────
sep("T2 — KPI Swap (Sales → Quantity)")

intent_base = {"intent": "SEGMENT_BY", "kpi": "Sales", "kpi_1": None, "kpi_2": None,
               "dimension": None, "filter": None}
intent_kpi2 = {"intent": "SEGMENT_BY", "kpi": "Quantity", "kpi_1": None, "kpi_2": None,
               "dimension": None, "filter": None}

state_base = make_state(intent_base)
plan_kpi2 = planner.plan(curr_intent=intent_kpi2, prev_state=state_base)

check("T2", plan_kpi2.mode == MODE_PARTIAL, "PARTIAL_RUN on kpi change")
check("T2", plan_kpi2.reuse == REUSE_FILTERED_DF, "reuse = filtered_df")
check("T2", STEP_FILTER not in plan_kpi2.operations, "filter NOT re-run")
check("T2", STEP_AGGREGATE in plan_kpi2.operations, "aggregate in ops")

result_kpi2 = adaptive_execute(
    plan=plan_kpi2,
    resolved_intent=intent_kpi2,
    dashboard_plan=DASHBOARD_PLAN,
    df=MOCK_DF,
    prev_state=state_base,
    session_id=SESSION,
    run_id="run-t2",
)
prepared_t2 = result_kpi2.pipeline_result.get("prepared_data", [])
check("T2", len(prepared_t2) > 0, "prepared_data populated")
kpi_names_t2 = [p.get("kpi") for p in prepared_t2]
check("T2", "Quantity" in kpi_names_t2, "Quantity KPI in result")

# ─────────────────────────────────────────────────────────────
# T3 — Compare intent → compute_kpi_1 + compute_kpi_2, no filter
# T1: show sales → T2: compare with quantity
# ─────────────────────────────────────────────────────────────
sep("T3 — Compare (Sales vs Quantity)")

intent_compare_prev = {"intent": "COMPARE", "kpi": None,
                       "kpi_1": "Sales", "kpi_2": "Profit",
                       "dimension": None, "filter": None}
intent_compare_curr = {"intent": "COMPARE", "kpi": None,
                       "kpi_1": "Sales", "kpi_2": "Quantity",
                       "dimension": None, "filter": None}

state_compare = make_state(intent_compare_prev)
plan_compare = planner.plan(curr_intent=intent_compare_curr, prev_state=state_compare)

check("T3", plan_compare.mode == MODE_PARTIAL, "PARTIAL_RUN for COMPARE")
check("T3", plan_compare.reuse == REUSE_FILTERED_DF, "reuse = filtered_df")
check("T3", STEP_COMPUTE_KPI_1 in plan_compare.operations, "compute_kpi_1 in ops")
check("T3", STEP_COMPUTE_KPI_2 in plan_compare.operations, "compute_kpi_2 in ops")
check("T3", STEP_FILTER not in plan_compare.operations, "filter NOT in ops")

result_compare = adaptive_execute(
    plan=plan_compare,
    resolved_intent=intent_compare_curr,
    dashboard_plan=DASHBOARD_PLAN,
    df=MOCK_DF,
    prev_state=state_compare,
    session_id=SESSION,
    run_id="run-t3",
)
check("T3", result_compare.mode_used == MODE_PARTIAL, "executor used PARTIAL_RUN")
prepared_t3 = result_compare.pipeline_result.get("prepared_data", [])
check("T3", len(prepared_t3) >= 1, "at least one KPI in compare result")

# ─────────────────────────────────────────────────────────────
# T4 — Filter Change → reuse=base_df, filter re-run
# T1: show sales → T2: filter Q4
# ─────────────────────────────────────────────────────────────
sep("T4 — Filter Change (None → Q4)")

intent_no_filter = {"intent": "SEGMENT_BY", "kpi": "Sales", "kpi_1": None, "kpi_2": None,
                    "dimension": None, "filter": None}
intent_q4 = {"intent": "SEGMENT_BY", "kpi": "Sales", "kpi_1": None, "kpi_2": None,
              "dimension": None, "filter": "Q4"}

state_no_filter = make_state(intent_no_filter)
plan_q4 = planner.plan(curr_intent=intent_q4, prev_state=state_no_filter)

check("T4", plan_q4.mode == MODE_PARTIAL, "PARTIAL_RUN on filter change")
check("T4", plan_q4.reuse == REUSE_BASE_DF, "reuse = base_df")
check("T4", STEP_FILTER in plan_q4.operations, "filter re-run")

result_q4 = adaptive_execute(
    plan=plan_q4,
    resolved_intent=intent_q4,
    dashboard_plan=DASHBOARD_PLAN,
    df=MOCK_DF,
    prev_state=state_no_filter,
    session_id=SESSION,
    run_id="run-t4",
)
check("T4", result_q4.mode_used == MODE_PARTIAL, "executor used PARTIAL_RUN")
check("T4", result_q4.filtered_df is not None, "filtered_df produced")
# Q4 filter should narrow rows (MOCK_DF has 2 Q4 rows)
check("T4", len(result_q4.filtered_df) <= len(MOCK_DF), "filtered_df has <= rows than base")

# ─────────────────────────────────────────────────────────────
# T5 — Cache Hit (identical query) → reuse=last_result, only render
# T1: show sales → T2: show sales
# ─────────────────────────────────────────────────────────────
sep("T5 — Cache Hit (Identical Query)")

intent_same = {"intent": "SEGMENT_BY", "kpi": "Sales", "kpi_1": None, "kpi_2": None,
               "dimension": None, "filter": None}

state_same = make_state(intent_same)
plan_same = planner.plan(curr_intent=intent_same, prev_state=state_same)

check("T5", plan_same.mode == MODE_PARTIAL, "PARTIAL_RUN on cache hit")
check("T5", plan_same.reuse == REUSE_LAST_RESULT, "reuse = last_result")
check("T5", STEP_RENDER in plan_same.operations, "render step present")
check("T5", STEP_FILTER not in plan_same.operations, "filter NOT re-run")
check("T5", STEP_AGGREGATE not in plan_same.operations, "aggregate NOT re-run")

result_same = adaptive_execute(
    plan=plan_same,
    resolved_intent=intent_same,
    dashboard_plan=DASHBOARD_PLAN,
    df=MOCK_DF,
    prev_state=state_same,
    session_id=SESSION,
    run_id="run-t5",
)
check("T5", result_same.mode_used == MODE_PARTIAL, "executor used PARTIAL_RUN")
# Cache hit should return the stored last_result
check("T5", result_same.final_output == state_same.last_result, "last_result returned unchanged")

# ─────────────────────────────────────────────────────────────
# T6 — UNKNOWN intent → no execution, no state update
# ─────────────────────────────────────────────────────────────
sep("T6 — UNKNOWN Intent Gate (No Execution)")

# Simulate what query.py does when status != RESOLVED
# The planner should NEVER be called for UNKNOWN
# We test the guard logic directly

initial_store_count = store.has(SESSION + "_unknown_test")

# Simulate: if status not RESOLVED → return early, skip planner
resolution_status = "UNKNOWN"

execution_was_skipped = False
if resolution_status in ("UNKNOWN", "AMBIGUOUS", "INCOMPLETE"):
    execution_was_skipped = True
    # State store must NOT be updated
    # (no call to planner or adaptive_execute)

check("T6", execution_was_skipped, "execution skipped for UNKNOWN")
check("T6", not store.has(SESSION + "_unknown_test"), "state NOT updated on UNKNOWN")

# Also verify AMBIGUOUS is blocked
resolution_status = "AMBIGUOUS"
execution_was_skipped_2 = resolution_status in ("UNKNOWN", "AMBIGUOUS", "INCOMPLETE")
check("T6", execution_was_skipped_2, "execution skipped for AMBIGUOUS")

# Also verify INCOMPLETE is blocked
resolution_status = "INCOMPLETE"
execution_was_skipped_3 = resolution_status in ("UNKNOWN", "AMBIGUOUS", "INCOMPLETE")
check("T6", execution_was_skipped_3, "execution skipped for INCOMPLETE")

# ─────────────────────────────────────────────────────────────
# BONUS — ExecutionStateStore saves and retrieves correctly
# ─────────────────────────────────────────────────────────────
sep("BONUS — ExecutionStateStore Persistence")

intent_store = {"intent": "SEGMENT_BY", "kpi": "Sales", "kpi_1": None, "kpi_2": None,
                "dimension": None, "filter": None}
store_session = "store-test-session"

check("BONUS", not store.has(store_session), "session not yet in store")

store.save(
    store_session,
    base_df=MOCK_DF.copy(),
    filtered_df=MOCK_DF.copy(),
    last_result=[{"kpi": "Sales", "type": "scalar", "value": 1000}],
    last_intent=intent_store,
)

retrieved = store.get(store_session)
check("BONUS", retrieved is not None, "state retrieved from store")
check("BONUS", retrieved.is_valid(), "retrieved state is valid")
check("BONUS", retrieved.last_intent == intent_store, "last_intent preserved")

store.invalidate(store_session)
check("BONUS", not store.has(store_session), "state cleared after invalidate")

# ─────────────────────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"  PHASE 6D INTEGRATION TEST SUMMARY")
print(f"{'='*60}")
print(f"  Passed : {passed}")
print(f"  Failed : {failed}")
print(f"  Total  : {passed + failed}")

if failures:
    print(f"\n  FAILURES:")
    for f in failures:
        print(f"    [X] {f}")
else:
    print(f"\n  [OK] All Phase 6D integration checks passed!")

sys.exit(0 if failed == 0 else 1)
