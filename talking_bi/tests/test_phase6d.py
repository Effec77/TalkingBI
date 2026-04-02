"""
Phase 6D — Execution Planner Tests

Validates the deterministic planning logic.
No LLM, no pipeline execution — tests the planner in isolation.

Tests:
  T1  — First turn (no state) → FULL_RUN
  T2  — Repeat query          → PARTIAL_RUN (reuse=last_result, steps=[render])
  T3  — KPI change only       → PARTIAL_RUN (reuse=filtered_df, steps=[groupby, aggregate])
  T4  — Dimension change only → PARTIAL_RUN (reuse=filtered_df, steps=[groupby, aggregate])
  T5  — Filter change         → PARTIAL_RUN (reuse=base_df, steps=[filter, groupby, aggregate])
  T6  — Intent type change    → FULL_RUN
  T7  — COMPARE intent        → PARTIAL_RUN (reuse=filtered_df, steps=[compute_kpi_1, compute_kpi_2])
  T8  — COMPARE + filter change → PARTIAL_RUN (reuse=base_df, steps=[filter, compute_kpi_1, compute_kpi_2])
  T9  — Partial state (invalid) → FULL_RUN (safety fallback)
  T10 — Step Skip: KPI change skips filter re-run
"""

import sys
sys.path.insert(0, ".")

import pandas as pd
from services.execution_planner import (
    ExecutionPlanner,
    ExecutionState,
    compute_intent_diff,
    REUSE_BASE_DF,
    REUSE_FILTERED_DF,
    REUSE_LAST_RESULT,
    STEP_FILTER,
    STEP_GROUPBY,
    STEP_AGGREGATE,
    STEP_COMPUTE_KPI_1,
    STEP_COMPUTE_KPI_2,
    STEP_RENDER,
    MODE_FULL,
    MODE_PARTIAL,
)

# ─────────────────────────────────────────────────────────────
# Test scaffolding
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


def make_state(intent: dict) -> ExecutionState:
    """Helper: build a valid ExecutionState with a dummy DataFrame."""
    dummy_df = pd.DataFrame({"Sales": [100, 200], "Region": ["North", "South"]})
    return ExecutionState(
        base_df=dummy_df.copy(),
        filtered_df=dummy_df.copy(),
        last_result=[{"kpi": "Sales", "type": "scalar", "value": 300}],
        last_intent=intent,
    )


planner = ExecutionPlanner()

# ─────────────────────────────────────────────────────────────
# T1 — First turn: no prior state → FULL_RUN
# ─────────────────────────────────────────────────────────────
sep("T1 — First Turn (No Prior State)")
intent = {"intent": "SEGMENT_BY", "kpi": "Sales", "dimension": "region", "filter": None}
plan = planner.plan(curr_intent=intent, prev_state=None)

check("T1", plan.mode == MODE_FULL, "FULL_RUN when no prior state")
check("T1", plan.reuse is None, "reuse=None on FULL_RUN")
check("T1", STEP_FILTER in plan.operations, "filter step present")
check("T1", STEP_AGGREGATE in plan.operations, "aggregate step present")

# ─────────────────────────────────────────────────────────────
# T2 — Identical repeat query → PARTIAL_RUN, reuse=last_result
# ─────────────────────────────────────────────────────────────
sep("T2 — Identical Repeat Query (Cache Hit)")
intent = {"intent": "SEGMENT_BY", "kpi": "Sales", "dimension": "region", "filter": None}
state = make_state(intent)
plan = planner.plan(curr_intent=intent, prev_state=state)

check("T2", plan.mode == MODE_PARTIAL, "PARTIAL_RUN on identical query")
check("T2", plan.reuse == REUSE_LAST_RESULT, "reuse=last_result")
check("T2", STEP_RENDER in plan.operations, "render step always present")
check("T2", STEP_FILTER not in plan.operations, "filter NOT re-run")
check("T2", STEP_AGGREGATE not in plan.operations, "aggregate NOT re-run")

# ─────────────────────────────────────────────────────────────
# T3 — KPI change only → PARTIAL_RUN, reuse=filtered_df
# ─────────────────────────────────────────────────────────────
sep("T3 — KPI Change Only (Sales → Quantity)")
prev = {"intent": "SEGMENT_BY", "kpi": "Sales", "dimension": "region", "filter": None}
curr = {"intent": "SEGMENT_BY", "kpi": "Quantity", "dimension": "region", "filter": None}
state = make_state(prev)
plan = planner.plan(curr_intent=curr, prev_state=state)

check("T3", plan.mode == MODE_PARTIAL, "PARTIAL_RUN on KPI change")
check("T3", plan.reuse == REUSE_FILTERED_DF, "reuse=filtered_df")
check("T3", STEP_FILTER not in plan.operations, "filter NOT re-run")
check("T3", STEP_AGGREGATE in plan.operations, "aggregate re-run")
check("T3", STEP_RENDER in plan.operations, "render always present")

# ─────────────────────────────────────────────────────────────
# T4 — Dimension change → PARTIAL_RUN, reuse=filtered_df
# ─────────────────────────────────────────────────────────────
sep("T4 — Dimension Change (region → product)")
prev = {"intent": "SEGMENT_BY", "kpi": "Sales", "dimension": "region", "filter": None}
curr = {"intent": "SEGMENT_BY", "kpi": "Sales", "dimension": "product", "filter": None}
state = make_state(prev)
plan = planner.plan(curr_intent=curr, prev_state=state)

check("T4", plan.mode == MODE_PARTIAL, "PARTIAL_RUN on dimension change")
check("T4", plan.reuse == REUSE_FILTERED_DF, "reuse=filtered_df")
check("T4", STEP_FILTER not in plan.operations, "filter NOT re-run")
check("T4", STEP_GROUPBY in plan.operations, "groupby re-run")
check("T4", STEP_AGGREGATE in plan.operations, "aggregate re-run")

# ─────────────────────────────────────────────────────────────
# T5 — Filter change → PARTIAL_RUN, reuse=base_df
# ─────────────────────────────────────────────────────────────
sep("T5 — Filter Change (None → Q4)")
prev = {"intent": "SEGMENT_BY", "kpi": "Sales", "dimension": "region", "filter": None}
curr = {"intent": "SEGMENT_BY", "kpi": "Sales", "dimension": "region", "filter": "Q4"}
state = make_state(prev)
plan = planner.plan(curr_intent=curr, prev_state=state)

check("T5", plan.mode == MODE_PARTIAL, "PARTIAL_RUN on filter change")
check("T5", plan.reuse == REUSE_BASE_DF, "reuse=base_df (filter invalidates filtered_df)")
check("T5", STEP_FILTER in plan.operations, "filter re-run")
check("T5", STEP_GROUPBY in plan.operations, "groupby re-run")
check("T5", STEP_AGGREGATE in plan.operations, "aggregate re-run")

# ─────────────────────────────────────────────────────────────
# T6 — Intent type change → FULL_RUN
# ─────────────────────────────────────────────────────────────
sep("T6 — Intent Type Change (SEGMENT_BY → EXPLAIN_TREND)")
prev = {"intent": "SEGMENT_BY", "kpi": "Sales", "dimension": "region", "filter": None}
curr = {"intent": "EXPLAIN_TREND", "kpi": "Sales", "dimension": "region", "filter": None}
state = make_state(prev)
plan = planner.plan(curr_intent=curr, prev_state=state)

check("T6", plan.mode == MODE_FULL, "FULL_RUN on intent type change")
check("T6", plan.reuse is None, "reuse=None on FULL_RUN")

# ─────────────────────────────────────────────────────────────
# T7 — COMPARE intent (filter same) → partial, reuse=filtered_df
# ─────────────────────────────────────────────────────────────
sep("T7 — COMPARE Intent (kpi_1=Sales, kpi_2=Quantity)")
prev = {"intent": "SEGMENT_BY", "kpi": "Sales", "kpi_1": None, "kpi_2": None,
        "dimension": None, "filter": None}
curr = {"intent": "COMPARE", "kpi": None, "kpi_1": "Sales", "kpi_2": "Quantity",
        "dimension": None, "filter": None}
state = make_state(prev)
plan = planner.plan(curr_intent=curr, prev_state=state)

# COMPARE forces FULL_RUN on intent_changed (SEGMENT_BY → COMPARE)
# so this tests the pure COMPARE path when intent is already COMPARE
prev2 = {"intent": "COMPARE", "kpi": None, "kpi_1": "Sales", "kpi_2": "Profit",
         "dimension": None, "filter": None}
curr2 = {"intent": "COMPARE", "kpi": None, "kpi_1": "Sales", "kpi_2": "Quantity",
         "dimension": None, "filter": None}
state2 = make_state(prev2)
plan2 = planner.plan(curr_intent=curr2, prev_state=state2)

check("T7", plan2.mode == MODE_PARTIAL, "PARTIAL_RUN on COMPARE (no filter change)")
check("T7", plan2.reuse == REUSE_FILTERED_DF, "reuse=filtered_df for COMPARE")
check("T7", STEP_COMPUTE_KPI_1 in plan2.operations, "compute_kpi_1 step present")
check("T7", STEP_COMPUTE_KPI_2 in plan2.operations, "compute_kpi_2 step present")
check("T7", STEP_FILTER not in plan2.operations, "filter NOT re-run")

# ─────────────────────────────────────────────────────────────
# T8 — COMPARE + filter change → base_df + filter + compute
# ─────────────────────────────────────────────────────────────
sep("T8 — COMPARE With Filter Change")
prev = {"intent": "COMPARE", "kpi": None, "kpi_1": "Sales", "kpi_2": "Profit",
        "dimension": None, "filter": None}
curr = {"intent": "COMPARE", "kpi": None, "kpi_1": "Sales", "kpi_2": "Profit",
        "dimension": None, "filter": "Q4"}
state = make_state(prev)
plan = planner.plan(curr_intent=curr, prev_state=state)

check("T8", plan.mode == MODE_PARTIAL, "PARTIAL_RUN for COMPARE with filter change")
check("T8", plan.reuse == REUSE_BASE_DF, "reuse=base_df when filter changed in COMPARE")
check("T8", STEP_FILTER in plan.operations, "filter step re-run")
check("T8", STEP_COMPUTE_KPI_1 in plan.operations, "compute_kpi_1 present")
check("T8", STEP_COMPUTE_KPI_2 in plan.operations, "compute_kpi_2 present")

# ─────────────────────────────────────────────────────────────
# T9 — Invalid/partial ExecutionState → FULL_RUN (safety fallback)
# ─────────────────────────────────────────────────────────────
sep("T9 — Safety Fallback: Partial ExecutionState")
intent = {"intent": "SEGMENT_BY", "kpi": "Sales", "dimension": "region", "filter": None}

# Partially initialized state — filtered_df missing
partial_state = ExecutionState(
    base_df=pd.DataFrame({"Sales": [100]}),
    filtered_df=None,          # Missing!
    last_result=None,          # Missing!
    last_intent=intent,
)

plan = planner.plan(curr_intent=intent, prev_state=partial_state)

check("T9", plan.mode == MODE_FULL, "FULL_RUN when ExecutionState is incomplete")
check("T9", plan.reuse is None, "reuse=None on forced FULL_RUN")

# ─────────────────────────────────────────────────────────────
# T10 — Step skipping validation: KPI change doesn't re-run filter
# ─────────────────────────────────────────────────────────────
sep("T10 — Query Node Skip: KPI Change Should NOT Re-Run Filter")
prev = {"intent": "SEGMENT_BY", "kpi": "Sales", "dimension": None, "filter": "Q3"}
curr = {"intent": "SEGMENT_BY", "kpi": "Quantity", "dimension": None, "filter": "Q3"}
state = make_state(prev)
plan = planner.plan(curr_intent=curr, prev_state=state)

check("T10", plan.mode == MODE_PARTIAL, "PARTIAL_RUN on kpi-only change")
check("T10", plan.reuse == REUSE_FILTERED_DF, "reuse=filtered_df (filter unchanged)")
check("T10", STEP_FILTER not in plan.operations, "[CRITICAL] filter node NOT executed")
check("T10", STEP_AGGREGATE in plan.operations, "aggregate step present")

# ─────────────────────────────────────────────────────────────
# BONUS — IntentDiff unit test
# ─────────────────────────────────────────────────────────────
sep("BONUS — IntentDiff Direct Unit Test")
a = {"intent": "SEGMENT_BY", "kpi": "Sales", "kpi_1": None, "kpi_2": None,
     "dimension": "region", "filter": "Q3"}
b = {"intent": "SEGMENT_BY", "kpi": "Sales", "kpi_1": None, "kpi_2": None,
     "dimension": "product", "filter": "Q3"}

diff = compute_intent_diff(a, b)
check("BONUS", diff["intent_changed"] is False, "intent_changed=False")
check("BONUS", diff["kpi_changed"] is False, "kpi_changed=False")
check("BONUS", diff["dimension_changed"] is True, "dimension_changed=True")
check("BONUS", diff["filter_changed"] is False, "filter_changed=False")

# ─────────────────────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"  PHASE 6D TEST SUMMARY")
print(f"{'='*60}")
print(f"  Passed : {passed}")
print(f"  Failed : {failed}")
print(f"  Total  : {passed + failed}")

if failures:
    print(f"\n  FAILURES:")
    for f in failures:
        print(f"    [X] {f}")
else:
    print(f"\n  [OK] All Phase 6D checks passed!")
