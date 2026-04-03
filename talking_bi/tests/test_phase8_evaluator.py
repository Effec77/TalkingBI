"""
Phase 8: Evaluation & Guardrails — Test Suite

Tests:
  T1 — record() stores correct fields
  T2 — classify_failure() for all failure types
  T3 — compute_metrics() correct values
  T4 — regression compare_runs() delta + new_failures + resolved_failures
  T5 — save() writes valid JSON
  T6 — timed_record context manager
  T7 — semantic_rejection classification
  T8 — execution_error classification
  T9 — partial_execution_rate only counts RESOLVED records
  T10 — empty evaluator metrics safe
"""

import sys
import os
import json
import tempfile
sys.path.insert(0, ".")

from services.evaluator import (
    Evaluator,
    classify_failure,
    FailureType,
    timed_record,
    get_evaluator,
    reset_evaluator,
)

# ─────────────────────────────────────────────────────────────
# Test framework
# ─────────────────────────────────────────────────────────────

passed = 0
failed = 0
failures = []

def check(tid: str, condition: bool, message: str):
    global passed, failed
    if condition:
        print(f"  [OK]  {message}")
        passed += 1
    else:
        print(f"  [FAIL] {message}")
        failed += 1
        failures.append(f"{tid}: {message}")

def sep(title: str):
    print(f"\n{'='*60}\n  {title}\n{'='*60}")


# ─────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────

RESOLVED_RESULT = {
    "status": "RESOLVED",
    "intent_resolved": {"intent": "SEGMENT_BY", "kpi": "Revenue",
                        "dimension": None, "filter": None},
    "plan_6d": {"mode": "FULL_RUN"},
    "errors": [],
    "semantic_meta": {"applied": False, "reason": "kpi already set"},
}

RESOLVED_PARTIAL = {
    "status": "RESOLVED",
    "intent_resolved": {"intent": "SEGMENT_BY", "kpi": "Revenue",
                        "dimension": "region", "filter": None},
    "plan_6d": {"mode": "PARTIAL_RUN"},
    "errors": [],
    "semantic_meta": {"applied": True, "vague_term": "revenue",
                      "mapped_to": "Revenue", "confidence": 0.85},
}

UNKNOWN_RESULT = {
    "status": "UNKNOWN",
    "intent_resolved": None,
    "errors": [],
    "semantic_meta": {},
}

INCOMPLETE_RESULT = {
    "status": "INCOMPLETE",
    "intent_resolved": {"intent": "SEGMENT_BY", "kpi": None},
    "errors": [],
    "semantic_meta": {},
}

INVALID_RESULT = {
    "status": "INVALID",
    "intent": {"intent": "SEGMENT_BY", "kpi": "fake_column"},
    "errors": [],
    "semantic_meta": {},
}

EXEC_ERROR_RESULT = {
    "status": "RESOLVED",
    "intent_resolved": {"intent": "SEGMENT_BY", "kpi": "Revenue"},
    "plan_6d": {"mode": "FULL_RUN"},
    "errors": ["KeyError: 'Revenue' not found"],
    "semantic_meta": {},
}

SEMANTIC_REJECTION_RESULT = {
    "status": "RESOLVED",
    "intent_resolved": {"intent": "SEGMENT_BY", "kpi": "Revenue"},
    "plan_6d": {"mode": "FULL_RUN"},
    "errors": [],
    "semantic_meta": {
        "applied": False,
        "reason": "low_confidence",
        "confidence": 0.42,
    },
}


# ─────────────────────────────────────────────────────────────
# T1 — record() stores correct fields
# ─────────────────────────────────────────────────────────────
sep("T1 — record() correct fields")

ev = Evaluator()
rec = ev.record(
    query="show revenue",
    dataset="ecommerce.csv",
    result=RESOLVED_RESULT,
    latency_ms=123.4,
)

check("T1", rec.query == "show revenue", "query stored correctly")
check("T1", rec.dataset == "ecommerce.csv", "dataset stored correctly")
check("T1", rec.status == "RESOLVED", "status stored correctly")
check("T1", rec.latency_ms == 123.4, "latency_ms stored correctly")
check("T1", rec.execution_mode == "FULL_RUN", "execution_mode from plan_6d")
check("T1", rec.semantic_applied is False, "semantic_applied = False")
check("T1", rec.failure_type is None, "no failure for RESOLVED with no errors")
check("T1", len(ev.records) == 1, "record appended to evaluator")


# ─────────────────────────────────────────────────────────────
# T2 — classify_failure() for all types
# ─────────────────────────────────────────────────────────────
sep("T2 — classify_failure() all types")

check("T2", classify_failure(RESOLVED_RESULT) is None,
      "RESOLVED → None (no failure)")
check("T2", classify_failure(UNKNOWN_RESULT) == FailureType.AMBIGUOUS_QUERY,
      "UNKNOWN → AMBIGUOUS_QUERY")
check("T2", classify_failure(INCOMPLETE_RESULT) == FailureType.CONTEXT_MISSING,
      "INCOMPLETE → CONTEXT_MISSING")
check("T2", classify_failure(INVALID_RESULT) == FailureType.INVALID_INTENT,
      "INVALID → INVALID_INTENT")
check("T2", classify_failure(EXEC_ERROR_RESULT) == FailureType.EXECUTION_ERROR,
      "RESOLVED + errors → EXECUTION_ERROR")
check("T2", classify_failure(SEMANTIC_REJECTION_RESULT) == FailureType.SEMANTIC_REJECTION,
      "low_confidence semantic → SEMANTIC_REJECTION")


# ─────────────────────────────────────────────────────────────
# T3 — compute_metrics() correct values
# ─────────────────────────────────────────────────────────────
sep("T3 — compute_metrics() accuracy")

ev2 = Evaluator()
# 3 RESOLVED (2 FULL, 1 PARTIAL), 1 UNKNOWN, 1 INCOMPLETE
ev2.record("q1", "ds.csv", RESOLVED_RESULT,  50.0)
ev2.record("q2", "ds.csv", RESOLVED_PARTIAL, 30.0)
ev2.record("q3", "ds.csv", RESOLVED_RESULT,  70.0)
ev2.record("q4", "ds.csv", UNKNOWN_RESULT,   20.0)
ev2.record("q5", "ds.csv", INCOMPLETE_RESULT, 25.0)

m = ev2.compute_metrics()

check("T3", m["total"] == 5, "total = 5")
check("T3", m["success_rate"] == round(3/5, 4), f"success_rate = {round(3/5,4)}")
check("T3", "AMBIGUOUS_QUERY" in m["failure_breakdown"],
      "failure_breakdown contains AMBIGUOUS_QUERY")
check("T3", "CONTEXT_MISSING" in m["failure_breakdown"],
      "failure_breakdown contains CONTEXT_MISSING")
check("T3", m["failure_breakdown"].get("AMBIGUOUS_QUERY", 0) == 1,
      "AMBIGUOUS_QUERY count = 1")
check("T3", m["avg_latency_ms"] == round((50+30+70+20+25)/5, 2),
      "avg_latency_ms correct")
check("T3", m["semantic_usage_rate"] == round(1/5, 4),
      "semantic_usage_rate = 0.2 (1 semantic hit)")

# partial_execution_rate = 1 partial out of 3 resolved
check("T3", m["partial_execution_rate"] == round(1/3, 4),
      f"partial_execution_rate = {round(1/3, 4)} (1/3 resolved are PARTIAL)")

check("T3", "RESOLVED" in m["status_breakdown"],
      "status_breakdown contains RESOLVED")
check("T3", m["status_breakdown"]["RESOLVED"] == 3,
      "status_breakdown RESOLVED = 3")


# ─────────────────────────────────────────────────────────────
# T4 — compare_runs() regression detection
# ─────────────────────────────────────────────────────────────
sep("T4 — compare_runs() regression detection")

# Build a "previous" evaluator and save it
ev_prev = Evaluator()
ev_prev.record("show revenue",  "ds.csv", RESOLVED_RESULT,   50.0)
ev_prev.record("show usage",    "ds.csv", UNKNOWN_RESULT,     20.0)  # used to fail
ev_prev.record("show quantity", "ds.csv", RESOLVED_RESULT,   40.0)   # used to pass

with tempfile.NamedTemporaryFile(
    mode="w", suffix=".json", delete=False, encoding="utf-8"
) as tmp:
    tmp_path = tmp.name

ev_prev.save(tmp_path)

# Build current evaluator:
# - "show revenue"  → now FAILS  (regression)
# - "show usage"    → now PASSES  (improvement)
# - "show quantity" → still passes
ev_curr = Evaluator()
ev_curr.record("show revenue",  "ds.csv", UNKNOWN_RESULT,    55.0)   # regressed
ev_curr.record("show usage",    "ds.csv", RESOLVED_RESULT,   22.0)   # fixed
ev_curr.record("show quantity", "ds.csv", RESOLVED_RESULT,   42.0)   # stable

cmp = ev_curr.compare_runs(tmp_path)

check("T4", "delta_success_rate" in cmp, "delta_success_rate in comparison")
check("T4", len(cmp["new_failures"]) == 1,
      "1 new failure detected (show revenue regressed)")
check("T4", cmp["new_failures"][0]["query"] == "show revenue",
      "correct query identified as regressed")
check("T4", len(cmp["resolved_failures"]) == 1,
      "1 resolved failure detected (show usage fixed)")
check("T4", cmp["resolved_failures"][0]["query"] == "show usage",
      "correct query identified as fixed")
check("T4", "current_metrics" in cmp and "previous_metrics" in cmp,
      "both metrics present in comparison")
# current: 2 of 3 resolved = 0.6667; prev: 2 of 3 = 0.6667 → delta = 0
check("T4", isinstance(cmp["delta_success_rate"], float),
      "delta_success_rate is a float")

# Cleanup
os.unlink(tmp_path)


# ─────────────────────────────────────────────────────────────
# T5 — save() writes valid JSON
# ─────────────────────────────────────────────────────────────
sep("T5 — save() output is valid JSON")

ev3 = Evaluator()
ev3.record("show profit", "finance.csv", RESOLVED_RESULT, 88.0)

with tempfile.NamedTemporaryFile(
    suffix=".json", delete=False, mode="w", encoding="utf-8"
) as tmp:
    save_path = tmp.name

ev3.save(save_path)

with open(save_path, "r", encoding="utf-8") as f:
    saved = json.load(f)

check("T5", "metrics" in saved, "saved JSON has 'metrics' key")
check("T5", "records" in saved, "saved JSON has 'records' key")
check("T5", saved["metrics"]["total"] == 1, "metrics.total = 1")
check("T5", len(saved["records"]) == 1, "records list has 1 entry")
check("T5", saved["records"][0]["query"] == "show profit",
      "record query preserved in JSON")

os.unlink(save_path)


# ─────────────────────────────────────────────────────────────
# T6 — timed_record context manager
# ─────────────────────────────────────────────────────────────
sep("T6 — timed_record context manager")

ev4 = Evaluator()
with timed_record(ev4, "show sales", "sales.csv") as ctx:
    ctx.result = RESOLVED_RESULT

check("T6", len(ev4.records) == 1, "timed_record stored one record")
check("T6", ev4.records[0].query == "show sales", "query stored by context manager")
check("T6", ev4.records[0].latency_ms >= 0, "latency_ms >= 0 (measured by perf_counter)")


# ─────────────────────────────────────────────────────────────
# T7 — SEMANTIC_REJECTION classification
# ─────────────────────────────────────────────────────────────
sep("T7 — SEMANTIC_REJECTION classification")

check("T7",
      classify_failure(SEMANTIC_REJECTION_RESULT) == FailureType.SEMANTIC_REJECTION,
      "semantic rejection classified correctly")

# Semantic applied=True should NOT be a rejection
check("T7",
      classify_failure(RESOLVED_PARTIAL) is None,
      "semantic applied=True with RESOLVED → no failure")


# ─────────────────────────────────────────────────────────────
# T8 — EXECUTION_ERROR classification
# ─────────────────────────────────────────────────────────────
sep("T8 — EXECUTION_ERROR classification")

check("T8",
      classify_failure(EXEC_ERROR_RESULT) == FailureType.EXECUTION_ERROR,
      "RESOLVED + errors list → EXECUTION_ERROR")

# RESOLVED + empty errors → None (success)
clean = {**EXEC_ERROR_RESULT, "errors": []}
check("T8",
      classify_failure(clean) is None,
      "RESOLVED + empty errors → None (success)")


# ─────────────────────────────────────────────────────────────
# T9 — partial_execution_rate only counts RESOLVED
# ─────────────────────────────────────────────────────────────
sep("T9 — partial_execution_rate denominator = RESOLVED only")

ev5 = Evaluator()
ev5.record("q1", "ds.csv", RESOLVED_PARTIAL, 10.0)   # RESOLVED PARTIAL
ev5.record("q2", "ds.csv", UNKNOWN_RESULT,   15.0)   # UNKNOWN (not resolved)
ev5.record("q3", "ds.csv", INCOMPLETE_RESULT, 12.0)  # INCOMPLETE (not resolved)

m5 = ev5.compute_metrics()
# Only 1 resolved record, and it's PARTIAL → rate = 1/1 = 1.0
check("T9", m5["partial_execution_rate"] == 1.0,
      "partial_execution_rate = 1.0 (1 partial / 1 resolved)")
check("T9", m5["success_rate"] == round(1/3, 4),
      "success_rate = 1/3 (1 resolved out of 3)")


# ─────────────────────────────────────────────────────────────
# T10 — empty evaluator safe
# ─────────────────────────────────────────────────────────────
sep("T10 — empty evaluator metrics safe")

ev_empty = Evaluator()
m_empty = ev_empty.compute_metrics()

check("T10", m_empty["total"] == 0, "total = 0 on empty evaluator")
check("T10", m_empty["success_rate"] == 0.0, "success_rate = 0.0")
check("T10", m_empty["avg_latency_ms"] == 0.0, "avg_latency_ms = 0.0")
check("T10", m_empty["failure_breakdown"] == {}, "empty failure_breakdown")


# ─────────────────────────────────────────────────────────────
# T11 — singleton get/reset
# ─────────────────────────────────────────────────────────────
sep("T11 — singleton get_evaluator / reset_evaluator")

ev_a = get_evaluator()
ev_b = get_evaluator()
check("T11", ev_a is ev_b, "get_evaluator returns same instance")

ev_reset = reset_evaluator()
check("T11", len(ev_reset.records) == 0, "reset clears records")
check("T11", get_evaluator() is ev_reset, "get_evaluator returns reset instance")


# ─────────────────────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"  PHASE 8 EVALUATOR TEST SUMMARY")
print(f"{'='*60}")
print(f"  Passed : {passed}")
print(f"  Failed : {failed}")
print(f"  Total  : {passed + failed}")

if failures:
    print(f"\n  FAILURES:")
    for f in failures:
        print(f"    [X] {f}")
else:
    print(f"\n  [OK] All Phase 8 checks passed!")

sys.exit(0 if failed == 0 else 1)
