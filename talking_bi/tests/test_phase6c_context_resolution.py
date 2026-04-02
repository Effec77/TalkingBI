"""
Phase 6C Context Resolution Test - 10 Scenarios
Strict validation of context resolution rules.
"""

import sys
import json

sys.path.insert(0, ".")

from services.context_resolver import create_resolver, ResolutionStatus


# Helper functions for assertions (define at top)
def assert_eq(actual, expected, message=""):
    if actual != expected:
        raise AssertionError(f"{message}: expected {expected}, got {actual}")


def assert_true(condition, message=""):
    if not condition:
        raise AssertionError(message)


print("=" * 70)
print("PHASE 6C: CONTEXT RESOLUTION - 10 SCENARIO TEST")
print("=" * 70)
print("PHASE 6C: CONTEXT RESOLUTION - 10 SCENARIO TEST")
print("=" * 70)

# Test configuration
KPI_CANDIDATES = ["Sales", "Quantity"]
AMBIGUITY_MAP = {
    "sales": ["gross_sales", "net_sales"],
    "profit": ["gross_profit", "net_profit"],
}
DASHBOARD_PLAN = {"kpis": ["Sales", "Quantity"]}

# Test results storage
results = []
failures = []


def run_turn(
    turn_num, query, parsed_intent, resolver, expected_status, expected_checks=None
):
    """Execute a single turn and validate."""
    print(f"\n[TURN {turn_num}] Query: '{query}'")
    print(f"  Parsed: {json.dumps(parsed_intent, indent=2)}")

    result = resolver.resolve(parsed_intent, DASHBOARD_PLAN)

    output = {
        "turn": turn_num,
        "query": query,
        "parsed_intent": parsed_intent,
        "resolution_result": {
            "status": result.status,
            "intent": result.intent,
            "source_map": result.source_map,
            "warnings": result.warnings,
            "missing_fields": result.missing_fields,
            "ambiguity": result.ambiguity,
        },
    }

    print(f"  Result Status: {result.status}")
    if result.intent:
        print(f"  Resolved Intent: {json.dumps(result.intent, indent=2)}")
    print(f"  Source Map: {result.source_map}")
    print(f"  Warnings: {result.warnings}")
    if result.missing_fields:
        print(f"  Missing Fields: {result.missing_fields}")
    if result.ambiguity:
        print(f"  Ambiguity: {result.ambiguity}")

    # Validate status
    if result.status != expected_status:
        failure = {
            "test": f"Turn {turn_num}",
            "issue": f"Status mismatch",
            "expected": expected_status,
            "actual": result.status,
        }
        failures.append(failure)
        print(f"  [X] FAILED: Expected {expected_status}, got {result.status}")
    else:
        print(f"  [OK] Status correct: {result.status}")

    # Run additional checks
    if expected_checks:
        for check_name, check_fn in expected_checks.items():
            try:
                check_fn(result)
                print(f"  [OK] Check '{check_name}' passed")
            except AssertionError as e:
                failure = {
                    "test": f"Turn {turn_num} - {check_name}",
                    "issue": str(e),
                    "expected": "Check condition",
                    "actual": "Failed",
                }
                failures.append(failure)
                print(f"  [X] FAILED: Check '{check_name}': {e}")

    results.append(output)
    return result


# Initialize resolver
resolver = create_resolver(KPI_CANDIDATES, AMBIGUITY_MAP)

print("\n" + "=" * 70)
print("TEST 1 — Basic Context Carryover")
print("=" * 70)

r1 = run_turn(
    1,
    "show sales",
    {"intent": "SEGMENT_BY", "kpi": "Sales", "dimension": None, "filter": None},
    resolver,
    "RESOLVED",
    {
        "kpi_is_user": lambda r: assert_eq(
            r.source_map.get("kpi"), "user", "KPI should be from user"
        ),
        "no_warnings": lambda r: assert_eq(
            len(r.warnings), 0, "Should have no warnings"
        ),
    },
)

r2 = run_turn(
    2,
    "now by region",
    {"intent": "SEGMENT_BY", "kpi": None, "dimension": "region", "filter": None},
    resolver,
    "RESOLVED",
    {
        "kpi_from_context": lambda r: assert_eq(
            r.source_map.get("kpi"), "context", "KPI should inherit from context"
        ),
        "dimension_is_user": lambda r: assert_eq(
            r.source_map.get("dimension"), "user", "Dimension should be from user"
        ),
        "kpi_is_sales": lambda r: assert_eq(
            r.intent.get("kpi"), "Sales", "KPI should be Sales from context"
        ),
    },
)

print("\n" + "=" * 70)
print("TEST 2 — Context Override")
print("=" * 70)

resolver2 = create_resolver(KPI_CANDIDATES, AMBIGUITY_MAP)

r1 = run_turn(
    1,
    "show sales",
    {"intent": "SEGMENT_BY", "kpi": "Sales", "dimension": None, "filter": None},
    resolver2,
    "RESOLVED",
)

r2 = run_turn(
    2,
    "show quantity by region",
    {"intent": "SEGMENT_BY", "kpi": "Quantity", "dimension": "region", "filter": None},
    resolver2,
    "RESOLVED",
    {
        "kpi_is_user": lambda r: assert_eq(
            r.source_map.get("kpi"), "user", "KPI should be from user (override)"
        ),
        "kpi_is_quantity": lambda r: assert_eq(
            r.intent.get("kpi"),
            "Quantity",
            "KPI should be Quantity (not Sales from context)",
        ),
    },
)

print("\n" + "=" * 70)
print("TEST 3 — Ambiguity Detection")
print("=" * 70)

resolver3 = create_resolver(KPI_CANDIDATES, AMBIGUITY_MAP)

r1 = run_turn(
    1,
    "show sales",
    {"intent": "SEGMENT_BY", "kpi": "sales", "dimension": None, "filter": None},
    resolver3,
    "AMBIGUOUS",
    {
        "has_ambiguity": lambda r: assert_true(
            r.ambiguity is not None, "Should have ambiguity data"
        ),
        "ambiguity_field_is_kpi": lambda r: assert_eq(
            r.ambiguity.get("field"), "kpi", "Ambiguity field should be kpi"
        ),
        "has_options": lambda r: assert_true(
            len(r.ambiguity.get("options", [])) > 0, "Should have ambiguity options"
        ),
    },
)

print("\n" + "=" * 70)
print("TEST 4 — Incomplete Query (No Context)")
print("=" * 70)

resolver4 = create_resolver(KPI_CANDIDATES, AMBIGUITY_MAP)

r1 = run_turn(
    1,
    "now by region",
    {"intent": "SEGMENT_BY", "kpi": None, "dimension": "region", "filter": None},
    resolver4,
    "INCOMPLETE",
    {
        "missing_kpi": lambda r: assert_true(
            "kpi" in r.missing_fields, "Should report missing KPI"
        ),
        "kpi_is_none": lambda r: assert_true(
            r.intent.get("kpi") is None, "KPI should be None"
        ),
        "has_source_map": lambda r: assert_true(
            "dimension" in r.source_map, "Dimension should have source map"
        ),
    },
)

print("\n" + "=" * 70)
print("TEST 5 — Fallback Trigger")
print("=" * 70)

resolver5 = create_resolver(KPI_CANDIDATES, AMBIGUITY_MAP)

r1 = run_turn(
    1,
    "show trends",
    {"intent": "EXPLAIN_TREND", "kpi": None, "dimension": None, "filter": None},
    resolver5,
    "RESOLVED",
    {
        "kpi_from_fallback": lambda r: assert_eq(
            r.source_map.get("kpi"), "fallback", "KPI should come from fallback"
        ),
        "kpi_is_sales": lambda r: assert_eq(
            r.intent.get("kpi"),
            "Sales",
            "KPI should be Sales (first in dashboard plan)",
        ),
        "has_fallback_warning": lambda r: assert_true(
            any(w.type == "fallback_used" and w.field == "kpi" for w in r.warnings),
            "Should have fallback warning",
        ),
    },
)

print("\n" + "=" * 70)
print("TEST 6 — Ambiguous Then Follow-up")
print("=" * 70)

resolver6 = create_resolver(KPI_CANDIDATES, AMBIGUITY_MAP)

r1 = run_turn(
    1,
    "show sales",
    {"intent": "SEGMENT_BY", "kpi": "sales", "dimension": None, "filter": None},
    resolver6,
    "AMBIGUOUS",
)

# After ambiguous turn, context should NOT be updated (only RESOLVED adds to context)
# So follow-up should still have no context
r2 = run_turn(
    2,
    "now by region",
    {"intent": "SEGMENT_BY", "kpi": None, "dimension": "region", "filter": None},
    resolver6,
    "INCOMPLETE",  # No context available, so still incomplete
    {
        "no_context_inherited": lambda r: assert_true(
            "kpi" in r.missing_fields,
            "KPI should be missing (no context from ambiguous turn)",
        )
    },
)

print("\n" + "=" * 70)
print("TEST 7 — Context Chain Correctness")
print("=" * 70)

resolver7 = create_resolver(KPI_CANDIDATES, AMBIGUITY_MAP)

r1 = run_turn(
    1,
    "show quantity",
    {"intent": "SEGMENT_BY", "kpi": "Quantity", "dimension": None, "filter": None},
    resolver7,
    "RESOLVED",
)

r2 = run_turn(
    2,
    "now by region",
    {"intent": "SEGMENT_BY", "kpi": None, "dimension": "region", "filter": None},
    resolver7,
    "RESOLVED",
    {
        "kpi_is_quantity_from_context": lambda r: assert_eq(
            r.intent.get("kpi"), "Quantity", "KPI should be Quantity from context"
        )
    },
)

r3 = run_turn(
    3,
    "show sales",
    {"intent": "SEGMENT_BY", "kpi": "Sales", "dimension": None, "filter": None},
    resolver7,
    "RESOLVED",
    {
        "kpi_from_user": lambda r: assert_eq(
            r.source_map.get("kpi"), "user", "KPI should come from user"
        ),
        "no_dimension_inheritance": lambda r: assert_eq(
            r.intent.get("dimension"),
            None,
            "Dimension should be null when user provides KPI",
        ),
    },
)

r4 = run_turn(
    4,
    "now by product",
    {"intent": "SEGMENT_BY", "kpi": None, "dimension": "product", "filter": None},
    resolver7,
    "RESOLVED",
    {
        "kpi_is_sales_from_context": lambda r: assert_eq(
            r.intent.get("kpi"),
            "Sales",
            "KPI should be Sales from context (not Quantity)",
        )
    },
)

print("\n" + "=" * 70)
print("TEST 8 — Compare Intent")
print("=" * 70)

resolver8 = create_resolver(KPI_CANDIDATES, AMBIGUITY_MAP)

r1 = run_turn(
    1,
    "show sales",
    {"intent": "SEGMENT_BY", "kpi": "Sales", "dimension": None, "filter": None},
    resolver8,
    "RESOLVED",
)

r2 = run_turn(
    2,
    "compare with quantity",
    {
        "intent": "COMPARE",
        "kpi": None,
        "dimension": None,
        "filter": None,
        "kpi_2": "quantity",
    },
    resolver8,
    "RESOLVED",
    {
        "kpi_1_from_context": lambda r: assert_eq(
            r.source_map.get("kpi_1"), "context", "KPI-1 should inherit from context"
        ),
        "kpi_1_is_sales": lambda r: assert_eq(
            r.intent.get("kpi_1"), "Sales", "KPI-1 should be Sales from context"
        ),
        "kpi_2_from_user": lambda r: assert_eq(
            r.source_map.get("kpi_2"), "user", "KPI-2 should come from user"
        ),
        "kpi_2_is_quantity": lambda r: assert_eq(
            r.intent.get("kpi_2"), "Quantity", "KPI-2 should be Quantity (normalized)"
        ),
    },
)

print("\n" + "=" * 70)
print("TEST 9 — UNKNOWN Intent")
print("=" * 70)

resolver9 = create_resolver(KPI_CANDIDATES, AMBIGUITY_MAP)

r1 = run_turn(
    1,
    "random analytics magic",
    {"intent": "UNKNOWN", "kpi": None, "dimension": None, "filter": None},
    resolver9,
    "UNKNOWN",
    {
        "intent_is_none": lambda r: assert_true(
            r.intent is None, "Intent should be None for UNKNOWN"
        ),
        "no_context_used": lambda r: assert_eq(
            r.context_used, False, "Context should not be used for UNKNOWN"
        ),
        "no_warnings": lambda r: assert_eq(
            len(r.warnings), 0, "No warnings for UNKNOWN"
        ),
    },
)

print("\n" + "=" * 70)
print("TEST 10 — First Query Edge Case")
print("=" * 70)

resolver10 = create_resolver(KPI_CANDIDATES, AMBIGUITY_MAP)

r1 = run_turn(
    1,
    "show quantity by product",
    {"intent": "SEGMENT_BY", "kpi": "Quantity", "dimension": "product", "filter": None},
    resolver10,
    "RESOLVED",
    {
        "all_from_user": lambda r: assert_eq(
            all(v == "user" for v in r.source_map.values()),
            True,
            "All fields should come from user on first query",
        ),
        "no_warnings": lambda r: assert_eq(
            len(r.warnings), 0, "First complete query should have no warnings"
        ),
    },
)

# Print final summary
print("\n" + "=" * 70)
print("FINAL SUMMARY")
print("=" * 70)

passed = len(results) - len(failures)
total = len(results)

summary = {"passed": passed, "failed": len(failures), "failures": failures}

print(json.dumps(summary, indent=2))

print(f"\nTotal: {passed}/{total} checks passed")
if failures:
    print(f"[X] {len(failures)} failure(s) detected")
else:
    print("[OK] All tests passed!")
