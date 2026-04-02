"""
Phase 6B: Intent Parser Unit Tests
Tests parser produces minimal, non-assumptive, structurally correct intent.
"""

import sys

sys.path.insert(0, ".")

print("=" * 70)
print("PHASE 6B: INTENT PARSER UNIT TESTS")
print("=" * 70)
print("Validating parser produces minimal, raw output for resolver")
print()

from services.intent_parser import parse_intent

# Test results
passed = 0
failed = 0
failures = []


def test_parser(test_name, query, expected_checks):
    """Test parser output for a query."""
    global passed, failed, failures

    print(f"\n[TEST] {test_name}")
    print(f"Query: '{query}'")
    print("-" * 70)

    result = parse_intent(query)
    print(f"Parsed: {result}")

    test_passed = True
    for check_name, check_fn in expected_checks.items():
        try:
            check_fn(result)
            print(f"  [OK] {check_name}")
        except AssertionError as e:
            print(f"  [X] FAILED: {check_name}: {e}")
            test_passed = False
            failures.append({"test": test_name, "check": check_name, "issue": str(e)})

    if test_passed:
        passed += 1
    else:
        failed += 1

    return test_passed


def assert_eq(actual, expected, message=""):
    if actual != expected:
        raise AssertionError(f"{message}: expected {expected}, got {actual}")


def assert_true(condition, message=""):
    if not condition:
        raise AssertionError(message)


def assert_is_none(value, message=""):
    if value is not None:
        raise AssertionError(f"{message}: expected None, got {value}")


# TEST A: COMPARE Parsing
print("=" * 70)
print("TEST A — Compare Parsing (CRITICAL)")
print("=" * 70)

test_parser(
    "COMPARE with quantity",
    "compare with quantity",
    {
        "intent_is_COMPARE": lambda r: assert_eq(r["intent"], "COMPARE", "Intent"),
        "kpi_is_null": lambda r: assert_is_none(r["kpi"], "kpi must be null"),
        "kpi_1_is_null": lambda r: assert_is_none(
            r["kpi_1"], "kpi_1 must be null for context"
        ),
        "kpi_2_is_quantity": lambda r: assert_eq(
            r["kpi_2"], "quantity", "kpi_2 must be 'quantity'"
        ),
    },
)

test_parser(
    "COMPARE sales versus quantity",
    "compare sales versus quantity",
    {
        "intent_is_COMPARE": lambda r: assert_eq(r["intent"], "COMPARE", "Intent"),
        "kpi_1_is_null": lambda r: assert_is_none(r["kpi_1"], "kpi_1 must be null"),
        "kpi_2_is_quantity": lambda r: assert_eq(r["kpi_2"], "quantity", "kpi_2"),
    },
)

# TEST B: Trend Parsing
print("\n" + "=" * 70)
print("TEST B — Trend Parsing")
print("=" * 70)

test_parser(
    "show trends",
    "show trends",
    {
        "intent_is_EXPLAIN_TREND": lambda r: assert_eq(
            r["intent"], "EXPLAIN_TREND", "Intent"
        ),
        "kpi_is_null": lambda r: assert_is_none(
            r["kpi"], "kpi must be null (not pre-filled)"
        ),
    },
)

test_parser(
    "sales growth over time",
    "sales growth over time",
    {
        "intent_is_EXPLAIN_TREND": lambda r: assert_eq(
            r["intent"], "EXPLAIN_TREND", "Intent"
        ),
        "kpi_is_sales": lambda r: assert_eq(
            r["kpi"], "sales", "kpi should be raw 'sales'"
        ),
    },
)

test_parser(
    "show trends for revenue",
    "show trends for revenue",
    {
        "intent_is_EXPLAIN_TREND": lambda r: assert_eq(
            r["intent"], "EXPLAIN_TREND", "Intent"
        ),
        "kpi_is_revenue": lambda r: assert_eq(r["kpi"], "revenue", "kpi"),
    },
)

# TEST C: Ambiguity Preservation
print("\n" + "=" * 70)
print("TEST C — Ambiguity Preservation (CRITICAL)")
print("=" * 70)

test_parser(
    "show sales (lowercase - ambiguous)",
    "show sales",
    {
        "intent_is_SEGMENT_BY": lambda r: assert_eq(
            r["intent"], "SEGMENT_BY", "Intent"
        ),
        "kpi_is_lowercase": lambda r: assert_eq(
            r["kpi"], "sales", "kpi must be lowercase 'sales' for ambiguity detection"
        ),
        "kpi_not_capitalized": lambda r: assert_true(
            r["kpi"] != "Sales", "kpi must NOT be capitalized"
        ),
    },
)

test_parser(
    "show Sales (capitalized - valid)",
    "show Sales",
    {
        "intent_is_SEGMENT_BY": lambda r: assert_eq(
            r["intent"], "SEGMENT_BY", "Intent"
        ),
        "kpi_is_Sales": lambda r: assert_eq(
            r["kpi"], "Sales", "kpi must preserve casing 'Sales'"
        ),
    },
)

# TEST D: Partial Query
print("\n" + "=" * 70)
print("TEST D — Partial Query (No Pre-filling)")
print("=" * 70)

test_parser(
    "now by region",
    "now by region",
    {
        "intent_is_SEGMENT_BY": lambda r: assert_eq(
            r["intent"], "SEGMENT_BY", "Intent"
        ),
        "kpi_is_null": lambda r: assert_is_none(
            r["kpi"], "kpi must be null (not inferred)"
        ),
        "dimension_is_region": lambda r: assert_eq(
            r["dimension"], "region", "dimension"
        ),
    },
)

test_parser(
    "by product",
    "by product",
    {
        "intent_is_SEGMENT_BY": lambda r: assert_eq(
            r["intent"], "SEGMENT_BY", "Intent"
        ),
        "kpi_is_null": lambda r: assert_is_none(r["kpi"], "kpi must be null"),
        "dimension_is_product": lambda r: assert_eq(
            r["dimension"], "product", "dimension"
        ),
    },
)

test_parser(
    "show",
    "show",
    {
        "kpi_is_null": lambda r: assert_is_none(r["kpi"], "kpi must be null"),
        "dimension_is_null": lambda r: assert_is_none(
            r["dimension"], "dimension must be null"
        ),
    },
)

# Additional edge cases
print("\n" + "=" * 70)
print("TEST E — Additional Edge Cases")
print("=" * 70)

test_parser(
    "UNKNOWN query",
    "random gibberish xyz",
    {
        "intent_is_UNKNOWN": lambda r: assert_eq(r["intent"], "UNKNOWN", "Intent"),
    },
)

test_parser(
    "SUMMARIZE query",
    "what is the summary",
    {
        "intent_is_SUMMARIZE": lambda r: assert_eq(r["intent"], "SUMMARIZE", "Intent"),
    },
)

test_parser(
    "FILTER query",
    "show last quarter",
    {
        "intent_is_FILTER": lambda r: assert_eq(r["intent"], "FILTER", "Intent"),
    },
)

# Final Summary
print("\n" + "=" * 70)
print("FINAL SUMMARY")
print("=" * 70)

total = passed + failed
print(f"\nPassed: {passed}/{total}")
print(f"Failed: {failed}/{total}")

if failures:
    print("\nFailures:")
    for f in failures:
        print(f"  - {f['test']} ({f['check']}): {f['issue']}")
    print("\n[RESULT] SOME TESTS FAILED - Parser needs adjustment")
else:
    print("\n[RESULT] ALL TESTS PASSED - Parser produces correct minimal output")
    print("\nParser (6B) output verified for Resolver (6C) integration")

print("=" * 70)
