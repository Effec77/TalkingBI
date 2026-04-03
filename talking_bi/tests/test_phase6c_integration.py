"""
Phase 6C End-to-End Integration Test
Full pipeline execution via API calls
"""

import sys
import json

sys.path.insert(0, ".")

print("=" * 70)
print("PHASE 6C: END-TO-END INTEGRATION TEST")
print("=" * 70)
print("Testing Context Resolution through full API pipeline")
print()

import asyncio
import pandas as pd
from dataclasses import dataclass
from api.query import query_endpoint, QueryPayload
from services.session_manager import create_session, get_session, delete_session
from services.conversation_manager import get_conversation_manager


# Test configuration
@dataclass
class MockDataset:
    filename: str
    columns: list
    shape: tuple


# Create test data
print("[SETUP] Creating test dataset...")
df = pd.DataFrame(
    {
        "date": pd.date_range("2024-01-01", periods=10, freq="D"),
        "sales": [100, 120, 90, 150, 180, 200, 190, 220, 250, 230],
        "quantity": [10, 12, 9, 15, 18, 20, 19, 22, 25, 23],
        "region": ["North"] * 5 + ["South"] * 5,
        "product": ["A", "B"] * 5,
    }
)

metadata = MockDataset(filename="test.csv", columns=list(df.columns), shape=df.shape)

# Create session
session_id = create_session(df, metadata)
print(f"[OK] Session created: {session_id}")

# Test results storage
test_results = []
failures = []


async def run_api_turn(turn_num, query, expected_status, validation_checks=None):
    """Execute a single turn via API and validate."""
    print(f"\n[TURN {turn_num}] Query: '{query}'")
    print("-" * 70)

    payload = QueryPayload(query=query)

    try:
        response = await query_endpoint(session_id, payload)

        # Extract key metrics
        result = {
            "turn": turn_num,
            "query": query,
            "status": response.get("status"),
            "intent_raw": response.get("intent"),  # API returns intent (resolved)
            "intent_resolved": response.get("intent"),  # alias for compatibility
            "source_map": response.get("trace", {}).get("mapped_fields", {}),
            "warnings": response.get("warnings", []),
            "charts_generated": len(response.get("charts", [])),
            "insights_generated": len(response.get("insights", [])),
            "execution_trace": response.get("trace", {}),
            "errors": response.get("errors", []),
        }

        print(f"  Status: {result['status']}")
        print(f"  Intent Raw: {json.dumps(result['intent_raw'], indent=2)[:100]}...")
        if result["intent_resolved"]:
            print(
                f"  Intent Resolved: {json.dumps(result['intent_resolved'], indent=2)[:100]}..."
            )
        print(f"  Source Map: {result['source_map']}")
        print(
            f"  Charts: {result['charts_generated']}, Insights: {result['insights_generated']}"
        )
        print(f"  Execution Trace: {result['execution_trace']}")

        # Validate status
        if result["status"] != expected_status:
            failure = {
                "test": f"Turn {turn_num}",
                "issue": f"Status mismatch",
                "expected": expected_status,
                "actual": result["status"],
            }
            failures.append(failure)
            print(f"  [X] FAILED: Expected {expected_status}, got {result['status']}")
        else:
            print(f"  [OK] Status correct: {result['status']}")

        # Run validation checks
        if validation_checks:
            for check_name, check_fn in validation_checks.items():
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

        test_results.append(result)
        return result

    except Exception as e:
        print(f"  [X] ERROR: {e}")
        failure = {
            "test": f"Turn {turn_num}",
            "issue": f"Exception: {str(e)}",
            "expected": expected_status,
            "actual": "Exception",
        }
        failures.append(failure)
        return None


# Helper functions for assertions
def assert_eq(actual, expected, message=""):
    if actual != expected:
        raise AssertionError(f"{message}: expected {expected}, got {actual}")


def assert_true(condition, message=""):
    if not condition:
        raise AssertionError(message)


print("\n" + "=" * 70)
print("TEST 1 — Context Carryover (Execution Check)")
print("=" * 70)

r1 = asyncio.get_event_loop().run_until_complete(
    run_api_turn(
        1,
        "show sales",
        "RESOLVED",
        {
            "kpi_is_sales": lambda r: assert_eq(
                r["intent_resolved"].get("kpi"), "Sales", "KPI should be Sales"
            ),
            "charts_generated": lambda r: assert_true(
                r["charts_generated"] >= 1, "Should generate at least 1 chart"
            ),
            "insights_generated": lambda r: assert_true(
                r["insights_generated"] >= 1, "Should generate at least 1 insight"
            ),
            "no_errors": lambda r: assert_eq(
                len(r["errors"]), 0, "Should have no errors"
            ),
        },
    )
)

r2 = asyncio.get_event_loop().run_until_complete(
    run_api_turn(
        2,
        "now by region",
        "RESOLVED",
        {
            "kpi_from_context": lambda r: assert_eq(
                r["source_map"].get("kpi"), "context", "KPI should come from context"
            ),
            "kpi_is_sales": lambda r: assert_eq(
                r["intent_resolved"].get("kpi"),
                "Sales",
                "KPI should be Sales from context",
            ),
            "charts_generated": lambda r: assert_true(
                r["charts_generated"] >= 1, "Should generate charts"
            ),
            "execution_trace_valid": lambda r: assert_true(
                len(r["execution_trace"]) > 0, "Should have execution trace"
            ),
        },
    )
)

print("\n" + "=" * 70)
print("TEST 2 — Context Override")
print("=" * 70)

# Clear conversation history for fresh test
conv_manager = get_conversation_manager()
conv_manager.clear_session(session_id)

r1 = asyncio.get_event_loop().run_until_complete(
    run_api_turn(1, "show sales", "RESOLVED")
)

r2 = asyncio.get_event_loop().run_until_complete(
    run_api_turn(
        2,
        "show quantity by region",
        "RESOLVED",
        {
            "kpi_is_quantity": lambda r: assert_eq(
                r["intent_resolved"].get("kpi"),
                "Quantity",
                "KPI should be Quantity (override)",
            ),
            "no_context_leak": lambda r: assert_eq(
                r["source_map"].get("kpi"),
                "user",
                "KPI should be from user, not context",
            ),
        },
    )
)

print("\n" + "=" * 70)
print("TEST 3 — COMPARE Integration (CRITICAL)")
print("=" * 70)

conv_manager.clear_session(session_id)

r1 = asyncio.get_event_loop().run_until_complete(
    run_api_turn(1, "show sales", "RESOLVED")
)

r2 = asyncio.get_event_loop().run_until_complete(
    run_api_turn(
        2,
        "compare with quantity",
        "RESOLVED",
        {
            "has_kpi_1": lambda r: assert_true(
                r["intent_resolved"].get("kpi_1") is not None, "Should have kpi_1"
            ),
            "has_kpi_2": lambda r: assert_true(
                r["intent_resolved"].get("kpi_2") is not None, "Should have kpi_2"
            ),
            "kpi_1_is_sales": lambda r: assert_eq(
                r["intent_resolved"].get("kpi_1"), "Sales", "kpi_1 should be Sales"
            ),
            "kpi_2_is_quantity": lambda r: assert_eq(
                r["intent_resolved"].get("kpi_2"),
                "Quantity",
                "kpi_2 should be Quantity",
            ),
            "no_crash": lambda r: assert_eq(
                r["status"], "RESOLVED", "Should not crash"
            ),
        },
    )
)

print("\n" + "=" * 70)
print("TEST 4 — UNKNOWN Intent (Execution Block)")
print("=" * 70)

conv_manager.clear_session(session_id)

r1 = asyncio.get_event_loop().run_until_complete(
    run_api_turn(
        1,
        "random analytics magic",
        "UNKNOWN",
        {
            "status_unknown": lambda r: assert_eq(
                r["status"], "UNKNOWN", "Status should be UNKNOWN"
            ),
            "no_pipeline_execution": lambda r: assert_eq(
                r["charts_generated"], 0, "Should NOT generate charts"
            ),
            "no_insights": lambda r: assert_eq(
                r["insights_generated"], 0, "Should NOT generate insights"
            ),
            "no_execution_trace": lambda r: assert_eq(
                len(r["execution_trace"]), 0, "Should NOT have execution trace"
            ),
        },
    )
)

print("\n" + "=" * 70)
print("TEST 5 — Fallback Execution")
print("=" * 70)

conv_manager.clear_session(session_id)

r1 = asyncio.get_event_loop().run_until_complete(
    run_api_turn(
        1,
        "show trends",
        "RESOLVED",
        {
            "kpi_resolved": lambda r: assert_true(
                r["intent_resolved"].get("kpi") is not None,
                "KPI should be resolved via fallback",
            ),
            "charts_generated": lambda r: assert_true(
                r["charts_generated"] >= 1, "Should generate charts"
            ),
            "has_fallback_warning": lambda r: assert_true(
                any(w.get("type") == "fallback_used" for w in r["warnings"]),
                "Should have fallback warning",
            ),
        },
    )
)

print("\n" + "=" * 70)
print("TEST 6 — Ambiguity Block")
print("=" * 70)

conv_manager.clear_session(session_id)

r1 = asyncio.get_event_loop().run_until_complete(
    run_api_turn(
        1,
        "show sales",
        "AMBIGUOUS",
        {
            "status_ambiguous": lambda r: assert_eq(
                r["status"], "AMBIGUOUS", "Status should be AMBIGUOUS"
            ),
            "no_execution": lambda r: assert_eq(
                r["charts_generated"], 0, "Should NOT generate charts"
            ),
            "no_insights": lambda r: assert_eq(
                r["insights_generated"], 0, "Should NOT generate insights"
            ),
        },
    )
)

print("\n" + "=" * 70)
print("TEST 7 — Context Chain Stability")
print("=" * 70)

conv_manager.clear_session(session_id)

r1 = asyncio.get_event_loop().run_until_complete(
    run_api_turn(1, "show quantity", "RESOLVED")
)

r2 = asyncio.get_event_loop().run_until_complete(
    run_api_turn(2, "now by region", "RESOLVED")
)

r3 = asyncio.get_event_loop().run_until_complete(
    run_api_turn(3, "show sales", "RESOLVED")
)

r4 = asyncio.get_event_loop().run_until_complete(
    run_api_turn(
        4,
        "now by product",
        "RESOLVED",
        {
            "kpi_is_sales": lambda r: assert_eq(
                r["intent_resolved"].get("kpi"),
                "Sales",
                "KPI should be Sales (not stale Quantity)",
            ),
            "charts_correct": lambda r: assert_true(
                r["charts_generated"] >= 1, "Should generate charts for Sales"
            ),
        },
    )
)

print("\n" + "=" * 70)
print("TEST 8 — Mixed Flow Stress")
print("=" * 70)

conv_manager.clear_session(session_id)

r1 = asyncio.get_event_loop().run_until_complete(
    run_api_turn(1, "show sales", "RESOLVED")
)

r2 = asyncio.get_event_loop().run_until_complete(
    run_api_turn(2, "now by region", "RESOLVED")
)

r3 = asyncio.get_event_loop().run_until_complete(
    run_api_turn(3, "compare with quantity", "RESOLVED")
)

r4 = asyncio.get_event_loop().run_until_complete(
    run_api_turn(4, "show trends", "RESOLVED")
)

r5 = asyncio.get_event_loop().run_until_complete(
    run_api_turn(
        5,
        "random text",
        "UNKNOWN",
        {
            "no_state_corruption": lambda r: assert_eq(
                r["status"], "UNKNOWN", "UNKNOWN should not break session"
            ),
        },
    )
)

# Verify session still works after UNKNOWN
r6 = asyncio.get_event_loop().run_until_complete(
    run_api_turn(
        6,
        "show sales",
        "RESOLVED",
        {
            "session_recovered": lambda r: assert_eq(
                r["status"], "RESOLVED", "Session should work after UNKNOWN"
            ),
        },
    )
)

# Cleanup
print("\n" + "=" * 70)
print("[CLEANUP] Removing test session...")
conv_manager.clear_session(session_id)
try:
    delete_session(session_id)
except:
    pass
print("[OK] Test session cleaned up")

# Final Summary
print("\n" + "=" * 70)
print("FINAL SUMMARY")
print("=" * 70)

passed = len(test_results) - len(failures)
total_checks = len(test_results)

summary = {
    "total_tests": 8,
    "passed": passed,
    "failed": len(failures),
    "failures": failures,
}

print(json.dumps(summary, indent=2))

print(f"\nTotal: {passed}/{total_checks} checks passed")
if failures:
    print(f"[X] {len(failures)} failure(s) detected")
    print("\nCritical Issues:")
    for f in failures:
        print(f"  - {f['test']}: {f['issue']}")
else:
    print("[OK] All integration tests passed!")
    print("\nPhase 6C End-to-End: VERIFIED")
    print("Context Resolution integrates cleanly with Execution Pipeline")

print("=" * 70)
