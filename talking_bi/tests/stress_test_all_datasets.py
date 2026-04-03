"""
TalkingBI Production Stress Test
Real datasets: ecommerce, saas, finance
Tests: 7 flows, 25+ turns
Validates: Parser, Resolver, Execution, Context handling
"""

import sys
import json
import asyncio
import pandas as pd
from dataclasses import dataclass

sys.path.insert(0, ".")

from api.query import query_endpoint, QueryPayload
from services.session_manager import create_session, delete_session
from services.conversation_manager import get_conversation_manager

print("=" * 80)
print("TALKINGBI PRODUCTION STRESS TEST")
print("=" * 80)
print("Datasets: ecommerce, saas, finance")
print("Flows: 7 | Expected Turns: 25+")
print("=" * 80)

# Results tracking
test_results = []
critical_failures = []
system_weaknesses = []
passed = 0
failed = 0


@dataclass
class TestCase:
    flow: str
    dataset: str
    turn: int
    query: str
    expected_checks: dict


async def run_stress_turn(session_id, flow, dataset, turn_num, query, checks):
    """Execute a single turn and validate results."""
    global passed, failed, critical_failures

    print(f"\n[{flow}] T{turn_num}: '{query}'")
    print("-" * 80)

    payload = QueryPayload(query=query)

    try:
        response = await query_endpoint(session_id, payload)

        result = {
            "flow": flow,
            "dataset": dataset,
            "turn": turn_num,
            "query": query,
            "status": response.get("status"),
            "intent_resolved": response.get("intent"),  # alias for compatibility
            "source_map": response.get("trace", {}).get("mapped_fields", {}),
            "charts": len(response.get("charts", [])),
            "insights": len(response.get("insights", [])),
            "errors": response.get("errors", []),
            "warnings": response.get("warnings", []),
        }

        print(f"  Status: {result['status']}")
        print(f"  Intent: {json.dumps(result['intent_resolved'], indent=2)[:150]}")
        print(f"  Charts: {result['charts']}, Insights: {result['insights']}")

        # Run validations
        turn_passed = True
        for check_name, check_fn in checks.items():
            try:
                check_fn(result)
                print(f"  [OK] {check_name}")
            except AssertionError as e:
                print(f"  [X] FAILED: {check_name}: {e}")
                turn_passed = False
                critical_failures.append(
                    {
                        "flow": flow,
                        "turn": turn_num,
                        "query": query,
                        "check": check_name,
                        "error": str(e),
                    }
                )

        if turn_passed:
            passed += 1
        else:
            failed += 1

        test_results.append(result)
        return result

    except Exception as e:
        print(f"  [X] CRITICAL ERROR: {e}")
        failed += 1
        critical_failures.append(
            {
                "flow": flow,
                "turn": turn_num,
                "query": query,
                "check": "EXECUTION",
                "error": str(e),
            }
        )
        return None


# Assertion helpers
def assert_eq(actual, expected, msg=""):
    if actual != expected:
        raise AssertionError(f"{msg}: expected {expected}, got {actual}")


def assert_true(cond, msg=""):
    if not cond:
        raise AssertionError(msg)


def assert_not_null(val, msg=""):
    if val is None:
        raise AssertionError(f"{msg}: expected non-null")


# ============================================================
# DATASET 1: ECOMMERCE
# ============================================================

print("\n" + "=" * 80)
print("DATASET 1: ECOMMERCE")
print("=" * 80)

# Load ecommerce dataset
print("\n[SETUP] Loading ecommerce.csv...")
df_ecom = pd.read_csv(r"D:\datasets for TalkingBI\ecommerce.csv")
print(f"  Rows: {len(df_ecom)}, Columns: {list(df_ecom.columns)}")


@dataclass
class MockDataset:
    filename: str
    columns: list
    shape: tuple


session_ecom = create_session(
    df_ecom, MockDataset("ecommerce.csv", list(df_ecom.columns), df_ecom.shape)
)
print(f"  Session: {session_ecom}")

conv_mgr = get_conversation_manager()

# Flow 1: Business Exploration
print("\n" + "-" * 80)
print("FLOW 1: Business Exploration")
print("-" * 80)

r1 = asyncio.get_event_loop().run_until_complete(
    run_stress_turn(
        session_ecom,
        "Flow 1",
        "ecommerce",
        1,
        "show revenue",
        {
            "status_resolved": lambda r: assert_eq(r["status"], "RESOLVED", "Status"),
            "has_charts": lambda r: assert_true(
                r["charts"] > 0, "Should generate charts"
            ),
            "kpi_is_revenue": lambda r: assert_true(
                r["intent_resolved"]
                and "revenue" in str(r["intent_resolved"].get("kpi", "")).lower(),
                "KPI should be revenue",
            ),
        },
    )
)

r2 = asyncio.get_event_loop().run_until_complete(
    run_stress_turn(
        session_ecom,
        "Flow 1",
        "ecommerce",
        2,
        "by region",
        {
            "status_resolved": lambda r: assert_eq(r["status"], "RESOLVED", "Status"),
            "context_used": lambda r: assert_true(
                any(w.get("type") == "context_inheritance" for w in r["warnings"]),
                "Should use context",
            ),
            "dimension_is_region": lambda r: assert_true(
                r["intent_resolved"]
                and r["intent_resolved"].get("dimension") == "region",
                "Dimension should be region",
            ),
        },
    )
)

r3 = asyncio.get_event_loop().run_until_complete(
    run_stress_turn(
        session_ecom,
        "Flow 1",
        "ecommerce",
        3,
        "by product category",
        {
            "status_resolved": lambda r: assert_eq(r["status"], "RESOLVED", "Status"),
            "dimension_correct": lambda r: assert_true(
                r["intent_resolved"]
                and "category"
                in str(r["intent_resolved"].get("dimension", "")).lower(),
                "Dimension should be category",
            ),
        },
    )
)

# Clear context for COMPARE test
conv_mgr.clear_session(session_ecom)

r4 = asyncio.get_event_loop().run_until_complete(
    run_stress_turn(
        session_ecom,
        "Flow 1",
        "ecommerce",
        4,
        "compare with quantity",
        {
            "status_resolved": lambda r: assert_eq(r["status"], "RESOLVED", "Status"),
            "compare_both_kpis": lambda r: assert_true(
                r["intent_resolved"]
                and r["intent_resolved"].get("kpi_1")
                and r["intent_resolved"].get("kpi_2"),
                "COMPARE needs both kpi_1 and kpi_2",
            ),
        },
    )
)

r5 = asyncio.get_event_loop().run_until_complete(
    run_stress_turn(
        session_ecom,
        "Flow 1",
        "ecommerce",
        5,
        "filter electronics",
        {
            "status_resolved": lambda r: assert_eq(r["status"], "RESOLVED", "Status"),
            "filter_applied": lambda r: assert_true(
                r["intent_resolved"]
                and "electronics"
                in str(r["intent_resolved"].get("filter", "")).lower(),
                "Filter should be electronics",
            ),
        },
    )
)

# Flow 2: Noisy Queries
print("\n" + "-" * 80)
print("FLOW 2: Noisy Queries")
print("-" * 80)

conv_mgr.clear_session(session_ecom)

r1 = asyncio.get_event_loop().run_until_complete(
    run_stress_turn(
        session_ecom,
        "Flow 2",
        "ecommerce",
        1,
        "revenue numbers",
        {
            "status_resolved": lambda r: assert_eq(r["status"], "RESOLVED", "Status"),
            "handles_noise": lambda r: assert_true(
                r["charts"] > 0, "Should handle noisy query"
            ),
        },
    )
)

r2 = asyncio.get_event_loop().run_until_complete(
    run_stress_turn(
        session_ecom,
        "Flow 2",
        "ecommerce",
        2,
        "region wise",
        {
            "status_resolved": lambda r: assert_eq(r["status"], "RESOLVED", "Status"),
        },
    )
)

r3 = asyncio.get_event_loop().run_until_complete(
    run_stress_turn(
        session_ecom,
        "Flow 2",
        "ecommerce",
        3,
        "electronics only",
        {
            "status_resolved_or_incomplete": lambda r: assert_true(
                r["status"] in ["RESOLVED", "INCOMPLETE"], "Should handle partial query"
            ),
        },
    )
)

# Flow 3: Edge Cases
print("\n" + "-" * 80)
print("FLOW 3: Edge Cases (Null Handling)")
print("-" * 80)

conv_mgr.clear_session(session_ecom)

r1 = asyncio.get_event_loop().run_until_complete(
    run_stress_turn(
        session_ecom,
        "Flow 3",
        "ecommerce",
        1,
        "show revenue",
        {
            "status_resolved": lambda r: assert_eq(r["status"], "RESOLVED", "Status"),
        },
    )
)

r2 = asyncio.get_event_loop().run_until_complete(
    run_stress_turn(
        session_ecom,
        "Flow 3",
        "ecommerce",
        2,
        "filter null categories",
        {
            "no_crash": lambda r: assert_true(
                r["status"] != "ERROR", "Should not crash on null filter"
            ),
        },
    )
)

# ============================================================
# DATASET 2: SAAS
# ============================================================

print("\n" + "=" * 80)
print("DATASET 2: SAAS")
print("=" * 80)

print("\n[SETUP] Loading saas.csv...")
df_saas = pd.read_csv(r"D:\datasets for TalkingBI\saas.csv")
print(f"  Rows: {len(df_saas)}, Columns: {list(df_saas.columns)}")

session_saas = create_session(
    df_saas, MockDataset("saas.csv", list(df_saas.columns), df_saas.shape)
)
print(f"  Session: {session_saas}")

# Flow 4: Product Metrics
print("\n" + "-" * 80)
print("FLOW 4: Product Metrics")
print("-" * 80)

r1 = asyncio.get_event_loop().run_until_complete(
    run_stress_turn(
        session_saas,
        "Flow 4",
        "saas",
        1,
        "show revenue",
        {
            "status_resolved": lambda r: assert_eq(r["status"], "RESOLVED", "Status"),
            "has_charts": lambda r: assert_true(
                r["charts"] > 0, "Should generate charts"
            ),
        },
    )
)

r2 = asyncio.get_event_loop().run_until_complete(
    run_stress_turn(
        session_saas,
        "Flow 4",
        "saas",
        2,
        "show churn",
        {
            "status_resolved": lambda r: assert_eq(r["status"], "RESOLVED", "Status"),
            "handles_churn": lambda r: assert_true(
                r["intent_resolved"]
                and "churn" in str(r["intent_resolved"].get("kpi", "")).lower(),
                "Should handle churn KPI",
            ),
        },
    )
)

r3 = asyncio.get_event_loop().run_until_complete(
    run_stress_turn(
        session_saas,
        "Flow 4",
        "saas",
        3,
        "by country",
        {
            "status_resolved": lambda r: assert_eq(r["status"], "RESOLVED", "Status"),
            "dimension_country": lambda r: assert_true(
                r["intent_resolved"]
                and r["intent_resolved"].get("dimension") == "country",
                "Dimension should be country",
            ),
        },
    )
)

r4 = asyncio.get_event_loop().run_until_complete(
    run_stress_turn(
        session_saas,
        "Flow 4",
        "saas",
        4,
        "show trends",
        {
            "status_resolved": lambda r: assert_eq(r["status"], "RESOLVED", "Status"),
            "trend_intent": lambda r: assert_true(
                r["intent_resolved"]
                and r["intent_resolved"].get("intent") == "EXPLAIN_TREND",
                "Should be EXPLAIN_TREND",
            ),
        },
    )
)

# Flow 5: Ambiguity
print("\n" + "-" * 80)
print("FLOW 5: Ambiguity Handling")
print("-" * 80)

conv_mgr.clear_session(session_saas)

r1 = asyncio.get_event_loop().run_until_complete(
    run_stress_turn(
        session_saas,
        "Flow 5",
        "saas",
        1,
        "show usage",
        {
            "status_resolved": lambda r: assert_eq(r["status"], "RESOLVED", "Status"),
        },
    )
)

r2 = asyncio.get_event_loop().run_until_complete(
    run_stress_turn(
        session_saas,
        "Flow 5",
        "saas",
        2,
        "show revenue",
        {
            "status_resolved": lambda r: assert_eq(r["status"], "RESOLVED", "Status"),
        },
    )
)

r3 = asyncio.get_event_loop().run_until_complete(
    run_stress_turn(
        session_saas,
        "Flow 5",
        "saas",
        3,
        "now by plan",
        {
            "status_resolved": lambda r: assert_eq(r["status"], "RESOLVED", "Status"),
            "context_carryover": lambda r: assert_true(
                any(w.get("type") == "context_inheritance" for w in r["warnings"]),
                "Should inherit from context",
            ),
        },
    )
)

# ============================================================
# DATASET 3: FINANCE
# ============================================================

print("\n" + "=" * 80)
print("DATASET 3: FINANCE")
print("=" * 80)

print("\n[SETUP] Loading finance.csv...")
df_finance = pd.read_csv(r"D:\datasets for TalkingBI\finance.csv")
print(f"  Rows: {len(df_finance)}, Columns: {list(df_finance.columns)}")

session_finance = create_session(
    df_finance, MockDataset("finance.csv", list(df_finance.columns), df_finance.shape)
)
print(f"  Session: {session_finance}")

# Flow 6: Transactions
print("\n" + "-" * 80)
print("FLOW 6: Transactions")
print("-" * 80)

r1 = asyncio.get_event_loop().run_until_complete(
    run_stress_turn(
        session_finance,
        "Flow 6",
        "finance",
        1,
        "show total amount",
        {
            "status_resolved": lambda r: assert_eq(r["status"], "RESOLVED", "Status"),
            "handles_amount": lambda r: assert_true(
                r["charts"] > 0, "Should handle amount"
            ),
        },
    )
)

r2 = asyncio.get_event_loop().run_until_complete(
    run_stress_turn(
        session_finance,
        "Flow 6",
        "finance",
        2,
        "by category",
        {
            "status_resolved": lambda r: assert_eq(r["status"], "RESOLVED", "Status"),
        },
    )
)

r3 = asyncio.get_event_loop().run_until_complete(
    run_stress_turn(
        session_finance,
        "Flow 6",
        "finance",
        3,
        "filter salary",
        {
            "status_resolved": lambda r: assert_eq(r["status"], "RESOLVED", "Status"),
            "filter_applied": lambda r: assert_true(
                r["intent_resolved"]
                and "salary" in str(r["intent_resolved"].get("filter", "")).lower(),
                "Filter should be salary",
            ),
        },
    )
)

r4 = asyncio.get_event_loop().run_until_complete(
    run_stress_turn(
        session_finance,
        "Flow 6",
        "finance",
        4,
        "compare with expenses",
        {
            "status_resolved": lambda r: assert_eq(r["status"], "RESOLVED", "Status"),
            "compare_logic": lambda r: assert_true(
                r["intent_resolved"]
                and r["intent_resolved"].get("kpi_1")
                and r["intent_resolved"].get("kpi_2"),
                "COMPARE needs both KPIs",
            ),
        },
    )
)

# Flow 7: Noise + Filters
print("\n" + "-" * 80)
print("FLOW 7: Noise + Filters")
print("-" * 80)

conv_mgr.clear_session(session_finance)

r1 = asyncio.get_event_loop().run_until_complete(
    run_stress_turn(
        session_finance,
        "Flow 7",
        "finance",
        1,
        "expenses",
        {
            "status_resolved": lambda r: assert_eq(r["status"], "RESOLVED", "Status"),
        },
    )
)

r2 = asyncio.get_event_loop().run_until_complete(
    run_stress_turn(
        session_finance,
        "Flow 7",
        "finance",
        2,
        "food",
        {
            "status_resolved_or_incomplete": lambda r: assert_true(
                r["status"] in ["RESOLVED", "INCOMPLETE"], "Should handle single word"
            ),
        },
    )
)

r3 = asyncio.get_event_loop().run_until_complete(
    run_stress_turn(
        session_finance,
        "Flow 7",
        "finance",
        3,
        "by merchant",
        {
            "status_resolved": lambda r: assert_eq(r["status"], "RESOLVED", "Status"),
            "dimension_merchant": lambda r: assert_true(
                r["intent_resolved"]
                and r["intent_resolved"].get("dimension") == "merchant",
                "Dimension should be merchant",
            ),
        },
    )
)

# Cleanup
print("\n" + "=" * 80)
print("[CLEANUP] Removing test sessions...")
conv_mgr.clear_session(session_ecom)
conv_mgr.clear_session(session_saas)
conv_mgr.clear_session(session_finance)
try:
    delete_session(session_ecom)
    delete_session(session_saas)
    delete_session(session_finance)
except:
    pass
print("  Sessions cleaned up")

# ============================================================
# FINAL REPORT
# ============================================================

print("\n" + "=" * 80)
print("FINAL STRESS TEST REPORT")
print("=" * 80)

print(f"\n📊 SUMMARY")
print(f"  Total Tests: {passed + failed}")
print(f"  Passed: {passed}")
print(f"  Failed: {failed}")
print(f"  Success Rate: {passed / (passed + failed) * 100:.1f}%")

print(f"\n🔴 CRITICAL FAILURES: {len(critical_failures)}")
if critical_failures:
    for f in critical_failures:
        print(f"  - [{f['flow']}] T{f['turn']}: {f['check']}")
        print(f"    Query: '{f['query']}'")
        print(f"    Error: {f['error']}")
else:
    print("  None - System handled all test cases")

print(f"\n⚠️ SYSTEM WEAKNESSES:")
if failed > 0:
    print(f"  - {failed} validation checks failed")
    print(f"  - Some edge cases may need hardening")
else:
    print("  None identified")

print(f"\n💡 RECOMMENDATIONS:")
if len(critical_failures) > 0:
    print("  1. Fix critical failures before production")
    print("  2. Add more robust null handling")
    print("  3. Strengthen context validation")
elif failed > 0:
    print("  1. Monitor edge cases in production")
    print("  2. Consider stricter validation rules")
else:
    print("  1. System is production-ready")
    print("  2. Continue monitoring real-world usage")
    print("  3. Consider adding more datasets for diversity")

final_report = {
    "passed": passed,
    "failed": failed,
    "critical_failures": critical_failures,
    "system_weaknesses": system_weaknesses,
    "recommendations": [
        "Fix critical issues"
        if len(critical_failures) > 0
        else "System production-ready",
        "Add monitoring" if failed > 0 else "Continue monitoring",
        "Expand test coverage",
    ],
}

print("\n" + "=" * 80)
print(json.dumps(final_report, indent=2))
print("=" * 80)

if len(critical_failures) == 0 and failed == 0:
    print("\n✅ STRESS TEST PASSED - SYSTEM PRODUCTION READY")
elif len(critical_failures) == 0:
    print("\n⚠️ STRESS TEST PARTIAL - Minor issues, review recommended")
else:
    print("\n❌ STRESS TEST FAILED - Critical issues must be fixed")
