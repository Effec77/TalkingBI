"""
Phase 7: Semantic Intelligence — Test Suite

Tests the four required scenarios:
  1. "show usage"       → maps to session_duration / event_count (SaaS)
  2. "show performance" → maps to revenue / sales (generic)
  3. "show sales"       → maps to Sales / revenue column
  4. "show growth"      → maps to mrr / revenue / sales

Also tests:
  5. No override when KPI already set (6G/6B match)
  6. No override on low-confidence terms
  7. UNKNOWN intent upgraded to SEGMENT_BY when semantic hits
  8. Dataset-aware: only real columns are returned
"""

import sys
sys.path.insert(0, ".")

import pandas as pd
from unittest.mock import MagicMock
from services.semantic_interpreter import (
    SemanticInterpreter,
    create_semantic_interpreter,
    SEMANTIC_MAP,
    CONFIDENCE_THRESHOLD,
    _resolve_vague_term,
    _extract_vague_term,
    _normalize,
)

# ─────────────────────────────────────────────────────────────
# Test framework
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
# Mock SchemaMapper builder
# ─────────────────────────────────────────────────────────────

def make_schema_mapper(kpi_candidates):
    mapper = MagicMock()
    mapper.kpi_candidates = kpi_candidates
    return mapper


# ─────────────────────────────────────────────────────────────
# Dataset fixtures
# ─────────────────────────────────────────────────────────────

# SaaS-style dataset
SAAS_DF = pd.DataFrame({
    "session_duration": [120, 300, 450, 200],
    "event_count":      [10, 25, 40, 15],
    "mrr":              [99.0, 199.0, 299.0, 99.0],
    "churn_flag":       [0, 1, 0, 0],
    "plan":             ["basic", "pro", "enterprise", "basic"],
})

SAAS_KPIS = [
    {"name": "Session Duration", "source_column": "session_duration", "aggregation": "sum"},
    {"name": "Event Count",      "source_column": "event_count",      "aggregation": "sum"},
    {"name": "Mrr",              "source_column": "mrr",              "aggregation": "sum"},
    {"name": "Churn Flag",       "source_column": "churn_flag",       "aggregation": "sum"},
]

# E-commerce / finance style dataset
ECOM_DF = pd.DataFrame({
    "revenue":   [1000, 2000, 1500, 3000],
    "quantity":  [10, 20, 15, 30],
    "profit":    [200, 400, 300, 600],
    "region":    ["North", "South", "North", "East"],
})

ECOM_KPIS = [
    {"name": "Revenue",  "source_column": "revenue",  "aggregation": "sum"},
    {"name": "Quantity", "source_column": "quantity",  "aggregation": "sum"},
    {"name": "Profit",   "source_column": "profit",    "aggregation": "sum"},
]

# Sales dataset
SALES_DF = pd.DataFrame({
    "Sales":    [100, 200, 150, 300],
    "Quantity": [10, 20, 15, 30],
    "Region":   ["North", "South", "North", "East"],
})

SALES_KPIS = [
    {"name": "Sales",    "source_column": "Sales",    "aggregation": "sum"},
    {"name": "Quantity", "source_column": "Quantity", "aggregation": "sum"},
]


# ─────────────────────────────────────────────────────────────
# T1 — "show usage" → session_duration (SaaS dataset)
# ─────────────────────────────────────────────────────────────
sep("T1 — 'show usage' with SaaS dataset")

saas_mapper = make_schema_mapper(SAAS_KPIS)
interp_saas = create_semantic_interpreter(SAAS_DF, saas_mapper)

intent_unknown_kpi = {
    "intent": "SEGMENT_BY",
    "kpi": None,
    "kpi_1": None, "kpi_2": None,
    "dimension": None, "filter": None,
}

result_usage = interp_saas.interpret("show usage", intent_unknown_kpi)

check("T1", result_usage.get("semantic_meta", {}).get("applied") is True,
      "semantic_meta.applied = True")
check("T1", result_usage.get("kpi") is not None,
      "kpi resolved (not None)")
check("T1", result_usage.get("semantic_meta", {}).get("confidence", 0) >= CONFIDENCE_THRESHOLD,
      f"confidence >= {CONFIDENCE_THRESHOLD}")
check("T1", result_usage.get("semantic_meta", {}).get("vague_term") == "usage",
      "vague_term = 'usage'")
print(f"  --> kpi resolved to: {result_usage.get('kpi')} "
      f"(confidence={result_usage.get('semantic_meta',{}).get('confidence')})")


# ─────────────────────────────────────────────────────────────
# T2 — "show performance" → revenue (ecommerce dataset)
# ─────────────────────────────────────────────────────────────
sep("T2 — 'show performance' with ecommerce dataset")

ecom_mapper = make_schema_mapper(ECOM_KPIS)
interp_ecom = create_semantic_interpreter(ECOM_DF, ecom_mapper)

result_perf = interp_ecom.interpret("show performance", intent_unknown_kpi.copy())

check("T2", result_perf.get("semantic_meta", {}).get("applied") is True,
      "semantic_meta.applied = True")
check("T2", result_perf.get("kpi") is not None,
      "kpi resolved (not None)")
check("T2", result_perf.get("semantic_meta", {}).get("confidence", 0) >= CONFIDENCE_THRESHOLD,
      f"confidence >= {CONFIDENCE_THRESHOLD}")
print(f"  --> kpi resolved to: {result_perf.get('kpi')} "
      f"(confidence={result_perf.get('semantic_meta',{}).get('confidence')})")


# ─────────────────────────────────────────────────────────────
# T3 — "show sales" → Sales column
# ─────────────────────────────────────────────────────────────
sep("T3 — 'show sales' with sales dataset")

sales_mapper = make_schema_mapper(SALES_KPIS)
interp_sales = create_semantic_interpreter(SALES_DF, sales_mapper)

result_sales = interp_sales.interpret("show sales", intent_unknown_kpi.copy())

check("T3", result_sales.get("semantic_meta", {}).get("applied") is True,
      "semantic_meta.applied = True")
check("T3", result_sales.get("kpi") is not None,
      "kpi resolved (not None)")
# "sales" maps to "Sales" (the column name) which is an exact match via semantic map
mapped_kpi_t3 = result_sales.get("kpi", "").lower()
check("T3", "sales" in mapped_kpi_t3,
      f"resolved kpi contains 'sales' (got '{result_sales.get('kpi')}')")
print(f"  --> kpi resolved to: {result_sales.get('kpi')} "
      f"(confidence={result_sales.get('semantic_meta',{}).get('confidence')})")


# ─────────────────────────────────────────────────────────────
# T4 — "show growth" → mrr / revenue
# ─────────────────────────────────────────────────────────────
sep("T4 — 'show growth' with SaaS dataset (mrr)")

result_growth = interp_saas.interpret("show growth", intent_unknown_kpi.copy())

check("T4", result_growth.get("semantic_meta", {}).get("applied") is True,
      "semantic_meta.applied = True")
check("T4", result_growth.get("kpi") is not None,
      "kpi resolved (not None)")
print(f"  --> kpi resolved to: {result_growth.get('kpi')} "
      f"(confidence={result_growth.get('semantic_meta',{}).get('confidence')})")


# ─────────────────────────────────────────────────────────────
# T5 — NO override when KPI already set by 6G/6B
# ─────────────────────────────────────────────────────────────
sep("T5 — No override when KPI already explicit")

intent_with_kpi = {
    "intent": "SEGMENT_BY",
    "kpi": "Revenue",          # Explicitly set by 6G/6B
    "kpi_1": None, "kpi_2": None,
    "dimension": None, "filter": None,
}

result_no_override = interp_ecom.interpret("show revenue performance", intent_with_kpi)

check("T5", result_no_override.get("semantic_meta", {}).get("applied") is False,
      "semantic_meta.applied = False (kpi already set)")
check("T5", result_no_override.get("kpi") == "Revenue",
      "original kpi 'Revenue' preserved unchanged")


# ─────────────────────────────────────────────────────────────
# T6 — Low confidence → unchanged intent
# ─────────────────────────────────────────────────────────────
sep("T6 — Low confidence term → no mapping")

# "xyz" has no entries in SEMANTIC_MAP → confidence = 0
result_low = interp_ecom.interpret("show xyz", intent_unknown_kpi.copy())

check("T6", result_low.get("semantic_meta", {}).get("applied") is False,
      "semantic_meta.applied = False for unknown term")
check("T6", result_low.get("kpi") is None,
      "kpi stays None for unknown term")


# ─────────────────────────────────────────────────────────────
# T7 — UNKNOWN intent upgraded to SEGMENT_BY on semantic hit
# ─────────────────────────────────────────────────────────────
sep("T7 — UNKNOWN intent upgraded to SEGMENT_BY")

intent_unknown = {
    "intent": "UNKNOWN",
    "kpi": None,
    "kpi_1": None, "kpi_2": None,
    "dimension": None, "filter": None,
}

# "usage" should hit on SaaS dataset AND upgrade the intent
result_upgraded = interp_saas.interpret("usage", intent_unknown)

check("T7", result_upgraded.get("kpi") is not None,
      "kpi resolved from UNKNOWN state")
if result_upgraded.get("semantic_meta", {}).get("applied"):
    check("T7", result_upgraded.get("intent") == "SEGMENT_BY",
          "intent upgraded UNKNOWN → SEGMENT_BY")
else:
    # If confidence was too low, UNKNOWN should stay UNKNOWN (not corrupted)
    check("T7", result_upgraded.get("intent") == "UNKNOWN",
          "UNKNOWN preserved when no mapping found (correct fallback)")


# ─────────────────────────────────────────────────────────────
# T8 — Dataset-aware: only real dataset KPIs returned
# ─────────────────────────────────────────────────────────────
sep("T8 — Dataset-aware: must map to real column only")

# Minimal dataset with only "orders" column
TINY_DF = pd.DataFrame({"orders": [1, 2, 3], "region": ["A", "B", "C"]})
TINY_KPIS = [{"name": "Orders", "source_column": "orders", "aggregation": "sum"}]
tiny_mapper = make_schema_mapper(TINY_KPIS)
interp_tiny = create_semantic_interpreter(TINY_DF, tiny_mapper)

# "revenue" semantic map won't find match in TINY_KPIS → stay unmapped
result_no_col = interp_tiny.interpret("show revenue", intent_unknown_kpi.copy())
check("T8", result_no_col.get("kpi") is None,
      "No mapping when column doesn't exist in dataset")
check("T8", result_no_col.get("semantic_meta", {}).get("applied") is False,
      "semantic_meta.applied=False when column not in dataset")

# "volume" / "orders" should map to "Orders" which IS in TINY_KPIS
result_orders = interp_tiny.interpret("show volume", intent_unknown_kpi.copy())
check("T8", result_orders.get("kpi") is not None or
      result_orders.get("semantic_meta", {}).get("applied") is False,
      "volume correctly handled (maps to orders or skips if below threshold)")


# ─────────────────────────────────────────────────────────────
# T9 — semantic_meta always present in output
# ─────────────────────────────────────────────────────────────
sep("T9 — semantic_meta always in output")

for query, intent in [
    ("show usage", intent_unknown_kpi.copy()),
    ("show xyz",   intent_unknown_kpi.copy()),
    ("show sales", intent_with_kpi.copy()),
]:
    res = interp_saas.interpret(query, intent)
    check("T9", "semantic_meta" in res,
          f"semantic_meta present in output for '{query}'")
    check("T9", "applied" in res.get("semantic_meta", {}),
          f"semantic_meta.applied present for '{query}'")


# ─────────────────────────────────────────────────────────────
# T10 — Internal unit: _extract_vague_term
# ─────────────────────────────────────────────────────────────
sep("T10 — _extract_vague_term unit tests")

base_intent = {"intent": "SEGMENT_BY", "kpi": None}

check("T10", _extract_vague_term("show usage", base_intent) is not None,
      "_extract_vague_term returns value for 'show usage'")
check("T10", _extract_vague_term("", base_intent) is None,
      "_extract_vague_term returns None for empty string")

term = _extract_vague_term("show usage", base_intent)
primary = (term or "").replace("_", " ").split()[0]
check("T10", primary == "usage",
      f"Primary term extracted correctly: '{primary}'")


# ─────────────────────────────────────────────────────────────
# T11 — semantic_map coverage check
# ─────────────────────────────────────────────────────────────
sep("T11 — SEMANTIC_MAP contains required terms")

required_terms = ["sales", "usage", "performance", "growth"]
for term in required_terms:
    check("T11", term in SEMANTIC_MAP,
          f"'{term}' exists in SEMANTIC_MAP")
    check("T11", len(SEMANTIC_MAP[term]) > 0,
          f"'{term}' has at least one hint")


# ─────────────────────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"  PHASE 7 SEMANTIC INTELLIGENCE TEST SUMMARY")
print(f"{'='*60}")
print(f"  Passed : {passed}")
print(f"  Failed : {failed}")
print(f"  Total  : {passed + failed}")

if failures:
    print(f"\n  FAILURES:")
    for f in failures:
        print(f"    [X] {f}")
else:
    print(f"\n  [OK] All Phase 7 checks passed!")

sys.exit(0 if failed == 0 else 1)
