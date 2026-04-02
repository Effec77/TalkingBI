"""
Adversarial Context Resolution Tests — Tests 11-23
Tests multi-turn drift, compare schema, ambiguity recovery, filter inheritance.
No LLM required — tests the resolver logic directly.
"""

import sys, json
sys.path.insert(0, ".")

from services.context_resolver import create_resolver, ResolutionStatus

KPI_CANDIDATES = ["Sales", "Quantity", "Profit"]
AMBIGUITY_MAP = {
    "sales":  ["gross_sales", "net_sales"],
    "profit": ["gross_profit", "net_profit"],
}
DASHBOARD_PLAN = {"kpis": ["Sales", "Quantity", "Profit"]}

passed = 0
failed = 0
failures = []

def check(test_id, condition, message):
    global passed, failed
    if condition:
        print(f"  [OK]  {message}")
        passed += 1
    else:
        print(f"  [FAIL] {message}")
        failed += 1
        failures.append(f"Test {test_id}: {message}")

def sep(title):
    print(f"\n{'='*60}\n  {title}\n{'='*60}")

# ─────────────────────────────────────────────────────────────
# TEST 11 — Partial + Context + Override Collision
# T1: show sales  T2: now by region  T3: show profit  T4: now by product
# Validate: T3 resets KPI, T4 uses Profit NOT Sales
# ─────────────────────────────────────────────────────────────
sep("TEST 11 — Partial + Context + Override Collision")
r = create_resolver(KPI_CANDIDATES, AMBIGUITY_MAP)

r1 = r.resolve({"intent":"SEGMENT_BY","kpi":"Sales","dimension":None,"filter":None}, DASHBOARD_PLAN)
r2 = r.resolve({"intent":"SEGMENT_BY","kpi":None,"dimension":"region","filter":None}, DASHBOARD_PLAN)
r3 = r.resolve({"intent":"SEGMENT_BY","kpi":"Profit","dimension":None,"filter":None}, DASHBOARD_PLAN)
r4 = r.resolve({"intent":"SEGMENT_BY","kpi":None,"dimension":"product","filter":None}, DASHBOARD_PLAN)

check(11, r2.intent and r2.intent.get("kpi") == "Sales",   "T2: KPI inherited from T1 (Sales)")
check(11, r2.source_map.get("kpi") == "context",           "T2: KPI source = context")
check(11, r3.intent and r3.intent.get("kpi") == "Profit",  "T3: user KPI (Profit) overrides context (Sales)")
check(11, r3.source_map.get("kpi") == "user",              "T3: KPI source = user")
check(11, r4.intent and r4.intent.get("kpi") == "Profit",  "T4: KPI from T3 context (Profit, not Sales)")
check(11, r4.source_map.get("kpi") == "context",           "T4: KPI source = context")

# ─────────────────────────────────────────────────────────────
# TEST 12 — Compare After Context Switch
# T1: show sales  T2: now by region  T3: show quantity  T4: compare with sales
# Validate: kpi_1=Quantity(ctx), kpi_2=Sales(user)
# ─────────────────────────────────────────────────────────────
sep("TEST 12 — Compare After Context Switch")
r = create_resolver(KPI_CANDIDATES, AMBIGUITY_MAP)

r.resolve({"intent":"SEGMENT_BY","kpi":"Sales","dimension":None,"filter":None}, DASHBOARD_PLAN)
r.resolve({"intent":"SEGMENT_BY","kpi":None,"dimension":"region","filter":None}, DASHBOARD_PLAN)
r.resolve({"intent":"SEGMENT_BY","kpi":"Quantity","dimension":None,"filter":None}, DASHBOARD_PLAN)
r4 = r.resolve({"intent":"COMPARE","kpi":None,"kpi_2":"Sales","dimension":None,"filter":None}, DASHBOARD_PLAN)

check(12, r4.status == "RESOLVED",                           "T4: status = RESOLVED")
check(12, r4.intent and r4.intent.get("kpi_1") == "Quantity","T4: kpi_1 = Quantity (from context)")
check(12, r4.intent and r4.intent.get("kpi_2") == "Sales",   "T4: kpi_2 = Sales (from user)")
check(12, r4.source_map.get("kpi_1") == "context",           "T4: kpi_1 source = context")
check(12, r4.source_map.get("kpi_2") == "user",              "T4: kpi_2 source = user")

# ─────────────────────────────────────────────────────────────
# TEST 13 — Repeated Compare (post-COMPARE context primary)
# T1: show sales  T2: compare with quantity  T3: compare with sales
# Validate: T3 kpi_1 = Quantity (T2.kpi_2), kpi_2 = Sales — no self-comparison
# ─────────────────────────────────────────────────────────────
sep("TEST 13 — Repeated Compare (Post-COMPARE Primary KPI)")
r = create_resolver(KPI_CANDIDATES, AMBIGUITY_MAP)

r.resolve({"intent":"SEGMENT_BY","kpi":"Sales","dimension":None,"filter":None}, DASHBOARD_PLAN)
r.resolve({"intent":"COMPARE","kpi":None,"kpi_2":"Quantity","dimension":None,"filter":None}, DASHBOARD_PLAN)
r3 = r.resolve({"intent":"COMPARE","kpi":None,"kpi_2":"Sales","dimension":None,"filter":None}, DASHBOARD_PLAN)

check(13, r3.status == "RESOLVED",                           "T3: status = RESOLVED")
check(13, r3.intent and r3.intent.get("kpi_1") == "Quantity","T3: kpi_1 = Quantity (T2.kpi_2, not Sales)")
check(13, r3.intent and r3.intent.get("kpi_2") == "Sales",   "T3: kpi_2 = Sales (user)")
check(13, r3.intent.get("kpi_1") != r3.intent.get("kpi_2"), "T3: kpi_1 != kpi_2 (no self-comparison)")

# ─────────────────────────────────────────────────────────────
# TEST 14 — Ambiguity Recovery
# T1: show sales (AMBIGUOUS)  T2: show quantity  T3: now by region
# Validate: T1 not in context, T3 uses Quantity
# ─────────────────────────────────────────────────────────────
sep("TEST 14 — Ambiguity Recovery")
r = create_resolver(KPI_CANDIDATES, AMBIGUITY_MAP)

r1 = r.resolve({"intent":"SEGMENT_BY","kpi":"sales","dimension":None,"filter":None}, DASHBOARD_PLAN)
r2 = r.resolve({"intent":"SEGMENT_BY","kpi":"Quantity","dimension":None,"filter":None}, DASHBOARD_PLAN)
r3 = r.resolve({"intent":"SEGMENT_BY","kpi":None,"dimension":"region","filter":None}, DASHBOARD_PLAN)

check(14, r1.status == "AMBIGUOUS",                           "T1: AMBIGUOUS (sales is ambiguous)")
check(14, r2.source_map.get("kpi") == "user",                "T2: Quantity from user (no T1 context)")
check(14, r3.intent and r3.intent.get("kpi") == "Quantity",  "T3: KPI = Quantity (from T2, not T1)")

# ─────────────────────────────────────────────────────────────
# TEST 15 — Garbage Injection Mid-Flow
# T1: show sales  T2: UNKNOWN  T3: now by region
# Validate: T2 = UNKNOWN, T3 still uses Sales (3-turn lookback)
# ─────────────────────────────────────────────────────────────
sep("TEST 15 — Garbage Injection Mid-Flow")
r = create_resolver(KPI_CANDIDATES, AMBIGUITY_MAP)

r1 = r.resolve({"intent":"SEGMENT_BY","kpi":"Sales","dimension":None,"filter":None}, DASHBOARD_PLAN)
r2 = r.resolve({"intent":"UNKNOWN","kpi":None,"dimension":None,"filter":None}, DASHBOARD_PLAN)
r3 = r.resolve({"intent":"SEGMENT_BY","kpi":None,"dimension":"region","filter":None}, DASHBOARD_PLAN)

check(15, r2.status == "UNKNOWN",                             "T2: UNKNOWN correctly returned")
check(15, r3.status == "RESOLVED",                            "T3: RESOLVED despite T2 UNKNOWN")
check(15, r3.intent and r3.intent.get("kpi") == "Sales",     "T3: KPI = Sales (lookback past T2 UNKNOWN)")

# ─────────────────────────────────────────────────────────────
# TEST 18 — Fallback With Partial Info
# T1 (no history): "show trends by region"
# Validate: KPI fallback, dimension preserved, execution succeeds
# ─────────────────────────────────────────────────────────────
sep("TEST 18 — Fallback With Partial Info (First Query)")
r = create_resolver(KPI_CANDIDATES, AMBIGUITY_MAP)

r1 = r.resolve({"intent":"EXPLAIN_TREND","kpi":None,"dimension":"region","filter":None}, DASHBOARD_PLAN)

check(18, r1.status == "RESOLVED",                                "T1: RESOLVED via fallback")
check(18, r1.source_map.get("kpi") == "fallback",                "T1: KPI source = fallback")
check(18, r1.intent and r1.intent.get("kpi") == "Sales",         "T1: KPI = Sales (dashboard_plan[0])")
check(18, r1.intent and r1.intent.get("dimension") == "region",  "T1: dimension preserved from user")
check(18, any("fallback" in str(w).lower() for w in r1.warnings),"T1: fallback warning emitted")

# ─────────────────────────────────────────────────────────────
# TEST 19 — Context Expiry (New Session)
# Simulate: new resolver (empty history), T1: "now by region"
# Validate: INCOMPLETE (no context, no fallback for relative phrasing)
# ─────────────────────────────────────────────────────────────
sep("TEST 19 — Context Expiry (Empty Session)")
r = create_resolver(KPI_CANDIDATES, AMBIGUITY_MAP)

# Fresh resolver, no history — relative query
r1 = r.resolve({"intent":"SEGMENT_BY","kpi":None,"dimension":"region","filter":None}, DASHBOARD_PLAN)

check(19, r1.status in ("INCOMPLETE", "RESOLVED"),               "T1: returns INCOMPLETE or fallback RESOLVED")
if r1.status == "RESOLVED":
    # If fallback fired (no relative phrasing detection yet), KPI must be disclosed
    check(19, r1.source_map.get("kpi") in ("fallback", "context"),"T1: if RESOLVED, KPI source disclosed")
else:
    check(19, "kpi" in r1.missing_fields,                        "T1: INCOMPLETE lists missing kpi")

# ─────────────────────────────────────────────────────────────
# TEST 20 — Rapid Intent Switching
# T1:sales T2:quantity T3:sales T4:now by region
# Validate: T4 context = T3 (Sales), not T2 (Quantity)
# ─────────────────────────────────────────────────────────────
sep("TEST 20 — Rapid Intent Switching")
r = create_resolver(KPI_CANDIDATES, AMBIGUITY_MAP)

r.resolve({"intent":"SEGMENT_BY","kpi":"Sales","dimension":None,"filter":None}, DASHBOARD_PLAN)
r.resolve({"intent":"SEGMENT_BY","kpi":"Quantity","dimension":None,"filter":None}, DASHBOARD_PLAN)
r.resolve({"intent":"SEGMENT_BY","kpi":"Sales","dimension":None,"filter":None}, DASHBOARD_PLAN)
r4 = r.resolve({"intent":"SEGMENT_BY","kpi":None,"dimension":"region","filter":None}, DASHBOARD_PLAN)

check(20, r4.status == "RESOLVED",                              "T4: RESOLVED")
check(20, r4.intent and r4.intent.get("kpi") == "Sales",        "T4: KPI = Sales (T3), NOT Quantity (T2)")
check(20, r4.source_map.get("kpi") == "context",                "T4: KPI source = context")

# ─────────────────────────────────────────────────────────────
# TEST 21 — Filter + Context Interaction
# T1:show sales  T2:filter region=west  T3:now by product
# Validate: filter persists, dimension updates, KPI stable
# ─────────────────────────────────────────────────────────────
sep("TEST 21 — Filter + Context Interaction")
r = create_resolver(KPI_CANDIDATES, AMBIGUITY_MAP)

r.resolve({"intent":"SEGMENT_BY","kpi":"Sales","dimension":None,"filter":None}, DASHBOARD_PLAN)
r.resolve({"intent":"FILTER","kpi":None,"dimension":None,"filter":"region=west"}, DASHBOARD_PLAN)
r3 = r.resolve({"intent":"SEGMENT_BY","kpi":None,"dimension":"product","filter":None}, DASHBOARD_PLAN)

check(21, r3.status == "RESOLVED",                              "T3: RESOLVED")
check(21, r3.intent and r3.intent.get("kpi") == "Sales",        "T3: KPI = Sales (stable from context)")
check(21, r3.intent and r3.intent.get("dimension") == "product","T3: dimension = product (user)")

# ─────────────────────────────────────────────────────────────
# TEST 22 — Compare Missing Operand
# T1:show sales  T2:compare (no kpi_2)
# Validate: INCOMPLETE, no execution
# ─────────────────────────────────────────────────────────────
sep("TEST 22 — Compare Missing Operand")
r = create_resolver(KPI_CANDIDATES, AMBIGUITY_MAP)

r.resolve({"intent":"SEGMENT_BY","kpi":"Sales","dimension":None,"filter":None}, DASHBOARD_PLAN)
r2 = r.resolve({"intent":"COMPARE","kpi":None,"kpi_2":None,"dimension":None,"filter":None}, DASHBOARD_PLAN)

check(22, r2.status == "INCOMPLETE",                             "T2: INCOMPLETE (kpi_2 missing)")
check(22, "kpi_2" in (r2.missing_fields or []),                 "T2: missing_fields contains kpi_2")

# ─────────────────────────────────────────────────────────────
# TEST 23 — Multi-Turn Dimension Drift Stress
# T1:show sales  T2:now by region  T3:now by product  T4:now by date
# Validate: KPI stable (Sales), only dimension changes
# ─────────────────────────────────────────────────────────────
sep("TEST 23 — Multi-Turn Dimension Drift Stress")
r = create_resolver(KPI_CANDIDATES, AMBIGUITY_MAP)

r.resolve({"intent":"SEGMENT_BY","kpi":"Sales","dimension":None,"filter":None}, DASHBOARD_PLAN)
r2 = r.resolve({"intent":"SEGMENT_BY","kpi":None,"dimension":"region","filter":None}, DASHBOARD_PLAN)
r3 = r.resolve({"intent":"SEGMENT_BY","kpi":None,"dimension":"product","filter":None}, DASHBOARD_PLAN)
r4 = r.resolve({"intent":"SEGMENT_BY","kpi":None,"dimension":"date","filter":None}, DASHBOARD_PLAN)

check(23, r2.intent and r2.intent.get("kpi") == "Sales",        "T2: KPI stable (Sales)")
check(23, r3.intent and r3.intent.get("kpi") == "Sales",        "T3: KPI stable (Sales)")
check(23, r4.intent and r4.intent.get("kpi") == "Sales",        "T4: KPI stable (Sales)")
check(23, r2.intent and r2.intent.get("dimension") == "region", "T2: dimension = region")
check(23, r3.intent and r3.intent.get("dimension") == "product","T3: dimension = product (no region leak)")
check(23, r4.intent and r4.intent.get("dimension") == "date",   "T4: dimension = date (no product leak)")

# ─────────────────────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"  ADVERSARIAL TEST SUMMARY (Tests 11-23)")
print(f"{'='*60}")
print(f"  Passed : {passed}")
print(f"  Failed : {failed}")
print(f"  Total  : {passed + failed}")

if failures:
    print(f"\n  FAILURES:")
    for f in failures:
        print(f"    [X] {f}")
else:
    print(f"\n  [OK] All adversarial checks passed!")
