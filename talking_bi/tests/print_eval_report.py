import json

with open("tests/eval_report_phase8.json") as f:
    r = json.load(f)

m = r["metrics"]
print("METRICS")
for k, v in m.items():
    print("  " + str(k) + ": " + str(v))

print("\nFAILURE BREAKDOWN")
for k, v in r["failure_breakdown"].items():
    print("  " + str(k) + ": " + str(v))

print("\nSTATUS BREAKDOWN")
for k, v in r["status_breakdown"].items():
    print("  " + str(k) + ": " + str(v))

print("\nDATASET PERFORMANCE")
for ds, dr in r["dataset_wise_performance"].items():
    dm = dr["metrics"]
    sr  = str(round(dm["success_rate"] * 100, 1)) + "%"
    lat = str(round(dm["avg_latency_ms"], 2)) + "ms"
    sem = str(round(dm["semantic_usage_rate"] * 100, 1)) + "%"
    par = str(round(dm["partial_execution_rate"] * 100, 1)) + "%"
    print("  " + ds + ": success=" + sr + "  avg=" + lat + "  semantic=" + sem + "  partial=" + par)

print("\nCRITICAL ISSUES")
for c in r["critical_issues"]:
    print("  " + c)
if not r["critical_issues"]:
    print("  none")

print("\nSYSTEM WEAKNESSES")
for w in r["system_weaknesses"]:
    print("  " + w)
if not r["system_weaknesses"]:
    print("  none")

print("\nOBSERVATIONS")
for o in r["observations"]:
    print("  " + o)
