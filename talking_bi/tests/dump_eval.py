import json

with open("tests/eval_report_phase8.json", encoding="utf-8") as f:
    r = json.load(f)

with open("tests/eval_detail_dump.txt", "w", encoding="utf-8") as out:
    for ds, dr in r["dataset_wise_performance"].items():
        dm = dr["metrics"]
        out.write("DATASET: " + ds + "\n")
        out.write("  rows=" + str(dr["rows"]) + "  cols=" + str(dr["columns"]) + "\n")
        out.write("  success_rate=" + str(round(dm["success_rate"]*100,1)) + "%\n")
        out.write("  avg_latency=" + str(round(dm["avg_latency_ms"],2)) + "ms\n")
        out.write("  p95_latency=" + str(round(dm["p95_latency_ms"],2)) + "ms\n")
        out.write("  semantic_usage=" + str(round(dm["semantic_usage_rate"]*100,1)) + "%\n")
        out.write("  partial_exec_rate=" + str(round(dm["partial_execution_rate"]*100,1)) + "%\n")
        out.write("  failure_breakdown:\n")
        for k, v in dm["failure_breakdown"].items():
            out.write("    " + str(k) + ": " + str(v) + "\n")
        out.write("  flow_summary:\n")
        for flow, fs in dr["flow_summary"].items():
            out.write("    " + flow + ": resolved=" + str(fs["resolved"]) + "/" + str(fs["total"]) + "\n")
        out.write("\n")

    out.write("CRITICAL_ISSUES:\n")
    for c in r["critical_issues"]:
        out.write("  " + c + "\n")
    if not r["critical_issues"]:
        out.write("  none\n")

    out.write("\nSYSTEM_WEAKNESSES:\n")
    for w in r["system_weaknesses"]:
        out.write("  " + w + "\n")
    if not r["system_weaknesses"]:
        out.write("  none\n")

print("done")
