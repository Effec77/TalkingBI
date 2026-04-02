"""
TalkingBI — Master Test Runner
Runs all test suites in sequence and prints a consolidated report.
No LLM-dependent tests included (those run separately due to latency).
"""

import subprocess
import sys
import time

python = sys.executable

SUITES = [
    {
        "name": "Phase 4 — Analytical Intelligence",
        "file": "tests/run_phase4.py",
        "needs_server": True,
    },
    {
        "name": "Phase 5 — Visualization + UI Output",
        "file": "tests/verify_phase5.py",
        "needs_server": True,
    },
    {
        "name": "Phase 6C — Context Resolution (Unit)",
        "file": "tests/test_phase6c_context_resolution.py",
        "needs_server": False,
    },
    {
        "name": "Phase 6C — Adversarial Multi-Turn Tests (11–23)",
        "file": "tests/test_adversarial_6c.py",
        "needs_server": False,
    },
]

WIDTH = 70

def bar(char="═"):
    return char * WIDTH

def run_suite(suite):
    print(f"\n{bar()}")
    print(f"  RUNNING: {suite['name']}")
    print(f"  File   : {suite['file']}")
    print(bar())

    start = time.time()
    result = subprocess.run(
        [python, suite["file"]],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    elapsed = time.time() - start

    output = result.stdout + result.stderr

    # Print output
    for line in output.splitlines():
        print(f"  {line}")

    status = "PASS" if result.returncode == 0 else "FAIL"
    print(f"\n  ── Result: {status}  ({elapsed:.1f}s)  exit={result.returncode} ──")
    return status, elapsed, result.returncode

print(f"\n{bar('═')}")
print(f"  TALKINGBI — FULL TEST SUITE RUN")
print(f"  Phases Covered : 4 → 5 → 6B → 6C")
print(f"  Suites         : {len(SUITES)}")
print(f"  Started        : {time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{bar('═')}")

results = []
for suite in SUITES:
    status, elapsed, code = run_suite(suite)
    results.append({
        "name": suite["name"],
        "status": status,
        "elapsed": elapsed,
        "exit_code": code,
    })

# ─── CONSOLIDATED SUMMARY ─────────────────────────────────────
print(f"\n{bar('═')}")
print(f"  FINAL SUMMARY")
print(f"{bar('═')}")
print(f"  {'Suite':<46} {'Status':>8}  {'Time':>6}")
print(f"  {bar('-')}")
for r in results:
    icon = "✅" if r["status"] == "PASS" else "❌"
    print(f"  {icon}  {r['name']:<44} {r['status']:>6}  {r['elapsed']:>5.1f}s")

total   = len(results)
passed  = sum(1 for r in results if r["status"] == "PASS")
failed  = total - passed

print(f"\n  {bar('-')}")
print(f"  Passed : {passed}/{total}")
print(f"  Failed : {failed}/{total}")
print(f"  {'ALL SUITES PASSED ✅' if failed == 0 else f'{failed} SUITE(S) FAILED ❌'}")
print(f"{bar('═')}\n")

sys.exit(0 if failed == 0 else 1)
