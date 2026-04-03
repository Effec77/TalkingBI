"""
Phase 8 — Full Production-Grade Evaluation
==========================================

Stress-tests the TalkingBI pipeline (6E → 6G → 6B → 7 → 6F → 6C → 6D)
against 6 real-world datasets × 6 test flows.

No API server required. No LLM called (intent parsing is mocked via
deterministic fixtures — this isolates the resolution, mapping, planning,
and semantic layers from LLM non-determinism for repeatable evaluation).

Datasets:
  hr / supply_chain / marketing / banking / saas / manufacturing

Flows per dataset:
  1. Basic Exploration
  2. Semantic Queries
  3. Noisy Human Input
  4. Edge Case Behaviour
  5. Binary KPI / Flags
  6. Garbage + Recovery
"""

from __future__ import annotations

import sys
import json
import time
import os

sys.path.insert(0, ".")

import pandas as pd
from typing import Any, Dict, List, Optional, Tuple

from services.schema_mapper      import create_schema_mapper
from services.deterministic_override import DeterministicIntentDetector
from services.semantic_interpreter   import create_semantic_interpreter
from services.context_resolver       import create_resolver, ResolutionStatus
from services.execution_planner      import ExecutionPlanner, ExecutionStateStore
from services.evaluator              import Evaluator, classify_failure, FailureType
from graph.adaptive_executor         import (
    _apply_filter,
    _build_prepared_data,
    _lookup_kpi_spec,
)

# ── Dataset paths ──────────────────────────────────────────────────────
DATASET_BASE = "D:/datasets for TalkingBI"

DATASETS: Dict[str, Dict] = {
    "hr": {
        "file": f"{DATASET_BASE}/hr.csv",
        "primary_kpi":   "salary",
        "secondary_kpi": "performance_score",
        "binary_kpi":    "attrition_flag",    # binary flag
        "dimension":     "department",
        "filter_val":    "Engineering",
        "kpi_display":   "Salary",
    },
    "supply_chain": {
        "file": f"{DATASET_BASE}/supply_chain.csv",
        "primary_kpi":   "delivery_time",
        "secondary_kpi": "inventory_level",
        "binary_kpi":    "delay_flag",
        "dimension":     "supplier",
        "filter_val":    "Supplier_A",
        "kpi_display":   "Delivery Time",
    },
    "marketing": {
        "file": f"{DATASET_BASE}/marketing.csv",
        "primary_kpi":   "revenue",
        "secondary_kpi": "clicks",
        "binary_kpi":    None,
        "dimension":     "channel",
        "filter_val":    "Email",
        "kpi_display":   "Revenue",
    },
    "banking": {
        "file": f"{DATASET_BASE}/banking.csv",
        "primary_kpi":   "amount",
        "secondary_kpi": "fraud_flag",
        "binary_kpi":    "fraud_flag",
        "dimension":     "transaction_type",
        "filter_val":    "purchase",
        "kpi_display":   "Amount",
    },
    "saas": {
        "file": f"{DATASET_BASE}/saas.csv",
        "primary_kpi":   "mrr",
        "secondary_kpi": "feature_usage",
        "binary_kpi":    "churn_flag",
        "dimension":     "subscription_plan",
        "filter_val":    "pro",
        "kpi_display":   "Mrr",
    },
    "manufacturing": {
        "file": f"{DATASET_BASE}/manufacturing.csv",
        "primary_kpi":   "output_units",
        "secondary_kpi": "defect_count",
        "binary_kpi":    None,
        "dimension":     "shift",
        "filter_val":    "morning",
        "kpi_display":   "Output Units",
    },
}

# ── Shared intent template ─────────────────────────────────────────────

def _intent(intent_type, kpi=None, kpi_1=None, kpi_2=None,
            dimension=None, filter_val=None) -> Dict:
    return {
        "intent": intent_type,
        "kpi": kpi,
        "kpi_1": kpi_1,
        "kpi_2": kpi_2,
        "dimension": dimension,
        "filter": filter_val,
    }


# ── Pipeline runner (offline, no API server) ──────────────────────────

class OfflinePipeline:
    """
    Runs the DETERMINISTIC layers of the pipeline:
      Schema Mapper → Semantic Interpreter → Context Resolver → Execution Planner
      → Adaptive Executor (partial path only — no LangGraph, no LLM)

    Intent is passed in directly (no 6B LLM call, no 6E normalizer needed).
    """

    def __init__(self, df: pd.DataFrame, dataset_name: str):
        self.df = df
        self.dataset_name = dataset_name

        # Build KPI candidates from numeric columns
        self.kpi_candidates = self._build_kpi_candidates(df)
        self.schema_mapper = create_schema_mapper(df, self.kpi_candidates)
        self.semantic_interp = create_semantic_interpreter(df, self.schema_mapper)
        self.resolver = create_resolver(
            kpi_candidates=[k["name"] for k in self.kpi_candidates],
            ambiguity_map={},
        )
        self.planner = ExecutionPlanner()
        self.store   = ExecutionStateStore()
        self.session = f"eval-{dataset_name}"

        # conversation history for context resolver
        self._context_turns: List[Dict] = []

    def _build_kpi_candidates(self, df: pd.DataFrame) -> List[Dict]:
        candidates = []
        for col in df.select_dtypes(include=["int64","float64","int32","float32"]).columns:
            candidates.append({
                "name": col.replace("_", " ").title(),
                "source_column": col,
                "aggregation": "sum",
                "segment_by": None,
                "time_column": None,
            })
        return candidates

    def run(self, query: str, raw_intent: Dict) -> Dict:
        """
        Run a single query through 6F → 7 → 6C → 6D → execute.
        Returns result dict compatible with Evaluator.record().
        """
        t0 = time.perf_counter()

        try:
            # ── 6F: Schema mapping ──────────────────────────────────
            intent = self.schema_mapper.map_intent(raw_intent)

            # ── 7: Semantic interpretation (only if kpi is None) ──
            intent = self.semantic_interp.interpret(query, intent)

            # ── 6C: Context resolution ──────────────────────────────
            # Replay context
            for turn in self._context_turns[-3:]:
                if turn.get("kpi") and not turn.get("errors"):
                    self.resolver.add_to_context(turn["intent_resolved"] or {})

            dashboard_dict = {"kpis": [k["name"] for k in self.kpi_candidates]}
            resolution = self.resolver.resolve(intent, dashboard_dict)

            latency_ms = (time.perf_counter() - t0) * 1000

            # ── Gate: non-RESOLVED early return ─────────────────────
            if resolution.status != ResolutionStatus.RESOLVED.value:
                result = {
                    "status": resolution.status,
                    "query": query,
                    "intent_raw": raw_intent,
                    "intent_resolved": resolution.intent,
                    "errors": [],
                    "semantic_meta": intent.get("semantic_meta", {}),
                    "plan_6d": None,
                    "prepared_data": [],
                    "charts_generated": 0,
                    "insights_generated": 0,
                    "dataset": self.dataset_name,
                    "latency_ms": round(latency_ms, 2),
                }
                return result

            resolved_intent = resolution.intent

            # ── 6D: Execution planning ───────────────────────────────
            prev_state = self.store.get(self.session)
            plan = self.planner.plan(
                curr_intent=resolved_intent,
                prev_state=prev_state,
            )

            # ── Partial execution (no LangGraph) ─────────────────────
            exec_start = time.perf_counter()
            prepared_data, exec_errors, base_df, filtered_df = self._execute(
                plan, resolved_intent, prev_state
            )
            exec_latency = (time.perf_counter() - exec_start) * 1000
            latency_ms = (time.perf_counter() - t0) * 1000

            # ── Save state (only on success) ──────────────────────────
            if not exec_errors:
                self.store.save(
                    self.session,
                    base_df=base_df,
                    filtered_df=filtered_df,
                    last_result=prepared_data,
                    last_intent=resolved_intent,
                )

            # ── Record in context for next turn ───────────────────────
            self._context_turns.append({
                "intent_resolved": resolved_intent,
                "kpi": resolved_intent.get("kpi"),
                "errors": exec_errors,
            })

            result = {
                "status": "RESOLVED",
                "query": query,
                "intent_raw": raw_intent,
                "intent_resolved": resolved_intent,
                "errors": exec_errors,
                "semantic_meta": intent.get("semantic_meta", {}),
                "plan_6d": {
                    "mode": plan.mode,
                    "reuse": plan.reuse,
                    "operations": plan.operations,
                    "reason": plan.reason,
                },
                "prepared_data": prepared_data,
                "charts_generated": len(prepared_data),
                "insights_generated": len(prepared_data),
                "dataset": self.dataset_name,
                "latency_ms": round(latency_ms, 2),
            }
            return result

        except Exception as exc:
            latency_ms = (time.perf_counter() - t0) * 1000
            return {
                "status": "RESOLVED",
                "query": query,
                "intent_resolved": None,
                "errors": [str(exc)],
                "semantic_meta": {},
                "plan_6d": None,
                "prepared_data": [],
                "charts_generated": 0,
                "insights_generated": 0,
                "dataset": self.dataset_name,
                "latency_ms": round(latency_ms, 2),
            }

    def _execute(
        self,
        plan,
        resolved_intent: Dict,
        prev_state,
    ) -> Tuple[List, List, pd.DataFrame, pd.DataFrame]:
        """Execute pandas ops based on plan. Returns (prepared_data, errors, base_df, filtered_df)."""
        errors = []
        base_df = self.df.copy()

        # Determine working df
        if plan.reuse == "last_result" and prev_state and prev_state.last_result:
            return prev_state.last_result, [], prev_state.base_df, prev_state.filtered_df

        if plan.reuse == "base_df" or plan.mode == "FULL_RUN":
            filtered_df = _apply_filter(base_df, resolved_intent)
        elif plan.reuse == "filtered_df" and prev_state and prev_state.filtered_df is not None:
            filtered_df = prev_state.filtered_df.copy()
        else:
            filtered_df = _apply_filter(base_df, resolved_intent)

        # Resolve KPI
        kpi_name = resolved_intent.get("kpi")
        kpi_spec = _lookup_kpi_spec(kpi_name, {"kpis": self.kpi_candidates})

        if kpi_spec is None and self.kpi_candidates:
            kpi_spec = self.kpi_candidates[0]   # fallback to first KPI

        dimension = resolved_intent.get("dimension")

        # COMPARE
        intent_type = resolved_intent.get("intent", "")
        if intent_type == "COMPARE":
            kpi_1 = _lookup_kpi_spec(resolved_intent.get("kpi_1"), {"kpis": self.kpi_candidates})
            kpi_2 = _lookup_kpi_spec(resolved_intent.get("kpi_2"), {"kpis": self.kpi_candidates})
            specs = [s for s in [kpi_1, kpi_2] if s]
            if not specs and kpi_spec:
                specs = [kpi_spec]
            try:
                prepared_data = _build_prepared_data(specs, filtered_df, dimension)
            except Exception as e:
                errors.append(str(e))
                prepared_data = []
        else:
            specs = [kpi_spec] if kpi_spec else []
            try:
                prepared_data = _build_prepared_data(specs, filtered_df, dimension)
            except Exception as e:
                errors.append(str(e))
                prepared_data = []

        return prepared_data, errors, base_df, filtered_df

    def reset_context(self, preserve_kpi: bool = True):
        """Reset between flows, optionally carrying the last resolved KPI forward."""
        last_kpi = None
        if preserve_kpi and self._context_turns:
            last_kpi = self._context_turns[-1].get("kpi")

        self._context_turns = []
        self.store.invalidate(self.session)
        self.resolver = create_resolver(
            kpi_candidates=[k["name"] for k in self.kpi_candidates],
            ambiguity_map={},
        )

        # Carry last KPI forward so the first turn of the next flow has context
        if last_kpi:
            self._context_turns.append({
                "intent_resolved": {
                    "intent": "SEGMENT_BY",
                    "kpi": last_kpi,
                    "kpi_1": None,
                    "kpi_2": None,
                    "dimension": None,
                    "filter": None,
                },
                "kpi": last_kpi,
                "errors": [],
            })


# ── Test flows ─────────────────────────────────────────────────────────

def build_flows(cfg: Dict) -> List[Tuple[str, str, Dict]]:
    """
    Returns list of (flow_name, query_label, intent) tuples for one dataset.
    """
    pk   = cfg["primary_kpi"]
    sk   = cfg["secondary_kpi"]
    bk   = cfg.get("binary_kpi")
    dim  = cfg["dimension"]
    fval = cfg["filter_val"]
    pkd  = cfg["kpi_display"]

    flows = []

    # ── Flow 1: Basic Exploration ──────────────────────────────────────
    flows += [
        ("flow1_basic", f"show {pk}",
         _intent("SEGMENT_BY", kpi=pk)),
        ("flow1_basic", f"by {dim}",
         _intent("SEGMENT_BY", kpi=None, dimension=dim)),
        ("flow1_basic", f"compare {pk} with {sk}",
         _intent("COMPARE", kpi_1=pk, kpi_2=sk)),
    ]

    # ── Flow 2: Semantic Queries ────────────────────────────────────────
    flows += [
        ("flow2_semantic", "show performance",
         _intent("SEGMENT_BY", kpi=None)),       # kpi=None → semantic layer
        ("flow2_semantic", "show growth",
         _intent("SEGMENT_BY", kpi=None)),
        ("flow2_semantic", f"by {dim}",
         _intent("SEGMENT_BY", kpi=None, dimension=dim)),
    ]

    # ── Flow 3: Noisy Human Input ───────────────────────────────────────
    flows += [
        ("flow3_noisy", f"{pk} numbers",
         _intent("SEGMENT_BY", kpi=pk)),
        ("flow3_noisy", f"{dim} wise",
         _intent("SEGMENT_BY", kpi=None, dimension=dim)),
        ("flow3_noisy", f"filter {fval}",
         _intent("FILTER", filter_val=fval)),
    ]

    # ── Flow 4: Edge Cases ──────────────────────────────────────────────
    flows += [
        ("flow4_edge", f"show {pk}",
         _intent("SEGMENT_BY", kpi=pk)),
        ("flow4_edge", "filter null",
         _intent("FILTER", filter_val="null")),
        ("flow4_edge", f"by {dim}",
         _intent("SEGMENT_BY", kpi=None, dimension=dim)),
    ]

    # ── Flow 5: Binary KPI / Flags ──────────────────────────────────────
    binary = bk if bk else pk
    flows += [
        ("flow5_binary", f"show {binary}",
         _intent("SEGMENT_BY", kpi=binary)),
        ("flow5_binary", f"by {dim}",
         _intent("SEGMENT_BY", kpi=None, dimension=dim)),
        ("flow5_binary", f"compare {binary} with {pk}",
         _intent("COMPARE", kpi_1=binary, kpi_2=pk)),
    ]

    # ── Flow 6: Garbage + Recovery ──────────────────────────────────────
    flows += [
        ("flow6_garbage", f"show {pk}",
         _intent("SEGMENT_BY", kpi=pk)),
        ("flow6_garbage", "asdfgh",
         _intent("UNKNOWN")),           # garbage → UNKNOWN → no execution
        ("flow6_garbage", f"show {pk}",
         _intent("SEGMENT_BY", kpi=pk)),
    ]

    return flows


# ── Critical issue detection ────────────────────────────────────────────

# These planner reasons are architecturally correct — suppress as false alarms
_EXPECTED_FULL_RUN_REASONS = {
    "no_prior_state",
    "no_prior_intent",
    "first_turn",
    "reuse_base_df_for_filter",   # now a PARTIAL_RUN, kept here as fallback
}

def detect_critical(result: Dict, flow: str, label: str) -> Optional[str]:
    errors = result.get("errors", [])
    if errors:
        return f"[CRITICAL] Execution crash in {flow} '{label}': {errors[0][:80]}"

    plan = result.get("plan_6d") or {}
    reason = plan.get("reason") or ""

    # Suppress WARNs for correct planner behaviors
    if reason in _EXPECTED_FULL_RUN_REASONS:
        return None

    # SEGMENT_BY -> FILTER switch is now PARTIAL_RUN — no longer a WARN
    if "reuse_base_df_for_filter" in reason:
        return None

    if (
        flow in ("flow2_semantic", "flow3_noisy")
        and plan.get("mode") == "FULL_RUN"
        and result.get("status") == "RESOLVED"
    ):
        return (f"[WARN] Unexpected FULL_RUN "
                f"in {flow} '{label}' (reason={reason})")

    return None


# ── Main evaluation run ─────────────────────────────────────────────────

def run_evaluation() -> Dict:
    global_evaluator = Evaluator()
    dataset_results: Dict[str, Any] = {}
    all_critical: List[str] = []
    all_observations: List[str] = []

    print("\n" + "="*70)
    print("  PHASE 8 — FULL PRODUCTION EVALUATION")
    print("="*70)

    for ds_name, cfg in DATASETS.items():
        print(f"\n{'─'*70}")
        print(f"  DATASET: {ds_name.upper()}")
        print(f"{'─'*70}")

        # Load dataset
        df = pd.read_csv(cfg["file"])
        print(f"  Loaded {len(df)} rows × {len(df.columns)} cols")

        # Task 3: Auto-detect binary KPI from dataset at runtime
        cfg = cfg.copy()  # don't mutate global config
        if cfg.get("binary_kpi") is None:
            for col in df.select_dtypes(include=["int64", "float64"]).columns:
                vals = set(df[col].dropna().unique())
                if vals.issubset({0, 1, 0.0, 1.0}):
                    cfg["binary_kpi"] = col
                    print(f"  Auto-detected binary KPI: {col}")
                    break
            if cfg.get("binary_kpi") is None:
                cfg["binary_kpi"] = cfg["primary_kpi"]  # graceful fallback

        pipe = OfflinePipeline(df, ds_name)
        ds_evaluator = Evaluator()
        flows = build_flows(cfg)
        flow_groups: Dict[str, List] = {}

        current_flow = None

        for flow_name, query_label, raw_intent in flows:
            # Reset pipeline context between flows
            if flow_name != current_flow:
                pipe.reset_context()
                current_flow = flow_name
                print(f"\n  [{flow_name}]")

            t0 = time.perf_counter()
            result = pipe.run(query_label, raw_intent)
            latency = (time.perf_counter() - t0) * 1000
            # Use the pipeline's own latency if available
            latency = result.get("latency_ms", latency)

            # Record
            global_evaluator.record(query_label, ds_name, result, latency)
            ds_evaluator.record(query_label, ds_name, result, latency)

            status = result["status"]
            plan_mode = (result.get("plan_6d") or {}).get("mode", "N/A")
            sem = result.get("semantic_meta", {})
            sem_str = f"→ {sem.get('mapped_to')}" if sem.get("applied") else ""
            print(f"    {status:10s} | {plan_mode:12s} | {latency:6.1f}ms | "
                  f"{query_label[:40]}{sem_str}")

            # Collect critical issues
            issue = detect_critical(result, flow_name, query_label)
            if issue:
                all_critical.append(f"[{ds_name}] {issue}")
                print(f"    ⚠  {issue}")

            flow_groups.setdefault(flow_name, []).append(result)

        ds_metrics = ds_evaluator.compute_metrics()
        dataset_results[ds_name] = {
            "metrics": ds_metrics,
            "rows": len(df),
            "columns": len(df.columns),
            "flow_summary": {
                flow: {
                    "total": len(results),
                    "resolved": sum(1 for r in results if r["status"] == "RESOLVED"),
                }
                for flow, results in flow_groups.items()
            },
        }
        print(f"\n  ▶ {ds_name}: success={ds_metrics['success_rate']:.0%} | "
              f"avg_latency={ds_metrics['avg_latency_ms']:.1f}ms")

    # ── Global metrics ──────────────────────────────────────────────────
    gm = global_evaluator.compute_metrics()

    # ── Derive weaknesses ───────────────────────────────────────────────
    weaknesses: List[str] = []

    if gm["success_rate"] < 0.75:
        weaknesses.append(
            f"Low overall success rate ({gm['success_rate']:.0%}) — "
            "context resolution or schema mapping needs hardening"
        )

    if gm["semantic_usage_rate"] == 0.0:
        weaknesses.append(
            "Semantic layer (Phase 7) never activated — "
            "vague terms not triggering mapping"
        )

    if gm["partial_execution_rate"] == 0.0 and gm["total"] > 6:
        weaknesses.append(
            "Execution planner always uses FULL_RUN — "
            "6D state not persisting across turns"
        )

    fb = gm["failure_breakdown"]
    if fb.get("CONTEXT_MISSING", 0) > 3:
        weaknesses.append(
            f"High CONTEXT_MISSING failures ({fb['CONTEXT_MISSING']}) — "
            "context resolver not carrying intent across turns"
        )
    if fb.get("AMBIGUOUS_QUERY", 0) > 3:
        weaknesses.append(
            f"High AMBIGUOUS_QUERY failures ({fb['AMBIGUOUS_QUERY']}) — "
            "UNKNOWN intents leaking through (expected for garbage flow)"
        )
    if fb.get("EXECUTION_ERROR", 0) > 0:
        weaknesses.append(
            f"{fb['EXECUTION_ERROR']} EXECUTION_ERROR(s) — "
            "pipeline crashing on real data (see critical_issues)"
        )

    for ds, dr in dataset_results.items():
        ds_sr = dr["metrics"]["success_rate"]
        if ds_sr < 0.5:
            weaknesses.append(
                f"[{ds}] Very low success rate ({ds_sr:.0%}) — "
                "schema mapping likely failing for this domain"
            )

    # ── Observations ────────────────────────────────────────────────────
    all_observations = [
        f"Evaluated {gm['total']} queries across {len(DATASETS)} datasets",
        f"Overall success rate: {gm['success_rate']:.1%}",
        f"Semantic layer hit rate: {gm['semantic_usage_rate']:.1%}",
        f"Partial execution rate (of resolved): {gm['partial_execution_rate']:.1%}",
        f"Average latency: {gm['avg_latency_ms']:.1f}ms",
        f"p95 latency: {gm['p95_latency_ms']:.1f}ms",
    ]
    for ds, dr in dataset_results.items():
        sr = dr["metrics"]["success_rate"]
        all_observations.append(
            f"[{ds}] success={sr:.0%}, "
            f"avg_latency={dr['metrics']['avg_latency_ms']:.1f}ms"
        )

    report = {
        "metrics": {
            "total_queries": gm["total"],
            "success_rate": gm["success_rate"],
            "avg_latency_ms": gm["avg_latency_ms"],
            "p95_latency_ms": gm["p95_latency_ms"],
            "semantic_usage_rate": gm["semantic_usage_rate"],
            "partial_execution_rate": gm["partial_execution_rate"],
        },
        "failure_breakdown": gm["failure_breakdown"],
        "status_breakdown": gm["status_breakdown"],
        "dataset_wise_performance": dataset_results,
        "critical_issues": all_critical,
        "system_weaknesses": weaknesses,
        "observations": all_observations,
    }

    return report, global_evaluator


# ── Entry point ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    report, evaluator = run_evaluation()

    # Save JSON
    out_path = "tests/eval_report_phase8.json"
    evaluator.save(out_path.replace(".json", "_raw.json"))

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)

    # ── Print final report ──────────────────────────────────────────────
    m = report["metrics"]
    print("\n" + "="*70)
    print("  PHASE 8: FULL EVALUATION REPORT")
    print("="*70)
    print(f"  Total queries          : {m['total_queries']}")
    print(f"  Success rate           : {m['success_rate']:.1%}")
    print(f"  Avg latency            : {m['avg_latency_ms']:.2f}ms")
    print(f"  p95 latency            : {m['p95_latency_ms']:.2f}ms")
    print(f"  Semantic usage rate    : {m['semantic_usage_rate']:.1%}")
    print(f"  Partial execution rate : {m['partial_execution_rate']:.1%}")

    print("\n  Failure breakdown:")
    for ft, cnt in report["failure_breakdown"].items():
        print(f"    {ft:30s}: {cnt}")

    print("\n  Dataset performance:")
    for ds, dr in report["dataset_wise_performance"].items():
        sr = dr["metrics"]["success_rate"]
        lat = dr["metrics"]["avg_latency_ms"]
        bar = "✔" if sr >= 0.75 else ("~" if sr >= 0.5 else "✗")
        print(f"    {bar} {ds:20s}: {sr:.0%} success | {lat:.1f}ms avg")

    if report["critical_issues"]:
        print(f"\n  ⚠ Critical issues ({len(report['critical_issues'])}):")
        for ci in report["critical_issues"]:
            print(f"    {ci}")
    else:
        print("\n  ✔ No critical issues detected")

    if report["system_weaknesses"]:
        print(f"\n  System weaknesses ({len(report['system_weaknesses'])}):")
        for w in report["system_weaknesses"]:
            print(f"    → {w}")
    else:
        print("\n  ✔ No systemic weaknesses detected")

    print(f"\n  Reports saved:")
    print(f"    {os.path.abspath(out_path)}")
    print(f"    {os.path.abspath(out_path.replace('.json','_raw.json'))}")
    print("="*70)
