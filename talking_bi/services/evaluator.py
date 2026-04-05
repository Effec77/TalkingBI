"""
Phase 8: Evaluation & Guardrails

Pure observability layer — records query outcomes, classifies failures,
computes metrics, supports regression comparison.

Rules:
  - NO pipeline logic changes
  - NO LLM usage
  - NO side effects on query execution
  - Read-only observer pattern
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional


# ─────────────────────────────────────────────────────────────
# Failure types
# ─────────────────────────────────────────────────────────────

class FailureType:
    NONE               = None
    AMBIGUOUS_QUERY    = "AMBIGUOUS_QUERY"
    CONTEXT_MISSING    = "CONTEXT_MISSING"
    INVALID_INTENT     = "INVALID_INTENT"
    SEMANTIC_REJECTION = "SEMANTIC_REJECTION"
    EXECUTION_ERROR    = "EXECUTION_ERROR"
    UNKNOWN_STATUS     = "UNKNOWN_STATUS"


# ─────────────────────────────────────────────────────────────
# Record struct
# ─────────────────────────────────────────────────────────────

@dataclass
class EvalRecord:
    query:               str
    dataset:             str
    status:              str
    intent:              Optional[Dict[str, Any]]
    execution_mode:      Optional[str]
    semantic_applied:    bool
    latency_ms:          float
    failure_type:        Optional[str]
    failure_reason:      Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ─────────────────────────────────────────────────────────────
# Failure classifier
# ─────────────────────────────────────────────────────────────

def classify_failure(result: Dict[str, Any]) -> Optional[str]:
    """
    Classify why a query failed based on the pipeline result.

    Maps:
        UNKNOWN    → AMBIGUOUS_QUERY
        INCOMPLETE → CONTEXT_MISSING
        INVALID    → INVALID_INTENT
        semantic attempted but low-confidence → SEMANTIC_REJECTION
        errors present → EXECUTION_ERROR
        RESOLVED   → None (success)
    """
    status = result.get("status", "")

    if status == "UNKNOWN":
        return FailureType.AMBIGUOUS_QUERY

    if status == "INCOMPLETE":
        return FailureType.CONTEXT_MISSING

    if status == "INVALID":
        return FailureType.INVALID_INTENT

    # Semantic attempted but not applied (rejection)
    semantic_meta = result.get("semantic_meta", {})
    if (
        semantic_meta
        and not semantic_meta.get("applied", False)
        and semantic_meta.get("reason") == "low_confidence"
    ):
        return FailureType.SEMANTIC_REJECTION

    # Execution crash (RESOLVED but errors list is non-empty)
    if status == "RESOLVED" and result.get("errors"):
        return FailureType.EXECUTION_ERROR

    # Unrecognised non-success status
    if status not in ("RESOLVED", None, ""):
        return FailureType.UNKNOWN_STATUS

    return FailureType.NONE


# ─────────────────────────────────────────────────────────────
# Main evaluator
# ─────────────────────────────────────────────────────────────

class Evaluator:
    """
    Phase 8 evaluation recorder.

    Usage:
        evaluator = Evaluator()

        t0 = time.time()
        result = run_query(...)
        evaluator.record(
            query="show revenue",
            dataset="ecommerce.csv",
            result=result,
            latency_ms=(time.time() - t0) * 1000,
        )

        print(evaluator.compute_metrics())
        evaluator.save("evaluation.json")
    """

    def __init__(self):
        self.records: List[EvalRecord] = []

    # ── Record ────────────────────────────────────────────────

    def record(
        self,
        query: str,
        dataset: str,
        result: Dict[str, Any],
        latency_ms: float,
    ) -> EvalRecord:
        """
        Record a single query execution.

        Args:
            query:      Raw user query string
            dataset:    Dataset name / filename
            result:     Full API response dict from query endpoint
            latency_ms: Wall-clock latency in milliseconds

        Returns:
            EvalRecord (also stored internally)
        """
        status = result.get("status", "")
        failure = classify_failure(result)

        # Semantic meta can be at top-level (from intent) or nested
        semantic_meta = result.get("semantic_meta") or {}
        semantic_applied = bool(semantic_meta.get("applied", False))

        # execution_mode comes from plan_6d if present
        plan_6d = result.get("plan_6d") or {}
        execution_mode = plan_6d.get("mode") or result.get("execution_mode")

        # Extract failure reason from trace if present
        trace = result.get("trace", {})
        failure_reason = trace.get("failure_reason")

        rec = EvalRecord(
            query=query,
            dataset=dataset,
            status=status,
            intent=result.get("intent_resolved") or result.get("intent"),
            execution_mode=execution_mode,
            semantic_applied=semantic_applied,
            latency_ms=round(latency_ms, 2),
            failure_type=failure,
            failure_reason=failure_reason,
        )

        self.records.append(rec)

        # Logging
        print(f'[8] Recorded: query="{query}", status={status}')
        if failure:
            print(f"[8] Failure classified: {failure}")

        return rec

    # ── Metrics ───────────────────────────────────────────────

    def compute_metrics(self) -> Dict[str, Any]:
        """
        Compute aggregate metrics across all recorded queries.

        Returns:
            {
                "total": int,
                "success_rate": float (0–1),
                "failure_breakdown": {failure_type: count},
                "avg_latency_ms": float,
                "p95_latency_ms": float,
                "semantic_usage_rate": float (0–1),
                "partial_execution_rate": float (0–1),
                "status_breakdown": {status: count},
            }
        """
        total = len(self.records)
        if total == 0:
            return {
                "total": 0,
                "success_rate": 0.0,
                "failure_breakdown": {},
                "avg_latency_ms": 0.0,
                "p95_latency_ms": 0.0,
                "semantic_usage_rate": 0.0,
                "partial_execution_rate": 0.0,
                "status_breakdown": {},
            }

        successes = sum(1 for r in self.records if r.status == "RESOLVED")
        success_rate = round(successes / total, 4)

        # Failure breakdown
        failure_breakdown: Dict[str, int] = {}
        for r in self.records:
            ft = r.failure_type or "NONE"
            failure_breakdown[ft] = failure_breakdown.get(ft, 0) + 1

        # Latency
        latencies = sorted(r.latency_ms for r in self.records)
        avg_latency = round(sum(latencies) / total, 2)
        p95_idx = max(0, int(total * 0.95) - 1)
        p95_latency = round(latencies[p95_idx], 2)

        # Semantic usage
        semantic_used = sum(1 for r in self.records if r.semantic_applied)
        semantic_usage_rate = round(semantic_used / total, 4)

        # Partial execution rate (among RESOLVED only)
        resolved_records = [r for r in self.records if r.status == "RESOLVED"]
        partial_count = sum(
            1 for r in resolved_records
            if r.execution_mode == "PARTIAL_RUN"
        )
        partial_execution_rate = (
            round(partial_count / len(resolved_records), 4)
            if resolved_records else 0.0
        )

        # Status breakdown
        status_breakdown: Dict[str, int] = {}
        for r in self.records:
            status_breakdown[r.status] = status_breakdown.get(r.status, 0) + 1

        return {
            "total": total,
            "success_rate": success_rate,
            "failure_breakdown": failure_breakdown,
            "avg_latency_ms": avg_latency,
            "p95_latency_ms": p95_latency,
            "semantic_usage_rate": semantic_usage_rate,
            "partial_execution_rate": partial_execution_rate,
            "status_breakdown": status_breakdown,
        }

    # ── Save ──────────────────────────────────────────────────

    def save(self, filename: str = "evaluation.json") -> str:
        """
        Save records and metrics to JSON file.

        Args:
            filename: Output path (relative or absolute)

        Returns:
            Absolute path of written file
        """
        import os
        payload = {
            "metrics": self.compute_metrics(),
            "records": [r.to_dict() for r in self.records],
        }
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, default=str)

        abs_path = os.path.abspath(filename)
        print(f"[8] Saved evaluation report → {abs_path} ({len(self.records)} records)")
        return abs_path

    # ── Regression comparison ─────────────────────────────────

    def compare_runs(
        self,
        previous_file: str,
    ) -> Dict[str, Any]:
        """
        Compare current run metrics against a previous saved evaluation.

        Args:
            previous_file: Path to previous evaluation.json

        Returns:
            {
                "delta_success_rate": float,     # current - previous
                "delta_avg_latency_ms": float,   # current - previous
                "new_failures": [...],           # queries that now fail
                "resolved_failures": [...],      # queries that used to fail but now pass
            }
        """
        with open(previous_file, "r", encoding="utf-8") as f:
            prev_data = json.load(f)

        prev_metrics = prev_data.get("metrics", {})
        prev_records = prev_data.get("records", [])

        curr_metrics = self.compute_metrics()

        # Build lookup: query → status for each run
        prev_map = {r["query"]: r["status"] for r in prev_records}
        curr_map = {r.query: r.status for r in self.records}

        new_failures = []
        resolved_failures = []

        all_queries = set(prev_map) | set(curr_map)
        for q in all_queries:
            prev_status = prev_map.get(q)
            curr_status = curr_map.get(q)

            was_ok = prev_status == "RESOLVED"
            is_ok  = curr_status == "RESOLVED"

            if was_ok and not is_ok:
                new_failures.append({
                    "query": q,
                    "previous_status": prev_status,
                    "current_status": curr_status,
                })
            elif not was_ok and is_ok:
                resolved_failures.append({
                    "query": q,
                    "previous_status": prev_status,
                    "current_status": curr_status,
                })

        delta_success = round(
            curr_metrics["success_rate"] - prev_metrics.get("success_rate", 0.0), 4
        )
        delta_latency = round(
            curr_metrics["avg_latency_ms"] - prev_metrics.get("avg_latency_ms", 0.0), 2
        )

        comparison = {
            "delta_success_rate": delta_success,
            "delta_avg_latency_ms": delta_latency,
            "new_failures": new_failures,
            "resolved_failures": resolved_failures,
            "current_metrics": curr_metrics,
            "previous_metrics": prev_metrics,
        }

        # Log summary
        sign = "+" if delta_success >= 0 else ""
        print(f"[8] Regression check: success_rate {sign}{delta_success:+.2%}")
        if new_failures:
            print(f"[8] ⚠ New failures: {len(new_failures)}")
            for nf in new_failures:
                print(f'[8]   REGRESSED: "{nf["query"]}" '
                      f'({nf["previous_status"]} → {nf["current_status"]})')
        if resolved_failures:
            print(f"[8] ✓ Resolved: {len(resolved_failures)}")
            for rf in resolved_failures:
                print(f'[8]   FIXED: "{rf["query"]}" '
                      f'({rf["previous_status"]} → {rf["current_status"]})')

        return comparison


# ─────────────────────────────────────────────────────────────
# Context manager for timed recording
# ─────────────────────────────────────────────────────────────

class timed_record:
    """
    Context manager that measures latency and records result.

    Usage:
        with timed_record(evaluator, "show revenue", "sales.csv") as ctx:
            result = run_query(...)
            ctx.result = result
    """

    def __init__(self, evaluator: Evaluator, query: str, dataset: str):
        self.evaluator = evaluator
        self.query = query
        self.dataset = dataset
        self.result: Optional[Dict[str, Any]] = None
        self._t0: float = 0.0

    def __enter__(self):
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        latency_ms = (time.perf_counter() - self._t0) * 1000
        if exc_type is not None:
            # Pipeline crashed — record as execution error
            self.evaluator.record(
                query=self.query,
                dataset=self.dataset,
                result={"status": "RESOLVED", "errors": [str(exc_val)]},
                latency_ms=latency_ms,
            )
        elif self.result is not None:
            self.evaluator.record(
                query=self.query,
                dataset=self.dataset,
                result=self.result,
                latency_ms=latency_ms,
            )
        return False   # never suppress exceptions


# ─────────────────────────────────────────────────────────────
# Singleton
# ─────────────────────────────────────────────────────────────

_global_evaluator: Optional[Evaluator] = None


def get_evaluator() -> Evaluator:
    """Get or create the global Evaluator instance."""
    global _global_evaluator
    if _global_evaluator is None:
        _global_evaluator = Evaluator()
    return _global_evaluator


def reset_evaluator() -> Evaluator:
    """Reset global evaluator (useful between test runs)."""
    global _global_evaluator
    _global_evaluator = Evaluator()
    return _global_evaluator
