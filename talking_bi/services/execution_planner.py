"""
Execution Planner — Phase 6D (Adaptive Execution)

Decides whether to reuse prior computation (PARTIAL_RUN) or trigger
a fresh execution (FULL_RUN), based on a deterministic diff of the
resolved intent.

Rules:
  1. No LLM — purely structural comparison of intent dicts
  2. Aligned to YOUR schema: intent / kpi / kpi_1 / kpi_2 / dimension / filter
  3. No dataset switching logic (single-dataset system)
  4. Safe fallback: if uncertain → FULL_RUN
  5. COMPARE intent has first-class handling (kpi_1 + kpi_2)
  6. Reuse passes through chart + insight layer always (no stale-result skip)

Hierarchy of diff sensitivity:
  intent_changed  →  FULL_RUN
  filter_changed  →  PARTIAL_RUN (reuse=base_df, steps=filter→groupby→aggregate)
  dimension_changed → PARTIAL_RUN (reuse=filtered_df, steps=groupby→aggregate)
  kpi_changed     →  PARTIAL_RUN (reuse=filtered_df, steps=aggregate)
  COMPARE         →  PARTIAL_RUN (reuse=filtered_df, steps=compute_kpi_1+compute_kpi_2)
  no change       →  PARTIAL_RUN (reuse=last_result, steps=render)
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd


# ─────────────────────────────────────────────────────────────
# 1. ExecutionState — lean, no redundant metadata
# ─────────────────────────────────────────────────────────────

@dataclass
class ExecutionState:
    """
    Stores checkpointed artifacts from the previous pipeline run.

    Only three levels of reuse are tracked — aligned to the reuse
    hierarchy (base_df → filtered_df → last_result).

    last_intent is stored so IntentDiff can compare without
    passing the previous intent separately through the API.
    """

    # Checkpointed DataFrames (never None after a successful run)
    base_df: Optional[pd.DataFrame] = None          # After load (Phase 2)
    filtered_df: Optional[pd.DataFrame] = None      # After filter (Phase 3)

    # Final pipeline output — prepared_data list from prep_node
    last_result: Optional[List[Dict[str, Any]]] = None

    # The resolved intent that produced this state
    last_intent: Optional[Dict[str, Any]] = None

    def is_valid(self) -> bool:
        """Returns True only when all three artifacts are populated."""
        return (
            self.base_df is not None
            and self.filtered_df is not None
            and self.last_result is not None
            and self.last_intent is not None
        )


# ─────────────────────────────────────────────────────────────
# 2. IntentDiff — aligned to YOUR schema
# ─────────────────────────────────────────────────────────────

def compute_intent_diff(
    prev: Dict[str, Any],
    curr: Dict[str, Any],
) -> Dict[str, bool]:
    """
    Structural diff of two resolved intents.

    Uses YOUR schema fields:
        intent / kpi / kpi_1 / kpi_2 / dimension / filter

    Returns a flat bool dict — each key is True when that field
    changed between the previous and current turn.
    """
    # Intent type is the coarsest discriminator — checked first.
    intent_changed = prev.get("intent") != curr.get("intent")

    # KPI change covers single KPI AND both compare operands.
    # Any one of the three changing triggers kpi_changed.
    kpi_changed = (
        prev.get("kpi") != curr.get("kpi")
        or prev.get("kpi_1") != curr.get("kpi_1")
        or prev.get("kpi_2") != curr.get("kpi_2")
    )

    dimension_changed = prev.get("dimension") != curr.get("dimension")

    # Filter is the most expensive change — it invalidates filtered_df.
    filter_changed = prev.get("filter") != curr.get("filter")

    return {
        "intent_changed": intent_changed,
        "kpi_changed": kpi_changed,
        "dimension_changed": dimension_changed,
        "filter_changed": filter_changed,
    }


# ─────────────────────────────────────────────────────────────
# 3. ExecutionPlan dataclass — output of the planner
# ─────────────────────────────────────────────────────────────

# Valid run modes
MODE_FULL = "FULL_RUN"
MODE_PARTIAL = "PARTIAL_RUN"

# Valid reuse levels (None = no reuse, full pipeline from scratch)
REUSE_BASE_DF = "base_df"
REUSE_FILTERED_DF = "filtered_df"
REUSE_LAST_RESULT = "last_result"

# Valid step identifiers — map directly to executor entry points
STEP_FILTER = "filter"
STEP_GROUPBY = "groupby"
STEP_AGGREGATE = "aggregate"
STEP_COMPUTE_KPI_1 = "compute_kpi_1"
STEP_COMPUTE_KPI_2 = "compute_kpi_2"
STEP_RENDER = "render"      # chart + insight layer — always runs


@dataclass
class ExecutionPlan:
    """
    Describes what the executor should do for this turn.

    mode        — FULL_RUN or PARTIAL_RUN
    reuse       — which cached artifact to start from (None for FULL_RUN)
    operations  — ordered list of steps the executor must run
    reason      — human-readable explanation (for logs / debug)
    """

    mode: str
    reuse: Optional[str]
    operations: List[str]
    reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "mode": self.mode,
            "reuse": self.reuse,
            "operations": self.operations,
            "reason": self.reason,
        }


# ─────────────────────────────────────────────────────────────
# Internal plan factories
# ─────────────────────────────────────────────────────────────

def _full_run(reason: str) -> ExecutionPlan:
    return ExecutionPlan(
        mode=MODE_FULL,
        reuse=None,
        operations=["load", STEP_FILTER, STEP_GROUPBY, STEP_AGGREGATE, STEP_RENDER],
        reason=reason,
    )


def _partial(reuse: str, steps: List[str], reason: str) -> ExecutionPlan:
    # render is ALWAYS appended — we never skip chart+insight layer
    if STEP_RENDER not in steps:
        steps = steps + [STEP_RENDER]
    return ExecutionPlan(
        mode=MODE_PARTIAL,
        reuse=reuse,
        operations=steps,
        reason=reason,
    )


# ─────────────────────────────────────────────────────────────
# 4. ExecutionPlanner — main planning logic
# ─────────────────────────────────────────────────────────────

class ExecutionPlanner:
    """
    Deterministic execution planner for Phase 6D.

    Usage:
        planner = ExecutionPlanner()
        plan = planner.plan(curr_intent, prev_state)
        # → ExecutionPlan
    """

    def plan(
        self,
        curr_intent: Dict[str, Any],
        prev_state: Optional[ExecutionState] = None,
    ) -> ExecutionPlan:
        """
        Derive the execution plan for this turn.

        Args:
            curr_intent:  Resolved intent dict from Phase 6C
            prev_state:   ExecutionState from previous successful run
                          (None on first turn or after session reset)

        Returns:
            ExecutionPlan — always deterministic, always safe.
        """
        # ── FALLBACK GATE: no previous state → must run full pipeline ──
        if prev_state is None or not prev_state.is_valid():
            return _full_run("no_prior_state")

        prev_intent = prev_state.last_intent
        if not prev_intent:
            return _full_run("no_prior_intent")

        # ── DIFF ──────────────────────────────────────────────────────
        diff = compute_intent_diff(prev_intent, curr_intent)

        # ── RULE 1: intent type changed ───────────────────────────────
        # Special case: SEGMENT_BY → FILTER can reuse base_df because
        # filters always apply on top of raw data — no reload needed.
        if diff["intent_changed"]:
            curr_type = curr_intent.get("intent", "")
            if (
                curr_type == "FILTER"
                and prev_state.base_df is not None
            ):
                return _partial(
                    reuse=REUSE_BASE_DF,
                    steps=[STEP_FILTER, STEP_GROUPBY, STEP_AGGREGATE],
                    reason="reuse_base_df_for_filter",
                )
            # All other intent type changes → full reset
            return _full_run(
                f"intent_changed: {prev_intent.get('intent')} → {curr_intent.get('intent')}"
            )

        # ── COMPARE: first-class handling ─────────────────────────────
        # COMPARE always needs both KPIs computed over filtered data.
        # Only safe to reuse filtered_df IF filter has not changed.
        if curr_intent.get("intent") == "COMPARE":
            if diff["filter_changed"]:
                # Filter changed — must re-filter first
                return _partial(
                    reuse=REUSE_BASE_DF,
                    steps=[STEP_FILTER, STEP_COMPUTE_KPI_1, STEP_COMPUTE_KPI_2],
                    reason="compare_with_filter_change",
                )
            return _partial(
                reuse=REUSE_FILTERED_DF,
                steps=[STEP_COMPUTE_KPI_1, STEP_COMPUTE_KPI_2],
                reason="compare_kpi_recompute",
            )

        # ── RULE 2: filter changed → re-filter from base ──────────────
        # A filter change invalidates filtered_df and everything downstream.
        # We must go back to base_df.
        if diff["filter_changed"]:
            return _partial(
                reuse=REUSE_BASE_DF,
                steps=[STEP_FILTER, STEP_GROUPBY, STEP_AGGREGATE],
                reason="filter_changed",
            )

        # ── RULE 3: dimension changed → regroup from filtered_df ──────
        # Filter is unchanged, so filtered_df is valid.
        # Only the groupby axis changes → re-group and re-aggregate.
        if diff["dimension_changed"]:
            return _partial(
                reuse=REUSE_FILTERED_DF,
                steps=[STEP_GROUPBY, STEP_AGGREGATE],
                reason="dimension_changed",
            )

        # ── RULE 4: KPI changed → re-aggregate from filtered_df ───────
        # Filter + dimension unchanged; only the metric changes.
        # We can skip groupby if the dimension is None (scalar KPI).
        # For safety: always include groupby — it is cheap.
        if diff["kpi_changed"]:
            return _partial(
                reuse=REUSE_FILTERED_DF,
                steps=[STEP_GROUPBY, STEP_AGGREGATE],
                reason="kpi_changed",
            )

        # ── RULE 5: nothing changed → pass last_result to render ──────
        # All structural fields are identical. Re-run chart + insight
        # layer over cached last_result. Never silently return stale data.
        return _partial(
            reuse=REUSE_LAST_RESULT,
            steps=[STEP_RENDER],
            reason="no_change_rerender",
        )


# ─────────────────────────────────────────────────────────────
# 5. ExecutionStateStore — per-session state cache
# ─────────────────────────────────────────────────────────────

class ExecutionStateStore:
    """
    In-memory store of ExecutionState objects, keyed by session_id.

    Thread safety: Not thread-safe by design.
    This is single-threaded per session — extend with locks if needed.
    """

    def __init__(self) -> None:
        self._store: Dict[str, ExecutionState] = {}

    def get(self, session_id: str) -> Optional[ExecutionState]:
        """Return the cached state for this session, or None."""
        return self._store.get(session_id)

    def save(
        self,
        session_id: str,
        *,
        base_df: pd.DataFrame,
        filtered_df: pd.DataFrame,
        last_result: List[Dict[str, Any]],
        last_intent: Dict[str, Any],
    ) -> None:
        """Persist execution artifacts after a successful pipeline run."""
        self._store[session_id] = ExecutionState(
            base_df=base_df,
            filtered_df=filtered_df,
            last_result=last_result,
            last_intent=last_intent,
        )
        print(
            f"[6D:StateStore] Saved state for session={session_id} "
            f"(base_df={base_df.shape}, cached_results={len(last_result)})"
        )

    def invalidate(self, session_id: str) -> None:
        """Forcibly clear state for a session (e.g., on dataset re-upload)."""
        if session_id in self._store:
            del self._store[session_id]
            print(f"[6D:StateStore] Invalidated state for session={session_id}")

    def has(self, session_id: str) -> bool:
        return session_id in self._store


# ─────────────────────────────────────────────────────────────
# Module-level singleton — shared across the API process
# ─────────────────────────────────────────────────────────────

_state_store = ExecutionStateStore()
_planner = ExecutionPlanner()


def get_planner() -> ExecutionPlanner:
    return _planner


def get_state_store() -> ExecutionStateStore:
    return _state_store
