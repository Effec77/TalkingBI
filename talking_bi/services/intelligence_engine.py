"""
Intelligence Engine - Phase 0B Orchestrator (PATCHED)
Multi-Provider LLM with Python-First KPI Selection
"""

import pandas as pd
from typing import Dict
from models.contracts import UploadedDataset
from models.dashboard import DashboardPlan
from services.dataset_profiler import profile_dataset
from services.kpi_selector import select_kpis_python
from services.llm_manager import LLMManager
from services.kpi_enrichment import enrich_kpis
from services.dashboard_planner import create_dashboard_plan


def generate_dashboard_plan(
    session_id: str, df: pd.DataFrame, uploaded_dataset: UploadedDataset
) -> DashboardPlan:
    """
    Main orchestrator for Phase 0B - Dataset Intelligence (PATCHED).

    NEW Architecture:
    1. Python-First KPI Selection (ALWAYS returns 3 KPIs)
    2. Multi-Provider LLM Enrichment (optional)
    3. Dashboard Planning (LLM optional)

    Args:
        session_id: Session identifier
        df: DataFrame from session
        uploaded_dataset: Metadata from Phase 0A

    Returns:
        DashboardPlan ready for Phase 1
    """
    print(f"\n{'=' * 70}")
    print(f"  PHASE 0B: Dataset Intelligence (PATCHED)")
    print(f"  Session: {session_id}")
    print(f"  Multi-Provider LLM Orchestration")
    print(f"{'=' * 70}\n")

    # Step 1: Profile Dataset
    print("[0B.1] Dataset Profiler")
    profile = profile_dataset(df)

    # Step 1.5: Generate ALL KPI candidates (for intent validation)
    print("\n[0B.1.5] Generating KPI Candidates")
    from services.kpi_generator import generate_kpi_candidates

    kpi_candidates_raw = generate_kpi_candidates(df, profile)

    # Convert KPICandidate objects to dicts for storage
    kpi_candidates = [
        {
            "name": c.column.replace("_", " ").title(),
            "source_column": c.column,
            "aggregation": c.aggregations[0] if c.aggregations else "sum",
            "cardinality": c.cardinality,
            "missing_pct": c.missing_pct,
            "segment_by_options": c.segment_by_options,
            "time_column_options": c.time_column_options,
        }
        for c in kpi_candidates_raw
    ]
    print(f"[0B.1.5] Generated {len(kpi_candidates)} KPI candidates")

    # Step 2: Python-First KPI Selection (PRIMARY)
    print("\n[0B.2] Python-First KPI Selection (PRIMARY)")
    kpi_columns = select_kpis_python(df)

    print(f"[0B.2] OK Selected numeric KPI candidates: {kpi_columns}")

    # Step 3: Initialize Multi-Provider LLM Manager
    print("\n[0B.3] Initializing Multi-Provider LLM Manager")
    llm_manager = LLMManager()

    # Step 4: LLM Enrichment (OPTIONAL)
    print("\n[0B.4] KPI Enrichment (LLM Optional)")
    dataset_context = {
        "filename": uploaded_dataset.filename,
        "rows": uploaded_dataset.shape[0],
        "columns": uploaded_dataset.columns,
        "numeric_columns": profile.numeric_columns,
        "categorical_columns": profile.categorical_columns,
        "datetime_columns": profile.datetime_columns,
    }

    enriched_kpis = enrich_kpis(kpi_columns, dataset_context, llm_manager, df=df)

    # Step 5: Create Dashboard Plan
    print("\n[0B.5] Dashboard Planner")
    dashboard_plan = create_dashboard_plan(
        session_id=session_id,
        kpis=enriched_kpis,
        dataset_context=dataset_context,
        kpi_candidates=kpi_candidates,
    )

    print(f"\n{'=' * 70}")
    print(f"  PHASE 0B COMPLETE (PATCHED)")
    print(f"  KPIs: {len(dashboard_plan.kpis)}")
    print(f"  Charts: {len(dashboard_plan.charts)}")
    print(f"  Coverage: {dashboard_plan.kpi_coverage:.1%}")
    print(f"{'=' * 70}\n")

    return dashboard_plan
