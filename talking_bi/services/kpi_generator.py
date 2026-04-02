"""
KPI Candidate Generation - Phase 0B.2
Python ONLY - No LLM
Filters and validates potential KPI columns
"""

import pandas as pd
from typing import List, Dict
from dataclasses import dataclass
from services.dataset_profiler import DatasetProfile


@dataclass
class KPICandidate:
    """Candidate column for KPI"""

    column: str
    dtype: str
    cardinality: int
    missing_pct: float
    aggregations: List[str]
    segment_by_options: List[str]
    time_column_options: List[str]


def generate_kpi_candidates(
    df: pd.DataFrame, profile: DatasetProfile
) -> List[KPICandidate]:
    """
    Generate KPI candidates from dataset profile.

    Rules:
    - Numeric columns where:
      - nunique() > 5
      - missing_pct < 0.3

    Args:
        df: Input DataFrame
        profile: Dataset profile

    Returns:
        List of KPI candidates
    """
    print(f"[KPI_GEN] Generating KPI candidates")

    candidates = []

    # Separate binary columns (nunique == 2) - they ARE valid KPIs
    binary_columns = []
    non_binary_columns = []

    for col in profile.numeric_columns:
        col_profile = profile.column_profiles[col]

        # Check if binary (2 unique values)
        if col_profile.unique_values == 2 and col_profile.missing_pct < 0.3:
            binary_columns.append(col)
            print(f"[KPI_GEN] BINARY candidate: {col} (nunique=2, type=binary_kpi)")
        elif col_profile.unique_values > 5 and col_profile.missing_pct < 0.3:
            non_binary_columns.append(col)
        else:
            print(
                f"[KPI_GEN] Skipping {col}: nunique={col_profile.unique_values} (filtered)"
            )

    # Combine: binary columns are valid KPI candidates too
    valid_columns = non_binary_columns + binary_columns

    # Generate candidates for all valid columns
    for col in valid_columns:
        col_profile = profile.column_profiles[col]

        # Determine aggregations
        aggregations = ["sum", "avg", "count", "min", "max"]

        # Get segment_by options (categorical columns with reasonable cardinality)
        segment_by_options = [
            c
            for c in profile.categorical_columns
            if profile.column_profiles[c].cardinality <= 20
            and profile.column_profiles[c].missing_pct < 0.5
        ]

        # Get time_column options
        time_column_options = profile.datetime_columns.copy()

        candidate = KPICandidate(
            column=col,
            dtype=col_profile.dtype,
            cardinality=col_profile.cardinality,
            missing_pct=col_profile.missing_pct,
            aggregations=aggregations,
            segment_by_options=segment_by_options,
            time_column_options=time_column_options,
        )

        candidates.append(candidate)
        print(
            f"[KPI_GEN] Added candidate: {col} (cardinality={col_profile.cardinality}, missing={col_profile.missing_pct:.1%})"
        )

    print(f"[KPI_GEN] Generated {len(candidates)} KPI candidates")

    return candidates
