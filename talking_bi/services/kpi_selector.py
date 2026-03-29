"""
Python-First KPI Selector - Phase 0B Patch
CRITICAL: This is the PRIMARY KPI selection mechanism
LLM is ONLY for enrichment, NOT selection
"""
import pandas as pd
from typing import List


def select_kpis_python(df: pd.DataFrame) -> List[str]:
    """
    Python-only KPI selection (PRIMARY METHOD).
    MUST return EXACTLY 3 KPIs.
    
    Rules:
    1. Prefer numeric columns with > 5 unique values
    2. Prefer columns with < 30% missing values
    3. ALWAYS return exactly 3 KPIs
    4. Use fallback if needed
    
    Args:
        df: Input DataFrame
        
    Returns:
        List of exactly 3 column names
    """
    print("[KPI_SELECTOR] Python-first KPI selection starting")
    
    # Detect numeric and datetime columns explicitly.
    numeric_cols = df.select_dtypes(include=["int", "int64", "float", "float64"]).columns.tolist()
    datetime_cols = df.select_dtypes(include=["datetime", "datetimetz", "datetime64[ns]"]).columns.tolist()
    
    if not numeric_cols:
        print("[KPI_SELECTOR] Warning: No numeric columns found (will rely on semantic fallback KPIs)")
        return []
    
    print(f"[KPI_SELECTOR] Found {len(numeric_cols)} numeric columns")
    
    # Filter valid numeric columns only.
    valid_numeric = []
    for col in numeric_cols:
        nunique = df[col].nunique()
        missing_pct = df[col].isna().mean()
        
        if nunique > 5 and missing_pct < 0.3:
            valid_numeric.append(col)
            print(f"[KPI_SELECTOR] OK {col}: nunique={nunique}, missing={missing_pct:.1%}")
        else:
            print(f"[KPI_SELECTOR] SKIP {col}: nunique={nunique}, missing={missing_pct:.1%} (filtered out)")
    
    # Primary selection.
    kpis = list(valid_numeric[:3])
    
    # Numeric-only fallback if we don't have 3.
    if len(kpis) < 3:
        print(f"[KPI_SELECTOR] Only {len(kpis)} valid KPIs, using fallback")
        fallback = list(numeric_cols[:3])
        for col in fallback:
            if col not in kpis:
                kpis.append(col)
            if len(kpis) == 3:
                break

    # Never use datetime/object columns as numeric KPI sources.
    kpis = [col for col in kpis if col in numeric_cols and col not in datetime_cols][:3]
    
    print(f"[KPI_SELECTOR] Selected numeric KPI columns: {kpis}")
    return kpis
