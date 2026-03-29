"""
KPI Validator - Phase 0B.4
MANDATORY validation of LLM-selected KPIs
"""
import pandas as pd
from typing import List, Dict
from services.dataset_profiler import DatasetProfile


def validate_kpis(kpis: List[Dict], df: pd.DataFrame, profile: DatasetProfile) -> List[Dict]:
    """
    Validate KPIs from LLM selection.
    
    Checks:
    - Column exists
    - No duplicates
    - Semantic diversity
    
    Fallback:
    - If validation fails, use first 3 valid numeric columns
    
    Args:
        kpis: KPIs from LLM
        df: DataFrame
        profile: Dataset profile
        
    Returns:
        Validated KPIs (exactly 3)
    """
    print(f"[VALIDATOR] Validating {len(kpis)} KPIs")
    
    validated = []
    seen_columns = set()
    
    for kpi in kpis:
        # Check column exists
        if kpi['source_column'] not in df.columns:
            print(f"[VALIDATOR] Error: Column '{kpi['source_column']}' does not exist")
            continue
        
        # Check no duplicates
        if kpi['source_column'] in seen_columns:
            print(f"[VALIDATOR] Error: Duplicate column '{kpi['source_column']}'")
            continue
        
        # Check column is numeric
        if kpi['source_column'] not in profile.numeric_columns:
            print(f"[VALIDATOR] Error: Column '{kpi['source_column']}' is not numeric")
            continue
        
        # Check segment_by exists if specified
        if kpi.get('segment_by') and kpi['segment_by'] not in df.columns:
            print(f"[VALIDATOR] Warning: segment_by '{kpi['segment_by']}' does not exist, setting to None")
            kpi['segment_by'] = None
        
        # Check time_column exists if specified
        if kpi.get('time_column') and kpi['time_column'] not in df.columns:
            print(f"[VALIDATOR] Warning: time_column '{kpi['time_column']}' does not exist, setting to None")
            kpi['time_column'] = None
        
        # Valid KPI
        validated.append(kpi)
        seen_columns.add(kpi['source_column'])
        print(f"[VALIDATOR] ✓ Validated KPI: {kpi['name']}")
    
    # Check semantic diversity (simple check: different columns)
    if len(validated) < 3:
        print(f"[VALIDATOR] Warning: Only {len(validated)} valid KPIs, using fallback")
        validated = _fallback_kpis(profile)
    
    # Ensure exactly 3 KPIs
    validated = validated[:3]
    
    print(f"[VALIDATOR] Final: {len(validated)} validated KPIs")
    return validated


def _fallback_kpis(profile: DatasetProfile) -> List[Dict]:
    """
    Fallback KPIs when validation fails.
    Uses first 3 valid numeric columns.
    """
    print("[VALIDATOR] Using fallback KPIs")
    
    fallback = []
    for col in profile.numeric_columns[:3]:
        col_profile = profile.column_profiles[col]
        
        # Skip if too many missing values
        if col_profile.missing_pct >= 0.3:
            continue
        
        # Skip if too few unique values
        if col_profile.unique_values <= 5:
            continue
        
        kpi = {
            "name": col.replace('_', ' ').title(),
            "source_column": col,
            "aggregation": "sum",
            "segment_by": None,
            "time_column": None,
            "business_meaning": f"Total {col}",
            "confidence": 0.5
        }
        fallback.append(kpi)
        
        if len(fallback) >= 3:
            break
    
    return fallback
