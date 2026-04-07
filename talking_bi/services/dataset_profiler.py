"""
Dataset Profiler - Phase 0B.1
Python ONLY - No LLM
Computes dataset statistics and column characteristics
"""
import pandas as pd
from typing import Dict, List
from dataclasses import dataclass
import warnings


@dataclass
class ColumnProfile:
    """Profile for a single column"""
    name: str
    dtype: str
    cardinality: int
    missing_pct: float
    unique_values: int
    sample_values: List[str]
    is_numeric: bool
    is_categorical: bool
    is_datetime: bool


@dataclass
class DatasetProfile:
    """Complete dataset profile"""
    total_rows: int
    total_columns: int
    numeric_columns: List[str]
    categorical_columns: List[str]
    datetime_columns: List[str]
    column_profiles: Dict[str, ColumnProfile]


def profile_dataset(df: pd.DataFrame) -> DatasetProfile:
    """
    Profile a dataset and extract column characteristics.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DatasetProfile with complete statistics
    """
    print(f"[PROFILER] Profiling dataset with shape {df.shape}")
    
    numeric_cols = []
    categorical_cols = []
    datetime_cols = []
    column_profiles = {}
    
    for col in df.columns:
        # Basic stats
        dtype = str(df[col].dtype)
        cardinality = df[col].nunique()
        missing_pct = float(df[col].isna().mean())
        unique_values = df[col].nunique()
        sample_values = df[col].dropna().astype(str).unique()[:3].tolist()
        
        # Determine column type
        is_numeric = pd.api.types.is_numeric_dtype(df[col])
        is_datetime = pd.api.types.is_datetime64_any_dtype(df[col])
        is_categorical = not is_numeric and not is_datetime
        
        # Try to detect datetime from object columns
        if not is_datetime and df[col].dtype == 'object':
            try:
                sample = df[col].dropna().head(50)
                if not sample.empty:
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore", UserWarning)
                        parsed = pd.to_datetime(sample, errors="coerce", format="mixed")
                    if float(parsed.notna().mean()) >= 0.8:
                        is_datetime = True
                        is_categorical = False
            except:
                pass
        
        # Categorize
        if is_numeric:
            numeric_cols.append(col)
        elif is_datetime:
            datetime_cols.append(col)
        else:
            is_categorical = True
            categorical_cols.append(col)
        
        # Create profile
        column_profiles[col] = ColumnProfile(
            name=col,
            dtype=dtype,
            cardinality=cardinality,
            missing_pct=missing_pct,
            unique_values=unique_values,
            sample_values=sample_values,
            is_numeric=is_numeric,
            is_categorical=is_categorical,
            is_datetime=is_datetime
        )
    
    profile = DatasetProfile(
        total_rows=len(df),
        total_columns=len(df.columns),
        numeric_columns=numeric_cols,
        categorical_columns=categorical_cols,
        datetime_columns=datetime_cols,
        column_profiles=column_profiles
    )
    
    print(f"[PROFILER] Found {len(numeric_cols)} numeric, {len(categorical_cols)} categorical, {len(datetime_cols)} datetime columns")
    
    return profile
