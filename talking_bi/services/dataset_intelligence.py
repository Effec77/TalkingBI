import pandas as pd
from typing import Dict, Any, List
import warnings

class DatasetIntelligence:
    """Phase 9C.3 Upgrade - Advanced dataset intelligence extraction."""
    
    def __init__(self, df: pd.DataFrame):
        self.df = df
        
    def _analyze(self, col: str) -> Dict[str, Any]:
        series = self.df[col]
        dtype = str(series.dtype)
        unique_cnt = series.nunique()
        total = len(self.df)
        null_pct = float(series.isna().mean())
        
        # Sample values
        sample_values = series.dropna().astype(str).unique()[:3].tolist()
        
        # Determine semantic type & types
        is_numeric = pd.api.types.is_numeric_dtype(series)
        is_datetime = pd.api.types.is_datetime64_any_dtype(series)
        if not is_datetime and series.dtype == 'object':
            try:
                sample = series.dropna().head(50)
                if not sample.empty:
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore", UserWarning)
                        parsed = pd.to_datetime(sample, errors="coerce", format="mixed")
                    if float(parsed.notna().mean()) >= 0.8:
                        is_datetime = True
            except:
                pass
                
        if is_numeric:
            semantic_type = "kpi"
        elif is_datetime:
            semantic_type = "date"
        else:
            semantic_type = "dimension"
            
        # Cardinality bucket
        if unique_cnt <= 10:
            card_bucket = "low"
        elif unique_cnt <= 100:
            card_bucket = "med"
        else:
            card_bucket = "high"
            
        # Role Scores
        role_scores = {
            "is_kpi": 1.0 if is_numeric and card_bucket in ("high", "med") and not col.endswith("_id") else 0.0,
            "is_dimension": 1.0 if not is_numeric and card_bucket in ("low", "med") else 0.0,
            "is_date": 1.0 if is_datetime else 0.0
        }
        
        # ID columns are often high cardinality dimensions
        if col.endswith("_id") or card_bucket == "high" and not is_numeric:
            role_scores["is_dimension"] = 1.0
            
        # Distribution
        distribution = {}
        if is_numeric:
            distribution["mean"] = float(series.mean()) if not series.empty else 0.0
            distribution["min"] = float(series.min()) if not series.empty else 0.0
            distribution["max"] = float(series.max()) if not series.empty else 0.0
            
        return {
            "dtype": dtype,
            "semantic_type": semantic_type,
            "cardinality_bucket": card_bucket,
            "null_pct": null_pct,
            "unique": unique_cnt,
            "sample_values": sample_values,
            "distribution": distribution,
            "role_scores": role_scores
        }

    def build(self) -> Dict[str, Dict[str, Any]]:
        profile = {}
        for col in self.df.columns:
            profile[col] = self._analyze(col)
        return profile
