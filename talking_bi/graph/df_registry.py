"""
DataFrame Registry — Phase 2

LangGraph state must be JSON-serializable (TypedDict).
pandas DataFrames cannot live inside state directly.

This module provides a simple in-memory registry keyed by run_id.
Nodes call get_df(run_id) to retrieve the DataFrame for their run.
The registry is populated by api/run.py before graph.invoke() is called,
and cleared after the pipeline completes.
"""

from typing import Dict
import pandas as pd

_store: Dict[str, pd.DataFrame] = {}


def register_df(run_id: str, df: pd.DataFrame) -> None:
    """Store a DataFrame for the duration of a pipeline run."""
    _store[run_id] = df
    print(f"[DF_REGISTRY] Registered df for run_id={run_id} ({df.shape[0]} rows, {df.shape[1]} cols)")


def get_df(run_id: str) -> pd.DataFrame:
    """Retrieve the DataFrame for this run. Raises KeyError if not found."""
    if run_id not in _store:
        raise KeyError(f"[DF_REGISTRY] No DataFrame found for run_id={run_id}")
    return _store[run_id]


def deregister_df(run_id: str) -> None:
    """Free the DataFrame after pipeline completes."""
    if run_id in _store:
        del _store[run_id]
        print(f"[DF_REGISTRY] Deregistered df for run_id={run_id}")
