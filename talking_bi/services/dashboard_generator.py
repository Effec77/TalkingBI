from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


MAX_CHARTS = 4
MAX_KPIS = 2
MAX_DIMS = 2
MAX_WORKING_ROWS = 300_000
MAX_CATEGORY_POINTS = 20
MAX_TREND_POINTS = 120


def _clean_name_score(name: str) -> float:
    """
    Higher score for cleaner names (letters/underscores, fewer symbols).
    """
    if not name:
        return 0.0
    chars = list(name)
    alpha_num = sum(c.isalnum() or c == "_" for c in chars)
    return alpha_num / max(len(chars), 1)


def _is_identifier(col: str, meta: Dict[str, Any]) -> bool:
    name = (col or "").lower()
    if name.endswith("_id") or name == "id":
        return True
    semantic = str(meta.get("semantic_type", "")).lower()
    # Keep deterministic and conservative.
    return semantic == "identifier"


def _fallback_kpis_from_df(df: pd.DataFrame, profile: Dict[str, Dict[str, Any]]) -> List[str]:
    """
    Conservative fallback for messy/high-null datasets where DIL misses KPI columns.
    Data-backed only: numeric coercion success ratio + non-trivial value count.
    """
    candidates: List[Tuple[float, float, float, str]] = []
    for col in df.columns:
        meta = profile.get(col, {})
        if _is_identifier(col, meta):
            continue

        null_pct = float(meta.get("null_pct", 0.0))
        if null_pct > 0.95:
            continue

        series = df[col]
        parsed = _safe_numeric(series)
        parsed_non_null = int(parsed.notna().sum())
        original_non_null = int(series.notna().sum())
        if original_non_null == 0:
            continue

        parse_ratio = parsed_non_null / original_non_null
        # strict but practical for messy data
        if parse_ratio < 0.60 or parsed_non_null < 2:
            continue

        # Rank: lower nulls, higher parse ratio, cleaner names.
        candidates.append((null_pct, -parse_ratio, -_clean_name_score(col), col))

    candidates.sort()
    return [c[3] for c in candidates[:MAX_KPIS]]


def _select_kpis(profile: Dict[str, Dict[str, Any]], df: Optional[pd.DataFrame] = None) -> List[str]:
    candidates: List[tuple] = []
    for col, meta in profile.items():
        role_scores = meta.get("role_scores", {}) or {}
        if float(role_scores.get("is_kpi", 0.0)) != 1.0:
            continue
        if _is_identifier(col, meta):
            continue
        null_pct = float(meta.get("null_pct", 0.0))
        if null_pct > 0.50:
            continue

        # Rank: lower null first, cleaner name second.
        candidates.append((null_pct, -_clean_name_score(col), col))

    candidates.sort()
    selected = [c[2] for c in candidates[:MAX_KPIS]]
    if selected:
        return selected

    if df is not None:
        return _fallback_kpis_from_df(df, profile)
    return []


def _select_dimensions(profile: Dict[str, Dict[str, Any]]) -> List[str]:
    dims: List[tuple] = []
    for col, meta in profile.items():
        role_scores = meta.get("role_scores", {}) or {}
        if float(role_scores.get("is_dimension", 0.0)) != 1.0:
            continue
        if _is_identifier(col, meta):
            continue
        bucket = str(meta.get("cardinality_bucket", "")).lower()
        if bucket not in {"low", "med"}:
            continue
        null_pct = float(meta.get("null_pct", 0.0))
        dims.append((null_pct, -_clean_name_score(col), col))

    dims.sort()
    return [d[2] for d in dims[:MAX_DIMS]]


def _select_time_column(profile: Dict[str, Dict[str, Any]]) -> Optional[str]:
    time_candidates: List[str] = []
    for col, meta in profile.items():
        role_scores = meta.get("role_scores", {}) or {}
        if float(role_scores.get("is_date", 0.0)) == 1.0:
            time_candidates.append(col)
    return time_candidates[0] if time_candidates else None


def _safe_numeric(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")
    # Minimal cleaning for messy numeric strings.
    cleaned = (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("$", "", regex=False)
        .str.replace("₹", "", regex=False)
        .str.strip()
    )
    return pd.to_numeric(cleaned, errors="coerce")


def _working_df(df: pd.DataFrame) -> pd.DataFrame:
    if len(df) <= MAX_WORKING_ROWS:
        return df
    return df.sample(n=MAX_WORKING_ROWS, random_state=42).sort_index()


def _kpi_card(df: pd.DataFrame, col: str) -> Dict[str, Any]:
    s = _safe_numeric(df[col]).dropna()
    if s.empty:
        return {
            "name": col.replace("_", " ").title(),
            "column": col,
            "total": 0.0,
            "average": 0.0,
            "min": 0.0,
            "max": 0.0,
        }
    return {
        "name": col.replace("_", " ").title(),
        "column": col,
        "total": float(s.sum()),
        "average": float(s.mean()),
        "min": float(s.min()),
        "max": float(s.max()),
    }


def _bar_chart(df: pd.DataFrame, kpi_col: str, dim_col: str) -> Optional[Dict[str, Any]]:
    temp = df[[kpi_col, dim_col]].copy()
    temp[kpi_col] = _safe_numeric(temp[kpi_col])
    temp = temp.dropna(subset=[kpi_col, dim_col])
    if temp.empty:
        return None
    grouped = temp.groupby(dim_col, dropna=False)[kpi_col].sum().reset_index()
    grouped = grouped.sort_values(kpi_col, ascending=False).head(MAX_CATEGORY_POINTS)
    if grouped.empty:
        return None
    return {
        "type": "bar",
        "title": f"{kpi_col.replace('_', ' ').title()} by {dim_col.replace('_', ' ').title()}",
        "x": grouped[dim_col].astype(str).tolist(),
        "y": grouped[kpi_col].astype(float).tolist(),
        "kpi": kpi_col,
        "dimension": dim_col,
    }


def _trend_chart(df: pd.DataFrame, kpi_col: str, time_col: str) -> Optional[Dict[str, Any]]:
    temp = df[[kpi_col, time_col]].copy()
    temp[kpi_col] = _safe_numeric(temp[kpi_col])
    temp[time_col] = pd.to_datetime(temp[time_col], errors="coerce")
    temp = temp.dropna(subset=[kpi_col, time_col])
    if temp.empty:
        return None
    grouped = temp.groupby(time_col)[kpi_col].sum().reset_index().sort_values(time_col)
    if len(grouped) > MAX_TREND_POINTS:
        grouped = (
            grouped.set_index(time_col)
            .resample("ME")[kpi_col]
            .sum()
            .reset_index()
            .sort_values(time_col)
        )
    if len(grouped) > MAX_TREND_POINTS:
        grouped = grouped.tail(MAX_TREND_POINTS)
    if grouped.empty:
        return None
    return {
        "type": "line",
        "title": f"{kpi_col.replace('_', ' ').title()} over Time",
        "x": grouped[time_col].dt.strftime("%Y-%m-%d").tolist(),
        "y": grouped[kpi_col].astype(float).tolist(),
        "kpi": kpi_col,
        "dimension": time_col,
    }


def _histogram_chart(df: pd.DataFrame, kpi_col: str) -> Optional[Dict[str, Any]]:
    s = _safe_numeric(df[kpi_col]).dropna()
    if s.empty:
        return None
    # Keep payload bounded for UI responsiveness.
    values = s.astype(float).head(2000).tolist()
    return {
        "type": "histogram",
        "title": f"{kpi_col.replace('_', ' ').title()} Distribution",
        "values": values,
        "kpi": kpi_col,
    }


def generate_auto_dashboard(df: pd.DataFrame, profile: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Build deterministic default dashboard payload immediately on upload.
    """
    df_work = _working_df(df)
    selected_kpis = _select_kpis(profile, df=df_work)
    selected_dims = _select_dimensions(profile)
    time_col = _select_time_column(profile)

    if not selected_kpis:
        return {
            "kpis": [],
            "charts": [],
            "insights": [],
            "fallback": {
                "message": "No strong numeric metrics detected in this dataset.",
                "suggestions": [
                    "show column distribution",
                    "list columns",
                    "show sample data",
                ],
            },
        }

    kpi_cards = [_kpi_card(df_work, col) for col in selected_kpis]

    charts: List[Dict[str, Any]] = []

    # Chart 1..N: KPI by dimension (up to 2 charts total)
    for kpi_col in selected_kpis:
        for dim_col in selected_dims[:1]:
            chart = _bar_chart(df_work, kpi_col, dim_col)
            if chart:
                charts.append(chart)
            if len(charts) >= MAX_CHARTS:
                break
        if len(charts) >= 2:
            break

    # Trend chart (if time exists)
    if len(charts) < MAX_CHARTS and selected_kpis and time_col:
        trend = _trend_chart(df_work, selected_kpis[0], time_col)
        if trend:
            charts.append(trend)

    # Distribution chart
    if len(charts) < MAX_CHARTS and selected_kpis:
        hist = _histogram_chart(df_work, selected_kpis[0])
        if hist:
            charts.append(hist)

    return {
        "kpis": kpi_cards,
        "charts": charts[:MAX_CHARTS],
        "insights": [],
        "fallback": None,
    }
