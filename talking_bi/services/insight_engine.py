from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


MAX_INSIGHTS = 5
MAX_WORKING_ROWS = 300_000


def _safe_numeric(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")
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


def _format_pct(value: float) -> str:
    return f"{value:.1f}%"


def _primary_kpi(dashboard_output: Dict[str, Any]) -> Optional[str]:
    kpis = dashboard_output.get("kpis", []) or []
    if not kpis:
        return None
    return kpis[0].get("column")


def _pick_dimension_from_charts(charts: List[Dict[str, Any]], kpi_col: str) -> Optional[str]:
    for c in charts:
        if c.get("type") == "bar" and c.get("kpi") == kpi_col and c.get("dimension"):
            return c.get("dimension")
    return None


def _pick_time_column(profile: Dict[str, Dict[str, Any]]) -> Optional[str]:
    for col, meta in profile.items():
        role = meta.get("role_scores", {}) or {}
        if float(role.get("is_date", 0.0)) == 1.0:
            return col
    return None


def _group_sum(df: pd.DataFrame, kpi_col: str, dim_col: str) -> Optional[pd.Series]:
    temp = df[[kpi_col, dim_col]].copy()
    temp[kpi_col] = _safe_numeric(temp[kpi_col])
    temp = temp.dropna(subset=[kpi_col, dim_col])
    if temp.empty:
        return None
    grouped = temp.groupby(dim_col)[kpi_col].sum()
    if grouped.empty:
        return None
    return grouped


def _trend_pct(df: pd.DataFrame, kpi_col: str, time_col: str) -> Optional[Tuple[float, Any]]:
    temp = df[[kpi_col, time_col]].copy()
    temp[kpi_col] = _safe_numeric(temp[kpi_col])
    temp[time_col] = pd.to_datetime(temp[time_col], errors="coerce")
    temp = temp.dropna(subset=[kpi_col, time_col])
    if len(temp) < 2:
        return None
    trend = temp.groupby(time_col)[kpi_col].sum().sort_index()
    if len(trend) < 2:
        return None

    prev = float(trend.iloc[-2])
    last = float(trend.iloc[-1])
    latest_period = trend.index[-1]

    if prev == 0:
        return None
    pct = ((last - prev) / abs(prev)) * 100
    return pct, latest_period


def _anomaly_in_group(grouped: pd.Series) -> Optional[Tuple[Any, float]]:
    if grouped is None or len(grouped) < 3:
        return None
    mean = float(grouped.mean())
    std = float(grouped.std(ddof=0))
    if std == 0:
        return None
    threshold = mean + (2 * std)
    spikes = grouped[grouped > threshold]
    if spikes.empty:
        return None
    idx = spikes.idxmax()
    val = float(spikes.loc[idx])
    return idx, val


def generate_insights(
    df: pd.DataFrame, profile: Dict[str, Dict[str, Any]], dashboard_output: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Deterministic phase-11 insight generation (no LLM, max 5 insights).
    """
    df_work = _working_df(df)
    ranked: List[Tuple[int, str]] = []
    charts = dashboard_output.get("charts", []) or []
    kpi_col = _primary_kpi(dashboard_output)
    if not kpi_col or kpi_col not in df_work.columns:
        return {"primary_insight": None, "insights": []}

    kpi_label = kpi_col.replace("_", " ").title()
    dim_col = _pick_dimension_from_charts(charts, kpi_col)

    # 1) Top performer
    grouped = None
    top_text: Optional[str] = None
    if dim_col and dim_col in df_work.columns:
        grouped = _group_sum(df_work, kpi_col, dim_col)
        if grouped is not None and len(grouped) >= 1:
            top = grouped.idxmax()
            top_text = f"{kpi_label} is highest in {top}."
            ranked.append((2, top_text))

    # 2) Lowest performer
    if grouped is not None and len(grouped) >= 1:
        low = grouped.idxmin()
        ranked.append((5, f"{kpi_label} is lowest in {low}."))

    # 3) Contribution %
    if grouped is not None and len(grouped) >= 1:
        total = float(grouped.sum())
        if total != 0:
            top = grouped.idxmax()
            top_val = float(grouped.loc[top])
            contrib = (top_val / total) * 100
            ranked.append((3, f"{top} contributes {_format_pct(contrib)} of total {kpi_label.lower()}."))

    # 4) Trend insight
    time_col = _pick_time_column(profile)
    if time_col and time_col in df_work.columns:
        trend = _trend_pct(df_work, kpi_col, time_col)
        if trend:
            pct, latest_period = trend
            direction = "increased" if pct >= 0 else "decreased"
            latest_str = pd.to_datetime(latest_period).strftime("%Y-%m-%d")
            ranked.append(
                (1, f"{kpi_label} {direction} by {_format_pct(abs(pct))} in the latest period ({latest_str}).")
            )

    # 5) Simple anomaly
    anomaly_src = grouped
    axis_name = dim_col
    if anomaly_src is None and time_col and time_col in df_work.columns:
        temp = df_work[[kpi_col, time_col]].copy()
        temp[kpi_col] = _safe_numeric(temp[kpi_col])
        temp[time_col] = pd.to_datetime(temp[time_col], errors="coerce")
        temp = temp.dropna(subset=[kpi_col, time_col])
        if len(temp) >= 3:
            anomaly_src = temp.groupby(time_col)[kpi_col].sum().sort_index()
            axis_name = time_col

    spike = _anomaly_in_group(anomaly_src) if anomaly_src is not None else None
    if spike:
        where, _ = spike
        label = where
        if axis_name and "date" in axis_name:
            label = pd.to_datetime(where).strftime("%Y-%m-%d")
        ranked.append((4, f"Unusual spike detected in {label} for {kpi_label.lower()}."))

    if not ranked and top_text:
        ranked.append((2, top_text))

    ranked = sorted(ranked, key=lambda x: (x[0], x[1]))
    insights = [text for _priority, text in ranked[:MAX_INSIGHTS]]
    primary_insight = insights[0] if insights else (top_text or None)

    return {"primary_insight": primary_insight, "insights": insights}
