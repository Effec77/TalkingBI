from __future__ import annotations

import re
from typing import Any, Dict, List

import pandas as pd


HIGH_NULL_THRESHOLD_PCT = 30.0


def _canonical_value(value: Any) -> str:
    raw = str(value or "").strip().lower()
    raw = re.sub(r"[^a-z0-9]+", " ", raw).strip()
    return raw


def _is_dimension_col(col: str, profile: Dict[str, Dict[str, Any]]) -> bool:
    meta = profile.get(col, {}) or {}
    role = meta.get("role_scores", {}) or {}
    return float(role.get("is_dimension", 0.0)) == 1.0


def _build_dimension_value_map(df: pd.DataFrame, profile: Dict[str, Dict[str, Any]]) -> Dict[str, List[str]]:
    """
    Return distinct values for low/medium-cardinality dimension columns.
    Keeps output bounded for stability.
    """
    out: Dict[str, List[str]] = {}
    for col in df.columns:
        if not _is_dimension_col(col, profile):
            continue
        meta = profile.get(col, {}) or {}
        bucket = str(meta.get("cardinality_bucket", "")).lower()
        unique_est = int(meta.get("unique", int(df[col].nunique(dropna=True))))
        if bucket not in {"low", "med", "medium"} and unique_est > 300:
            continue

        vals = (
            df[col]
            .dropna()
            .astype(str)
            .str.strip()
        )
        vals = vals[vals != ""].drop_duplicates()
        if vals.empty:
            continue
        ordered = sorted(vals.tolist(), key=lambda x: x.lower())[:300]
        out[col] = ordered
    return out


def _infer_mixed_type_columns(df: pd.DataFrame) -> List[str]:
    """
    Detect likely mixed-type columns using parseability ratios.

    A column is marked mixed when it has both:
    - substantial numeric-like values
    - substantial non-numeric values
    """
    mixed: List[str] = []

    for col in df.columns:
        series = df[col].dropna()
        if series.empty:
            continue

        if pd.api.types.is_numeric_dtype(series) or pd.api.types.is_datetime64_any_dtype(
            series
        ):
            continue

        as_str = series.astype(str)
        numeric_ratio = pd.to_numeric(as_str, errors="coerce").notna().mean()

        # Mixed if both numeric and non-numeric portions are meaningful.
        if 0.15 <= numeric_ratio <= 0.85:
            mixed.append(col)

    return mixed


def build_dataset_summary(df: pd.DataFrame, profile: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Build deterministic dataset summary from dataframe + DIL profile.
    """
    row_count, column_count = df.shape
    total_cells = max(row_count * column_count, 1)
    missing_cells = int(df.isna().sum().sum())
    missing_cells_pct = round((missing_cells / total_cells) * 100, 2)

    columns: List[Dict[str, Any]] = []
    high_null_columns: List[str] = []

    for col in df.columns:
        meta = profile.get(col, {})
        null_pct = round(float(meta.get("null_pct", 0.0)) * 100, 2)
        unique = int(meta.get("unique", int(df[col].nunique(dropna=True))))
        semantic_type = str(meta.get("semantic_type", "unknown"))
        dtype = str(df[col].dtype)

        columns.append(
            {
                "name": col,
                "type": dtype,
                "semantic_type": semantic_type,
                "null_pct": null_pct,
                "unique": unique,
            }
        )

        if null_pct >= HIGH_NULL_THRESHOLD_PCT:
            high_null_columns.append(col)

    summary: Dict[str, Any] = {
        "row_count": int(row_count),
        "column_count": int(column_count),
        "columns": columns,
        "dimension_values": _build_dimension_value_map(df, profile),
        "data_quality": {
            "missing_cells_pct": missing_cells_pct,
            "high_null_columns": high_null_columns,
            "mixed_type_columns": _infer_mixed_type_columns(df),
        },
    }

    return summary


def generate_human_summary(summary: Dict[str, Any]) -> str:
    """
    Convert structured summary into readable text for product UX.
    """
    row_count = summary.get("row_count", 0)
    column_count = summary.get("column_count", 0)
    cols = summary.get("columns", [])
    quality = summary.get("data_quality", {})

    kpi_cols = [c["name"] for c in cols if c.get("semantic_type") == "kpi"][:3]
    dim_cols = [c["name"] for c in cols if c.get("semantic_type") == "dimension"][:3]
    date_cols = [c["name"] for c in cols if c.get("semantic_type") == "date"][:2]

    parts: List[str] = [
        f"This dataset contains {row_count:,} rows and {column_count} columns."
    ]

    key_bits: List[str] = []
    if kpi_cols:
        key_bits.append("KPIs: " + ", ".join(kpi_cols))
    if dim_cols:
        key_bits.append("Dimensions: " + ", ".join(dim_cols))
    if date_cols:
        key_bits.append("Time columns: " + ", ".join(date_cols))
    if key_bits:
        parts.append("Key columns include " + " | ".join(key_bits) + ".")

    missing_pct = quality.get("missing_cells_pct", 0.0)
    high_null = quality.get("high_null_columns", []) or []
    mixed = quality.get("mixed_type_columns", []) or []
    quality_bits = [f"overall missing cells: {missing_pct}%"]
    if high_null:
        quality_bits.append(f"high-null columns: {', '.join(high_null[:5])}")
    if mixed:
        quality_bits.append(f"mixed-type columns: {', '.join(mixed[:5])}")
    parts.append("Data quality: " + "; ".join(quality_bits) + ".")

    return " ".join(parts)


def answer_dataset_question(
    query: str, summary: Dict[str, Any], profile: Dict[str, Dict[str, Any]]
) -> str | None:
    """
    Deterministic DAL QA for basic dataset-understanding questions.
    Returns None when query is not a DAL-style metadata question.
    """
    q = (query or "").strip().lower()
    if not q:
        return None

    columns = summary.get("columns", []) or []
    col_names = [c.get("name") for c in columns if c.get("name")]
    row_count = int(summary.get("row_count", 0))
    col_count = int(summary.get("column_count", 0))
    dq = summary.get("data_quality", {}) or {}
    dimension_values = summary.get("dimension_values", {}) or {}
    missing_cells_pct = dq.get("missing_cells_pct", 0.0)
    high_null_columns = dq.get("high_null_columns", []) or []
    mixed_cols = dq.get("mixed_type_columns", []) or []

    kpi_cols = []
    for c, m in profile.items():
        role_kpi = float((m.get("role_scores", {}) or {}).get("is_kpi", 0.0)) == 1.0
        semantic_kpi = str(m.get("semantic_type", "")).lower() == "kpi"
        if role_kpi or semantic_kpi:
            kpi_cols.append(c)
    dim_cols = [
        c
        for c, m in profile.items()
        if float((m.get("role_scores", {}) or {}).get("is_dimension", 0.0)) == 1.0
        and not c.endswith("_id")
        and c != "id"
    ]
    if not dim_cols and dimension_values:
        dim_cols = list(dimension_values.keys())
    time_cols = [
        c
        for c, m in profile.items()
        if float((m.get("role_scores", {}) or {}).get("is_time", (m.get("role_scores", {}) or {}).get("is_date", 0.0)))
        == 1.0
    ]

    # Dataset overview
    if any(p in q for p in ["what is in this dataset", "dataset summary", "about this data"]):
        return generate_human_summary(summary)

    # Row / column counts
    if "how many rows" in q or "how many records" in q:
        return f"This dataset has {row_count:,} rows."
    if "how many columns" in q or "how many fields" in q:
        return f"This dataset has {col_count} columns."

    # KPI / metric awareness
    if "how many metrics" in q or "how many kpis" in q:
        return f"I found {len(kpi_cols)} metric columns: {', '.join(kpi_cols[:10])}."
    if "what metrics" in q or "list metrics" in q or "which metrics" in q:
        if not kpi_cols:
            return "I could not find any high-confidence metric columns."
        return f"Metric columns are: {', '.join(kpi_cols[:12])}."

    # Dimension awareness
    if "how many dimensions" in q:
        return f"I found {len(dim_cols)} dimension columns: {', '.join(dim_cols[:10])}."
    if "what dimensions" in q or "list dimensions" in q:
        if not dim_cols:
            return "I could not find any high-confidence dimension columns."
        return f"Dimension columns are: {', '.join(dim_cols[:12])}."

    # Dimension value awareness (e.g., "how many departments are there")
    singular_alias = {
        "department": "departments",
        "team": "teams",
        "region": "regions",
        "location": "locations",
        "office": "offices",
        "designation": "designations",
        "role": "roles",
        "manager": "managers",
        "specialty": "specialties",
        "doctor": "doctors",
        "patient": "patients",
        "hospital": "hospitals",
    }

    def _match_dim_for_value_question() -> str | None:
        q_tokens = set(re.findall(r"[a-z0-9_]+", q))
        for d in dim_cols:
            dn = str(d).lower()
            if dn in q:
                return d
            for alias in (dn, dn.rstrip("s"), dn + "s"):
                if alias and alias in q:
                    return d
            for alias in singular_alias.keys():
                if alias in q and alias in dn:
                    return d
            normalized_dn = re.sub(r"[^a-z0-9]+", "_", dn).strip("_")
            dn_tokens = set([t for t in normalized_dn.split("_") if t])
            if dn_tokens and dn_tokens.intersection(q_tokens):
                return d
        for c in col_names:
            cn = str(c).lower()
            if cn in q or cn.rstrip("s") in q:
                return c
        return None

    target_dim = _match_dim_for_value_question()
    if target_dim:
        dim_meta = profile.get(target_dim, {}) or {}
        unique_count = int(dim_meta.get("unique", 0) or 0)
        samples = dim_meta.get("sample_values", []) or []
        label = singular_alias.get(target_dim.lower(), f"{target_dim}s")

        if ("how many" in q or "count" in q or "number of" in q) and (
            "are there" in q or "do we have" in q or "exist" in q or target_dim in q
        ):
            full_values = dimension_values.get(target_dim, []) or []
            if full_values:
                canonical_count = len({_canonical_value(v) for v in full_values if _canonical_value(v)})
                if canonical_count > 0 and canonical_count != len(full_values):
                    return (
                        f"There are {len(full_values)} raw unique {label} "
                        f"({canonical_count} after normalization of case/spelling variants)."
                    )
                return f"There are {len(full_values)} unique {label} in this dataset."
            if unique_count <= 0:
                return f"I could not determine how many {label} are present."
            if samples:
                return (
                    f"There are {unique_count} unique {label} in this dataset. "
                    f"Examples: {', '.join([str(x) for x in samples[:6]])}."
                )
            return f"There are {unique_count} unique {label} in this dataset."

        if any(p in q for p in ["what are", "which are", "list", "show"]) and (
            target_dim in q or any(a in q for a in ["department", "team", "region", "location", "office"])
        ):
            full_values = dimension_values.get(target_dim, []) or []
            if full_values:
                if len(full_values) > 30:
                    preview = ", ".join([str(x) for x in full_values[:30]])
                    return (
                        f"All {label} ({len(full_values)}): {preview}, ... "
                        f"(showing first 30)."
                    )
                return f"All {label} ({len(full_values)}): {', '.join([str(x) for x in full_values])}."
            if samples:
                return f"Sample {label}: {', '.join([str(x) for x in samples[:12]])}."
            return f"I identified {unique_count} unique values in '{target_dim}'."

    # Generic unique-value questions when dimension can be inferred loosely
    if any(p in q for p in ["how many unique", "how many distinct", "list all", "show all"]):
        for d in dim_cols:
            if d in q or d.rstrip("s") in q:
                full_values = dimension_values.get(d, []) or []
                if full_values and ("how many" in q or "count" in q):
                    return f"There are {len(full_values)} unique values in {d}."
                if full_values:
                    preview = ", ".join([str(x) for x in full_values[:30]])
                    tail = ", ..." if len(full_values) > 30 else ""
                    return f"Values in {d} ({len(full_values)}): {preview}{tail}."

    # Time awareness
    if "time column" in q or "date column" in q:
        if not time_cols:
            return "I could not find a reliable time/date column."
        return f"Time/date columns are: {', '.join(time_cols[:5])}."

    # Column listing
    if "list columns" in q or "what columns" in q or "which columns" in q:
        preview = ", ".join(col_names[:20])
        suffix = " ..." if len(col_names) > 20 else ""
        return f"Columns are: {preview}{suffix}."

    # Data quality checks
    if "is data clean" in q or "data quality" in q or "is the data clean" in q:
        parts = [f"overall missing cells are {missing_cells_pct}%"]
        if high_null_columns:
            parts.append(f"high-null columns: {', '.join(high_null_columns[:6])}")
        if mixed_cols:
            parts.append(f"mixed-type columns: {', '.join(mixed_cols[:6])}")
        return "Data quality summary: " + "; ".join(parts) + "."

    if "missing" in q and ("column" in q or "data" in q):
        if not high_null_columns:
            return f"Overall missing cells are {missing_cells_pct}%, and no high-null columns were flagged."
        return f"Overall missing cells are {missing_cells_pct}%. High-null columns: {', '.join(high_null_columns[:10])}."

    return None
