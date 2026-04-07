from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


def _norm(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (text or "").lower()).strip("_")


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


def _best_column(term: str, columns: List[str]) -> Optional[str]:
    if not term or not columns:
        return None
    t = _norm(term)
    col_map = {_norm(c): c for c in columns}
    if t in col_map:
        return col_map[t]

    # token containment
    for c in columns:
        cn = _norm(c)
        if t in cn or cn in t:
            return c

    # fuzzy fallback
    scored = []
    for c in columns:
        r = SequenceMatcher(None, t, _norm(c)).ratio()
        scored.append((r, c))
    scored.sort(reverse=True)
    if scored and scored[0][0] >= 0.65:
        return scored[0][1]
    return None


def _kpi_columns(profile: Dict[str, Dict[str, Any]]) -> List[str]:
    out = []
    for c, m in profile.items():
        role = m.get("role_scores", {}) or {}
        if float(role.get("is_kpi", 0.0)) == 1.0 or str(m.get("semantic_type", "")).lower() == "kpi":
            out.append(c)
    return out


def _dimension_columns(profile: Dict[str, Dict[str, Any]]) -> List[str]:
    out = []
    for c, m in profile.items():
        role = m.get("role_scores", {}) or {}
        if float(role.get("is_dimension", 0.0)) == 1.0 and c not in _kpi_columns(profile):
            out.append(c)
    return out


def _entity_dimension(query: str, columns: List[str], profile: Dict[str, Dict[str, Any]]) -> Optional[str]:
    q = query.lower()
    # person-like queries
    if any(w in q for w in ["person", "employee", "user", "customer", "who"]):
        priority = [
            "name",
            "employee_name",
            "customer_name",
            "user_name",
            "employee_id",
            "user_id",
            "customer_id",
            "id",
        ]
        for p in priority:
            for c in columns:
                if _norm(c) == _norm(p):
                    return c
        # fallback: first identifier-like column
        for c in columns:
            if c.endswith("_id") or c == "id":
                return c

    if "department" in q and "department" in columns:
        return "department"

    dims = _dimension_columns(profile)
    return dims[0] if dims else None


def _extract_list_values(query: str) -> List[str]:
    m = re.search(r"(such as|like)\s+(.+)$", query.lower())
    if not m:
        return []
    raw = m.group(2)
    raw = raw.replace(" and ", ",")
    vals = [v.strip(" .") for v in raw.split(",") if v.strip()]
    return vals[:10]


def _norm_series(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
        .str.lower()
        .str.replace(r"[^a-z0-9]+", "_", regex=True)
        .str.strip("_")
    )


def _extract_subquery_filter(query: str, columns: List[str]) -> Tuple[Optional[str], List[str]]:
    """
    Parse filters like:
    - in \"accounts\" department
    - in accounts department
    - in department accounts
    Returns (dimension_column, values)
    """
    q = query.lower()

    # Pattern: in "value" <dimension>
    m = re.search(r'in\s+"([^"]+)"\s+([a-z0-9_]+)', q)
    if m:
        val, dim_term = m.group(1).strip(), m.group(2).strip()
        dim = _best_column(dim_term, columns)
        if dim:
            return dim, [val]

    # Pattern: in value department
    m = re.search(r"in\s+([a-z0-9_.-]+)\s+department", q)
    if m and "department" in columns:
        return "department", [m.group(1).strip()]

    # Pattern: in department value
    m = re.search(r"in\s+([a-z0-9_]+)\s+([a-z0-9_.-]+)", q)
    if m:
        dim = _best_column(m.group(1).strip(), columns)
        val = m.group(2).strip()
        if dim:
            return dim, [val]

    # Pattern: such as val1, val2
    vals = _extract_list_values(q)
    if vals and "department" in columns:
        return "department", vals

    return None, []


def _extract_id_values(query: str) -> List[str]:
    """
    Extract identifier-like tokens from query, e.g. EMP-123ABC.
    """
    ids = re.findall(r"\b[A-Za-z]{2,}-[A-Za-z0-9]+\b", query)
    # Keep original text values for display; match case-insensitive later.
    out: List[str] = []
    seen = set()
    for i in ids:
        k = i.lower()
        if k not in seen:
            seen.add(k)
            out.append(i)
    return out[:20]


def _choose_id_column(columns: List[str]) -> Optional[str]:
    priority = [
        "employee_id",
        "candidate_id",
        "user_id",
        "customer_id",
        "id",
    ]
    for p in priority:
        for c in columns:
            if _norm(c) == _norm(p):
                return c
    for c in columns:
        if c.endswith("_id") or c == "id":
            return c
    return None


def _extract_rank_request(query: str) -> Tuple[Optional[str], Optional[int]]:
    q = query.lower()
    m = re.search(r"\b(top|bottom)\s+(\d+)\b", q)
    if not m:
        return None, None
    side = m.group(1)
    n = int(m.group(2))
    if n <= 0:
        return None, None
    return side, min(n, 20)


def _extract_rank_metric(query: str, kpis: List[str]) -> Optional[str]:
    q = query.lower()
    # try explicit metric words first
    for term in ["salary", "performance", "performance_score", "revenue", "profit", "cost", "amount"]:
        if term in q:
            mapped = _best_column(term, kpis)
            if mapped:
                return mapped
    # fallback to direct kpi mention
    for k in kpis:
        if _norm(k) in _norm(q):
            return k
    return None


def _extract_department_list(query: str) -> List[str]:
    q = query.lower()
    # "in accounts , engg , hr department"
    m = re.search(r"\bin\s+(.+?)\s+department", q)
    if not m:
        m = re.search(r"\bin\s+(.+?)\s+departments", q)
    if not m:
        vals = _extract_list_values(q)
        if vals:
            return vals
        # Generic pattern: "in a, b, c"
        m2 = re.search(r"\bin\s+([a-z0-9_ ,.-]+)$", q)
        if m2 and "," in m2.group(1):
            raw2 = m2.group(1).replace(" and ", ",")
            vals2 = [v.strip(" .\"'") for v in raw2.split(",") if v.strip(" .\"'")]
            return vals2[:20]
        return []

    raw = m.group(1)
    raw = raw.replace("respectively", "")
    raw = raw.replace(" and ", ",")
    vals = [v.strip(" .\"'") for v in raw.split(",") if v.strip(" .\"'")]
    return vals[:20]


def _match_dim_values(series: pd.Series, wanted_values: List[str]) -> pd.Series:
    s_norm = _norm_series(series)
    wanted_norm = [_norm(v) for v in wanted_values if v]
    if not wanted_norm:
        return pd.Series([True] * len(series), index=series.index)

    matched = pd.Series([False] * len(series), index=series.index)
    for w in wanted_norm:
        hit = (s_norm == w) | s_norm.str.contains(w, regex=False) | pd.Series([w in x for x in s_norm], index=series.index)
        matched = matched | hit
    return matched


def _pick_metric_from_query(query: str, kpis: List[str], fallback: Optional[str]) -> Optional[str]:
    q = query.lower()
    # direct mention
    for k in kpis:
        if _norm(k) in _norm(q):
            return k
    for term in ["salary", "performance", "revenue", "profit", "cost", "amount"]:
        if term in q:
            mapped = _best_column(term, kpis)
            if mapped:
                return mapped
    return fallback or (kpis[0] if kpis else None)


def answer_data_question(
    query: str,
    df: pd.DataFrame,
    profile: Dict[str, Dict[str, Any]],
    context: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Deterministic QA for SQL-like dataset questions.
    Returns None when query is not recognized by this layer.
    """
    q = (query or "").strip().lower()
    if not q:
        return None

    columns = list(df.columns)
    kpis = _kpi_columns(profile)

    # 1) "how many entries in salary column"
    m = re.search(r"how many (entries|records|values).*(?:in|for)\s+(.+?)\s+column", q)
    if m:
        col_term = m.group(2)
        col = _best_column(col_term, columns)
        if not col:
            return {"answer": f"I could not find a column matching '{col_term}'."}
        count = int(df[col].notna().sum())
        return {"answer": f"There are {count:,} non-null entries in '{col}'."}

    # 2) Top-N / Bottom-N ranking (optionally per department list)
    side, n = _extract_rank_request(q)
    if side and n:
        metric_col = _extract_rank_metric(q, kpis)
        if metric_col:
            id_col = _choose_id_column(columns) or _entity_dimension(q, columns, profile)
            if not id_col:
                return {"answer": "I could not find an entity column for ranking."}

            temp = df[[id_col, metric_col]].copy()
            temp[metric_col] = _safe_numeric(temp[metric_col])
            temp = temp.dropna(subset=[id_col, metric_col])
            if temp.empty:
                return {"answer": f"There is not enough valid data in '{metric_col}' to rank."}

            # Optional department filter with per-department ranking
            dept_col = "department" if "department" in columns else None
            dept_vals = _extract_department_list(q)
            rows = []
            if dept_col and dept_vals:
                temp = temp.join(df[[dept_col]])
                mask = _match_dim_values(temp[dept_col], dept_vals)
                temp = temp[mask]
                if temp.empty:
                    return {"answer": "I could not match those department values."}

                for dep in dept_vals:
                    dep_mask = _match_dim_values(temp[dept_col], [dep])
                    sub = temp[dep_mask].copy()
                    if sub.empty:
                        continue
                    grouped = sub.groupby(id_col)[metric_col].mean()
                    ranked = grouped.sort_values(ascending=(side == "bottom")).head(n)
                    for idx, val in ranked.items():
                        rows.append(
                            {
                                "department": dep,
                                id_col: str(idx),
                                metric_col: float(val),
                            }
                        )
            else:
                grouped = temp.groupby(id_col)[metric_col].mean()
                ranked = grouped.sort_values(ascending=(side == "bottom")).head(n)
                rows = [{id_col: str(idx), metric_col: float(val)} for idx, val in ranked.items()]

            if not rows:
                return {"answer": "No ranked results could be computed."}

            metric_label = metric_col.replace("_", " ")
            side_word = "top" if side == "top" else "bottom"
            if dept_col and dept_vals:
                answer = (
                    f"I found the {side_word} {n} people by {metric_label} "
                    f"for each requested department."
                )
                x = [f"{r.get('department')} | {r.get(id_col, '')}" for r in rows]
            else:
                answer = f"I found the {side_word} {n} people by {metric_label}."
                x = [str(r.get(id_col, "")) for r in rows]

            chart = {
                "kpi": metric_col.replace("_", " ").title(),
                "type": "bar",
                "spec": {
                    "data": [
                        {
                            "x": x,
                            "y": [float(r.get(metric_col, 0.0)) for r in rows],
                            "type": "bar",
                        }
                    ],
                    "layout": {
                        "title": f"{side_word.title()} {n} by {metric_col}",
                        "xaxis": {"title": id_col},
                        "yaxis": {"title": metric_col},
                    },
                },
            }
            return {
                "answer": answer,
                "table": rows,
                "charts": [chart],
                "context": {"last_metric": metric_col, "last_table": rows, "id_col": id_col},
            }

    # 3) Chart between IDs / selected IDs
    explicit_ids = _extract_id_values(query)
    if "chart" in q and (
        "employee id" in q
        or "candidate id" in q
        or "user id" in q
        or "ids" in q
        or (len(explicit_ids) >= 2 and "between" in q)
    ):
        id_col = _choose_id_column(columns)
        if not id_col:
            return {"answer": "I could not find an ID column to build this chart."}

        metric_col = _pick_metric_from_query(q, kpis, fallback=(context or {}).get("last_metric"))
        if not metric_col:
            return {"answer": "I could not find a metric column for this chart."}

        temp = df[[id_col, metric_col]].copy()
        temp[metric_col] = _safe_numeric(temp[metric_col])
        temp = temp.dropna(subset=[id_col, metric_col])

        # Optional department/value filter.
        dim_filter_col, filter_vals = _extract_subquery_filter(q, columns)
        if dim_filter_col and filter_vals and dim_filter_col in df.columns:
            temp = temp.join(df[[dim_filter_col]])
            wanted = {_norm(v) for v in filter_vals}
            temp = temp[_norm_series(temp[dim_filter_col]).isin(wanted)]

        id_values = explicit_ids
        if not id_values and "these" in q and (context or {}).get("last_table"):
            # Follow-up query support: use IDs from last answer table.
            last_table = (context or {}).get("last_table") or []
            id_values = [str(r.get(id_col, "")) for r in last_table if r.get(id_col)]

        if id_values:
            wanted = {_norm(v) for v in id_values}
            temp = temp[_norm_series(temp[id_col]).isin(wanted)]

        if temp.empty:
            return {"answer": "I could not find matching rows for those IDs and filters."}

        grouped = temp.groupby(id_col)[metric_col].mean().sort_values(ascending=False).head(12)
        table = grouped.reset_index().to_dict(orient="records")
        chart = {
            "kpi": metric_col.replace("_", " ").title(),
            "type": "bar",
            "spec": {
                "data": [
                    {
                        "x": [str(x) for x in grouped.index.tolist()],
                        "y": [float(y) for y in grouped.values.tolist()],
                        "type": "bar",
                    }
                ],
                "layout": {
                    "title": f"{metric_col.replace('_', ' ').title()} by {id_col}",
                    "xaxis": {"title": id_col},
                    "yaxis": {"title": metric_col},
                },
            },
        }
        return {
            "answer": f"I generated a comparison chart for {len(table)} IDs using {metric_col}.",
            "table": table,
            "charts": [chart],
            "context": {"last_metric": metric_col, "last_table": table, "id_col": id_col},
        }

    # 4) Highest/lowest/best/worst metric queries
    m = re.search(
        r"(?:who|which[ a-z0-9_/\-]*)\s+has\s+(the\s+)?(highest|lowest|best|worst)\s+([a-z0-9_ ?.,\"']+)",
        q,
    )
    if m:
        direction = m.group(2)
        metric_term = m.group(3).strip(" ?.,\"'")
        # Remove trailing qualifier clauses before metric mapping.
        metric_term = re.split(r"\b(across|among|for|in)\b", metric_term)[0].strip()
        metric_col = _best_column(metric_term, kpis or columns)
        if not metric_col:
            return {"answer": f"I could not map '{metric_term}' to a metric column."}

        dim_col = _entity_dimension(q, columns, profile)
        if not dim_col:
            return {"answer": f"I found metric '{metric_col}', but no grouping column to answer who/which."}

        temp = df[[metric_col, dim_col]].copy()
        temp[metric_col] = _safe_numeric(temp[metric_col])
        temp = temp.dropna(subset=[metric_col, dim_col])
        if temp.empty:
            return {"answer": f"There is not enough valid data in '{metric_col}' to answer this."}

        # Optional subquery filtering: "in 'accounts' department", "such as engg, accounts"
        filt_col, filt_vals = _extract_subquery_filter(q, columns)
        if filt_col and filt_vals and filt_col in df.columns:
            if filt_col not in temp.columns:
                temp = temp.join(df[[filt_col]])
            wanted = {_norm(v) for v in filt_vals}
            temp = temp[_norm_series(temp[filt_col]).isin(wanted)]
            if temp.empty:
                return {"answer": f"I could not match those filter values in '{filt_col}'."}

        grouped = temp.groupby(dim_col)[metric_col].mean()
        if grouped.empty:
            return {"answer": "No grouped result could be computed."}

        want_max = direction in {"highest", "best"}
        target = grouped.idxmax() if want_max else grouped.idxmin()
        value = float(grouped.loc[target])
        adjective = "highest" if want_max else "lowest"
        return {
            "answer": f"{target} has the {adjective} {metric_col} (average {value:.2f}).",
            "table": grouped.sort_values(ascending=not want_max).head(10).reset_index().to_dict(orient="records"),
            "context": {
                "last_metric": metric_col,
                "last_table": grouped.sort_values(ascending=not want_max).head(10).reset_index().to_dict(orient="records"),
                "id_col": dim_col,
            },
        }

    return None
