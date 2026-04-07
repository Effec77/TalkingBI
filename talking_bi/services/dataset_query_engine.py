from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

MAX_WORKING_ROWS = 300_000


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
    if t.endswith("ies"):
        t = t[:-3] + "y"
    elif t.endswith("s"):
        t = t[:-1]
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


def _working_df(df: pd.DataFrame) -> pd.DataFrame:
    if len(df) <= MAX_WORKING_ROWS:
        return df
    return df.sample(n=MAX_WORKING_ROWS, random_state=42).sort_index()


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
    for term in ["salary", "salaries", "performance", "performances", "performance_score", "revenue", "profit", "cost", "amount"]:
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
            raw2 = (
                m2.group(1)
                .replace("respectively", "")
                .replace("respecitvely", "")
                .replace("respective", "")
                .replace(" and ", ",")
            )
            vals2 = [v.strip(" .\"'") for v in raw2.split(",") if v.strip(" .\"'")]
            return vals2[:20]
        return []

    raw = m.group(1)
    raw = raw.replace("respectively", "").replace("respecitvely", "").replace("respective", "")
    raw = raw.replace(" and ", ",")
    vals = [v.strip(" .\"'") for v in raw.split(",") if v.strip(" .\"'")]
    return vals[:20]


def _extract_in_list_values(query: str) -> List[str]:
    """
    Parse generic filters like:
    - in engg, mktg, sales respectively
    - in engg and mktg
    """
    q = query.lower()
    m = re.search(r"\bin\s+([a-z0-9_ ,.\-]+)", q)
    if not m:
        return []
    raw = m.group(1)
    raw = re.split(r"\b(department|departments|team|teams|where|for|with)\b", raw)[0]
    raw = raw.replace("respectively", "").replace("respecitvely", "").replace("respective", "")
    raw = raw.replace(" and ", ",")
    vals = [v.strip(" .\"'") for v in raw.split(",") if v.strip(" .\"'")]
    return vals[:20]


def _infer_dimension_for_values(df: pd.DataFrame, profile: Dict[str, Dict[str, Any]], values: List[str]) -> Optional[str]:
    wanted = [_norm(v) for v in values if _norm(v)]
    if not wanted:
        return None
    dims = _dimension_columns(profile)
    best_col: Optional[str] = None
    best_hits = 0
    for d in dims:
        if d not in df.columns:
            continue
        s = _norm_series(df[d].dropna())
        if s.empty:
            continue
        hits = 0
        for w in wanted:
            hits += int(((s == w) | s.str.contains(w, regex=False) | pd.Series([w in x for x in s], index=s.index)).sum())
        if hits > best_hits:
            best_hits = hits
            best_col = d
    return best_col if best_hits > 0 else None


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
    for term in ["salary", "salaries", "performance", "revenue", "profit", "cost", "amount", "metric", "value"]:
        if term in q:
            mapped = _best_column(term, kpis)
            if mapped:
                return mapped
    return fallback or (kpis[0] if kpis else None)


def _infer_dimension_for_value(df: pd.DataFrame, profile: Dict[str, Dict[str, Any]], value: str) -> Optional[str]:
    wanted = _norm(value)
    if not wanted:
        return None
    dims = _dimension_columns(profile)
    best_col: Optional[str] = None
    best_hits = 0
    for d in dims:
        if d not in df.columns:
            continue
        s = _norm_series(df[d].dropna())
        if s.empty:
            continue
        hits = int(((s == wanted) | s.str.contains(wanted, regex=False) | pd.Series([wanted in x for x in s], index=s.index)).sum())
        if hits > best_hits:
            best_hits = hits
            best_col = d
    return best_col if best_hits > 0 else None


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

    df_work = _working_df(df)
    columns = list(df_work.columns)
    kpis = _kpi_columns(profile)
    metric_candidates = list(kpis)
    if not metric_candidates:
        # Fallback for cases where DIL KPI tagging is conservative/missed.
        metric_candidates = [c for c in df_work.columns if pd.api.types.is_numeric_dtype(df_work[c])]

    # 0) Generic dimension cardinality and value listing
    if any(term in q for term in ["how many unique", "how many distinct", "how many", "list all", "show all"]):
        dims = _dimension_columns(profile)
        target_dim = None
        for d in dims:
            dn = _norm(d)
            if dn in _norm(q) or dn.rstrip("s") in _norm(q):
                target_dim = d
                break
        if target_dim and target_dim in df_work.columns:
            vals = (
                df_work[target_dim]
                .dropna()
                .astype(str)
                .str.strip()
            )
            vals = vals[vals != ""]
            unique_vals = sorted(vals.drop_duplicates().tolist(), key=lambda x: x.lower())
            if "how many" in q or "count" in q:
                return {"answer": f"There are {len(unique_vals)} unique values in {target_dim}."}
            preview = ", ".join(unique_vals[:40])
            suffix = ", ..." if len(unique_vals) > 40 else ""
            return {"answer": f"Values in {target_dim} ({len(unique_vals)}): {preview}{suffix}."}

    # 1) "how many entries in salary column"
    m = re.search(r"how many (entries|records|values).*(?:in|for)\s+(.+?)\s+column", q)
    if m:
        col_term = m.group(2)
        col = _best_column(col_term, columns)
        if not col:
            return {"answer": f"I could not find a column matching '{col_term}'."}
        count = int(df_work[col].notna().sum())
        return {"answer": f"There are {count:,} non-null entries in '{col}'."}

    # 2) Top-N / Bottom-N ranking (optionally per department list)
    side, n = _extract_rank_request(q)
    if side and n:
        metric_col = _extract_rank_metric(q, metric_candidates)
        if metric_col:
            id_col = _choose_id_column(columns) or _entity_dimension(q, columns, profile)
            if not id_col:
                return {"answer": "I could not find an entity column for ranking."}

            temp = df_work[[id_col, metric_col]].copy()
            temp[metric_col] = _safe_numeric(temp[metric_col])
            temp = temp.dropna(subset=[id_col, metric_col])
            if temp.empty:
                return {"answer": f"There is not enough valid data in '{metric_col}' to rank."}

            # Optional department filter with per-department ranking
            dept_col = "department" if "department" in columns else None
            dept_vals = _extract_department_list(q)
            if not dept_vals:
                dept_vals = _extract_in_list_values(q)
            if dept_vals and not dept_col:
                dept_col = _infer_dimension_for_values(df_work, profile, dept_vals)
            rows = []
            if dept_col and dept_vals:
                temp = temp.join(df_work[[dept_col]])
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

        temp = df_work[[id_col, metric_col]].copy()
        temp[metric_col] = _safe_numeric(temp[metric_col])
        temp = temp.dropna(subset=[id_col, metric_col])

        # Optional department/value filter.
        dim_filter_col, filter_vals = _extract_subquery_filter(q, columns)
        if dim_filter_col and filter_vals and dim_filter_col in df_work.columns:
            temp = temp.join(df_work[[dim_filter_col]])
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

    # 3.5) Segment metric query: "show salaries in finance", "show salary in accounts department"
    if any(w in q for w in ["show", "display", "list", "give"]) and kpis:
        metric_col = _pick_metric_from_query(q, metric_candidates, fallback=None)
        if metric_col and any(w in q for w in [" in ", " where ", " for ", " by "]):
            temp = df_work.copy()
            temp[metric_col] = _safe_numeric(temp[metric_col])
            temp = temp.dropna(subset=[metric_col])
            if temp.empty:
                return {"answer": f"I could not find valid values for '{metric_col}'."}

            filt_col, filt_vals = _extract_subquery_filter(q, columns)
            if not filt_col:
                m = re.search(r"\bin\s+([a-z0-9_.\- ]+)$", q)
                if m:
                    raw_val = m.group(1).strip(" .\"'")
                    raw_val = re.sub(r"\b(department|team|region|location|office)\b", "", raw_val).strip()
                    if raw_val:
                        inferred_dim = _infer_dimension_for_value(df_work, profile, raw_val)
                        if inferred_dim:
                            filt_col, filt_vals = inferred_dim, [raw_val]
            if not filt_col:
                # Support phrasing like: "show salary by finance"
                m = re.search(r"\bby\s+([a-z0-9_.\- ]+)$", q)
                if m:
                    raw_val = m.group(1).strip(" .\"'")
                    # If query already contains explicit dimension phrase ("by department"), skip.
                    if raw_val and not any(
                        _norm(raw_val) == _norm(dim_name) for dim_name in _dimension_columns(profile)
                    ):
                        inferred_dim = _infer_dimension_for_value(df_work, profile, raw_val)
                        if inferred_dim:
                            filt_col, filt_vals = inferred_dim, [raw_val]

            if filt_col and filt_vals and filt_col in temp.columns:
                mask = _match_dim_values(temp[filt_col], filt_vals)
                temp = temp[mask]
                if temp.empty:
                    return {"answer": f"I could not find rows where {filt_col} matches {', '.join(filt_vals)}."}
            else:
                return None

            group_col = filt_col
            id_col = _choose_id_column(columns)
            if id_col and id_col in temp.columns:
                grouped = temp.groupby(id_col)[metric_col].mean().sort_values(ascending=False).head(20)
                table = grouped.reset_index().to_dict(orient="records")
                chart_x = [str(x) for x in grouped.index.tolist()]
                chart_y = [float(y) for y in grouped.values.tolist()]
                x_label = id_col
            else:
                grouped = temp.groupby(group_col)[metric_col].mean().sort_values(ascending=False).head(20)
                table = grouped.reset_index().to_dict(orient="records")
                chart_x = [str(x) for x in grouped.index.tolist()]
                chart_y = [float(y) for y in grouped.values.tolist()]
                x_label = group_col

            chart = {
                "kpi": metric_col.replace("_", " ").title(),
                "type": "bar",
                "x": chart_x,
                "y": chart_y,
                "title": f"{metric_col.replace('_', ' ').title()} in {', '.join(filt_vals)}",
            }
            return {
                "answer": f"Here is {metric_col.replace('_', ' ')} for {', '.join(filt_vals)}.",
                "table": table,
                "charts": [chart],
                "context": {"last_metric": metric_col, "last_table": table, "id_col": x_label},
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

        temp = df_work[[metric_col, dim_col]].copy()
        temp[metric_col] = _safe_numeric(temp[metric_col])
        temp = temp.dropna(subset=[metric_col, dim_col])
        if temp.empty:
            return {"answer": f"There is not enough valid data in '{metric_col}' to answer this."}

        # Optional subquery filtering: "in 'accounts' department", "such as engg, accounts"
        filt_col, filt_vals = _extract_subquery_filter(q, columns)
        if filt_col and filt_vals and filt_col in df_work.columns:
            if filt_col not in temp.columns:
                temp = temp.join(df_work[[filt_col]])
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
