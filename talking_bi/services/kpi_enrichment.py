"""
KPI Enrichment Service - Phase 0B Patch
LLM is ONLY for enrichment, NOT selection
"""
import json
from typing import List, Dict, Optional
import pandas as pd
from services.llm_manager import LLMManager


def enrich_kpis(
    kpi_columns: List[str],
    dataset_context: Dict,
    llm_manager: LLMManager,
    df: Optional[pd.DataFrame] = None
) -> List[Dict]:
    """
    Enrich KPI columns with business meaning using LLM.
    
    CRITICAL: KPIs are already selected by Python.
    LLM only adds business context.
    
    Args:
        kpi_columns: List of 3 column names (from Python selector)
        dataset_context: Dataset metadata
        llm_manager: LLM manager instance
        
    Returns:
        List of 3 enriched KPIs with business meaning
    """
    print(f"[ENRICHMENT] Enriching {len(kpi_columns)} KPIs with LLM")
    
    datetime_cols = dataset_context.get("datetime_columns", [])
    categorical_cols = dataset_context.get("categorical_columns", [])
    prompt_columns = list(kpi_columns[:3])
    while len(prompt_columns) < 3:
        prompt_columns.append("null")

    # Build prompt with strict output constraints.
    prompt = f"""You are a business intelligence expert. Convert these data columns into business KPIs.

Dataset: {dataset_context.get('filename', 'unknown')}
Columns to enrich: {kpi_columns}
Datetime columns: {datetime_cols}
Categorical columns: {categorical_cols}

For each column, provide:
1. A business-friendly name
2. The best aggregation method (sum, count, nunique)
3. A brief business meaning

Return ONLY a JSON array with this exact format:
[
  {{
    "name": "descriptive business name",
    "source_column": "{prompt_columns[0]}",
    "aggregation": "sum|count|nunique",
    "business_meaning": "what this measures in business terms"
  }},
  {{
    "name": "descriptive business name",
    "source_column": "{prompt_columns[1]}",
    "aggregation": "sum|count|nunique",
    "business_meaning": "what this measures in business terms"
  }},
  {{
    "name": "descriptive business name",
    "source_column": "{prompt_columns[2]}",
    "aggregation": "sum|count|nunique",
    "business_meaning": "what this measures in business terms"
  }}
]

Return EXACTLY 3 items.
Return ONLY valid JSON.
No explanations."""
    
    # Try LLM enrichment
    cache_key = f"enrich_{tuple(kpi_columns)}"
    response = llm_manager.call_llm(prompt, cache_key=cache_key)
    
    if response:
        try:
            # Parse JSON response
            enriched = _parse_enrichment_response(response, kpi_columns, dataset_context, df)
            print(f"[ENRICHMENT] OK Successfully enriched with LLM")
            return enriched
        except Exception as e:
            print(f"[ENRICHMENT] ERROR Failed to parse LLM response: {e}")
    
    # Fallback: Basic enrichment without LLM
    print("[ENRICHMENT] Using Python fallback enrichment")
    return _fallback_enrichment(kpi_columns, dataset_context, df)


def _generate_kpi_name(col: str) -> str:
    col_lower = col.lower()
    if "sales" in col_lower or "revenue" in col_lower:
        return "Total Revenue"
    if "quantity" in col_lower or "units" in col_lower:
        return "Units Sold"
    return f"Total {col.replace('_', ' ').title()}"


def _parse_enrichment_response(
    response: str,
    kpi_columns: List[str],
    dataset_context: Dict,
    df: Optional[pd.DataFrame]
) -> List[Dict]:
    """Parse LLM response into enriched KPIs"""
    
    # Extract JSON from response
    start = response.find('[')
    end = response.rfind(']') + 1
    
    if start == -1 or end == 0:
        raise ValueError("No JSON array found in response")
    
    json_str = response[start:end]
    enriched = json.loads(json_str)
    
    if len(enriched) != 3:
        raise ValueError("Invalid LLM output")

    return _validate_and_normalize_kpis(enriched, kpi_columns, dataset_context, df)


def _fallback_enrichment(
    kpi_columns: List[str],
    dataset_context: Dict,
    df: Optional[pd.DataFrame]
) -> List[Dict]:
    """Fallback enrichment without LLM"""
    seed_kpis = [
        {
            "name": _generate_kpi_name(col),
            "source_column": col,
            "aggregation": "sum",
            "business_meaning": f"Total {col.replace('_', ' ')}"
        }
        for col in kpi_columns
    ]
    return _validate_and_normalize_kpis(seed_kpis, kpi_columns, dataset_context, df)


def _validate_and_normalize_kpis(
    candidate_kpis: List[Dict],
    kpi_columns: List[str],
    dataset_context: Dict,
    df: Optional[pd.DataFrame]
) -> List[Dict]:
    numeric_cols = set(dataset_context.get("numeric_columns", []))
    datetime_cols = list(dataset_context.get("datetime_columns", []))
    categorical_cols = list(dataset_context.get("categorical_columns", []))

    normalized: List[Dict] = []
    used_numeric = set()
    weak_meanings = {"total sales", "total quantity"}

    for i, raw in enumerate(candidate_kpis):
        source_column = raw.get("source_column") or (kpi_columns[i] if i < len(kpi_columns) else None)
        aggregation = (raw.get("aggregation") or "sum").lower()

        if aggregation == "sum":
            if not source_column or source_column not in numeric_cols:
                continue
            if source_column in used_numeric:
                continue
            used_numeric.add(source_column)
        elif aggregation == "nunique":
            if not categorical_cols:
                continue
            source_column = source_column if source_column in categorical_cols else categorical_cols[0]
        elif aggregation == "count":
            source_column = None
        else:
            # Unsupported aggregation -> normalize to sum when possible.
            if not source_column or source_column not in numeric_cols:
                continue
            aggregation = "sum"
            if source_column in used_numeric:
                continue
            used_numeric.add(source_column)

        name = raw.get("name") or (_generate_kpi_name(source_column) if source_column else "Total Records")
        if source_column:
            name = _generate_kpi_name(source_column)

        meaning = (raw.get("business_meaning") or "").strip()
        if meaning.lower() in weak_meanings or not meaning:
            meaning = f"Business performance measured by {name.lower()}."

        confidence = 0.8
        if source_column and df is not None and source_column in df.columns:
            confidence = float(1 - df[source_column].isna().mean())

        normalized.append({
            "name": name,
            "source_column": source_column,
            "aggregation": aggregation,
            "segment_by": categorical_cols[0] if categorical_cols else None,
            "time_column": None,
            "business_meaning": meaning,
            "confidence": max(0.0, min(1.0, confidence))
        })

        if len(normalized) == 3:
            break

    # Smart semantic fallback to guarantee exactly 3 KPIs.
    primary_numeric = [col for col in kpi_columns if col in numeric_cols and col not in used_numeric]
    for col in primary_numeric:
        if len(normalized) >= 3:
            break
        confidence = float(1 - df[col].isna().mean()) if df is not None and col in df.columns else 0.8
        normalized.append({
            "name": _generate_kpi_name(col),
            "source_column": col,
            "aggregation": "sum",
            "segment_by": categorical_cols[0] if categorical_cols else None,
            "time_column": None,
            "business_meaning": f"Business performance measured by {_generate_kpi_name(col).lower()}.",
            "confidence": max(0.0, min(1.0, confidence))
        })
        used_numeric.add(col)

    if len(normalized) < 3:
        normalized.append({
            "name": "Total Records",
            "source_column": None,
            "aggregation": "count",
            "segment_by": categorical_cols[0] if categorical_cols else None,
            "time_column": datetime_cols[0] if datetime_cols else None,
            "business_meaning": "Overall size of the dataset used for context and trend comparisons.",
            "confidence": 1.0
        })

    if len(normalized) < 3 and categorical_cols:
        normalized.append({
            "name": "Unique Categories",
            "source_column": categorical_cols[0],
            "aggregation": "nunique",
            "segment_by": None,
            "time_column": None,
            "business_meaning": "Breadth of category diversity in the primary segmentation column.",
            "confidence": 0.9
        })

    # Guaranteed final fallback.
    while len(normalized) < 3:
        normalized.append({
            "name": "Total Records",
            "source_column": None,
            "aggregation": "count",
            "segment_by": None,
            "time_column": datetime_cols[0] if datetime_cols else None,
            "business_meaning": "Overall size of the dataset used for context and trend comparisons.",
            "confidence": 1.0
        })

    normalized = normalized[:3]

    # Soft rule: diversify segment assignment across KPIs when possible.
    used_segments = set()
    for kpi in normalized:
        if categorical_cols:
            for col in categorical_cols:
                if col not in used_segments:
                    kpi["segment_by"] = col
                    used_segments.add(col)
                    break

    # Time-awareness rule: at least one KPI should carry time column when available.
    if datetime_cols and not any(kpi.get("time_column") for kpi in normalized):
        normalized[0]["time_column"] = datetime_cols[0]

    # Final hard validation layer.
    assert len(normalized) == 3
    for kpi in normalized:
        if kpi["aggregation"] == "sum":
            assert kpi["source_column"] in numeric_cols

    return normalized
