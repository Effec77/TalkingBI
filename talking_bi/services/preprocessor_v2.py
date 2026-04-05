from typing import Dict, Any, List
import pandas as pd
from services.nlp_normalizer import correct_tokens

def build_vocab(df: pd.DataFrame, profile: Dict[str, Dict[str, Any]]) -> List[str]:
    vocab = set(df.columns)
    # Add common analytics terms to vocabulary
    vocab.update(["show", "compare", "trend", "trends", "filter", "by", "for", "with", "only", "null", "not"])
    
    # Add some sample values for categorical dimensions to assist spelling
    for col, meta in profile.items():
        if meta.get("role_scores", {}).get("is_dimension") == 1.0:
            for val in meta.get("sample_values", []):
                if isinstance(val, str) and len(val.split()) == 1:
                    vocab.add(val.lower())
                    
    return list(vocab)

def inject_kpi(query: str, profile: Dict[str, Dict[str, Any]]) -> str:
    # 9C.3 Rule: ONLY inject KPI if exactly one strong candidate exists
    strong_kpis = [col for col, meta in profile.items() 
                   if meta.get("role_scores", {}).get("is_kpi") == 1.0]
    
    if len(strong_kpis) != 1:
        return query
        
    kpi = strong_kpis[0]
    
    # Check if a KPI is already mentioned or if it's already an explicit intent
    lower_query = query.lower()
    
    # Do not inject if user is explicitly doing something else or context might be used
    # e.g., "by region" might inherit context in 9C.2. Adding KPI overrides context.
    # The rule is: DO NOT override ambiguous queries, DO NOT remove user intent.
    # Therefore, if the query is just a single word dimension, injecting KPI might be safe but wait, 
    # the 6G deterministic override handles "by region" + context gracefully.
    # To keep it safe, we only inject if the query is practically a single term that is just a dimension (with no context),
    # but the instructions suggest to just inject it.
    
    # Let's add it carefully if query lacks *any* kpi candidate
    # But wait, we shouldn't force inject if we don't know the intent.
    # Instructions: "ONLY inject KPI if exactly one strong candidate exists. DO NOT remove user intent"
    # A safe injection: if "show" is there but no KPI is mentioned.
    if "show" in lower_query and kpi.lower() not in lower_query:
        # Check if no other KPI is in query
        all_numeric = [col for col, meta in profile.items() if meta["semantic_type"] == "kpi"]
        has_kpi = any(x.lower() in lower_query for x in all_numeric)
        if not has_kpi:
            # Inject kpi safely immediately after "show"
            return query.replace("show", f"show {kpi}")
            
    return query

def infer_time(query: str, profile: Dict[str, Dict[str, Any]]) -> str:
    lower_query = query.lower()
    
    # If looking for a trend
    if "trend" in lower_query:
        # Check if we have a date column
        date_cols = [col for col, meta in profile.items() 
                     if meta.get("role_scores", {}).get("is_date") == 1.0]
        if date_cols:
            date_col = date_cols[0]
            if "over time" not in lower_query and date_col.lower() not in lower_query:
                # Add explicit grouping by the date column
                return f"{query} by {date_col}"
                
    return query

def preprocess_v2(query: str, df: pd.DataFrame, profile: Dict[str, Dict[str, Any]]) -> str:
    vocab = build_vocab(df, profile)
    
    clean_query = correct_tokens(query, vocab)
    clean_query = inject_kpi(clean_query, profile)
    clean_query = infer_time(clean_query, profile)
    
    return clean_query
