"""
LLM Client - Gemini Integration
Phase 0B.3 - KPI Selection
"""
import os
import json
from typing import List, Dict
from dotenv import load_dotenv

load_dotenv()

# Gemini API configuration
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-pro")


def select_kpis_with_llm(candidates: List[Dict], dataset_context: Dict) -> List[Dict]:
    """
    Use Gemini LLM to select EXACTLY 3 KPIs from candidates.
    
    Args:
        candidates: List of KPI candidate dictionaries
        dataset_context: Context about the dataset
        
    Returns:
        List of exactly 3 selected KPIs
    """
    print(f"[LLM] Selecting KPIs from {len(candidates)} candidates using {GEMINI_MODEL}")
    
    if not GEMINI_API_KEY:
        print("[LLM] Warning: GEMINI_API_KEY not set, using fallback selection")
        return _fallback_kpi_selection(candidates)
    
    try:
        import google.generativeai as genai
        
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel(GEMINI_MODEL)
        
        # Build prompt
        prompt = _build_kpi_selection_prompt(candidates, dataset_context)
        
        # Call LLM
        print(f"[LLM] Calling Gemini API...")
        response = model.generate_content(prompt)
        
        # Parse response
        kpis = _parse_kpi_response(response.text)
        
        print(f"[LLM] Successfully selected {len(kpis)} KPIs via Gemini")
        return kpis
        
    except ImportError:
        print("[LLM] Error: google-generativeai not installed, using fallback")
        return _fallback_kpi_selection(candidates)
    except Exception as e:
        print(f"[LLM] Error calling Gemini: {e}")
        print(f"[LLM] Using fallback selection")
        return _fallback_kpi_selection(candidates)


def _build_kpi_selection_prompt(candidates: List[Dict], context: Dict) -> str:
    """Build prompt for KPI selection"""
    
    prompt = f"""You are a business intelligence expert. Select EXACTLY 3 KPIs from the following candidates.

Dataset Context:
- Filename: {context.get('filename', 'unknown')}
- Rows: {context.get('rows', 0)}
- Columns: {context.get('columns', [])}

KPI Candidates:
"""
    
    for i, candidate in enumerate(candidates, 1):
        prompt += f"\n{i}. Column: {candidate['column']}"
        prompt += f"\n   - Type: {candidate['dtype']}"
        prompt += f"\n   - Cardinality: {candidate['cardinality']}"
        prompt += f"\n   - Missing: {candidate['missing_pct']:.1%}"
        prompt += f"\n   - Aggregations: {', '.join(candidate['aggregations'])}"
        if candidate.get('segment_by_options'):
            prompt += f"\n   - Segment by: {', '.join(candidate['segment_by_options'])}"
        if candidate.get('time_column_options'):
            prompt += f"\n   - Time columns: {', '.join(candidate['time_column_options'])}"
    
    prompt += """

Requirements:
1. Select EXACTLY 3 KPIs
2. Choose KPIs with semantic diversity (different business aspects)
3. Prefer KPIs that can be segmented or trended over time
4. Provide business meaning for each KPI

Output Format (JSON):
[
  {
    "name": "descriptive KPI name",
    "source_column": "column name from candidates",
    "aggregation": "sum|avg|count|min|max",
    "segment_by": "categorical column or null",
    "time_column": "datetime column or null",
    "business_meaning": "what this KPI measures",
    "confidence": 0.0-1.0
  }
]

Return ONLY the JSON array, no other text.
"""
    
    return prompt


def _parse_kpi_response(response_text: str) -> List[Dict]:
    """Parse LLM response into KPI list"""
    try:
        # Extract JSON from response
        start = response_text.find('[')
        end = response_text.rfind(']') + 1
        
        if start == -1 or end == 0:
            raise ValueError("No JSON array found in response")
        
        json_str = response_text[start:end]
        kpis = json.loads(json_str)
        
        # Validate we have exactly 3 KPIs
        if len(kpis) != 3:
            print(f"[LLM] Warning: Expected 3 KPIs, got {len(kpis)}")
            kpis = kpis[:3]  # Take first 3
        
        return kpis
        
    except Exception as e:
        print(f"[LLM] Error parsing response: {e}")
        raise


def _fallback_kpi_selection(candidates: List[Dict]) -> List[Dict]:
    """
    Fallback KPI selection when LLM is unavailable.
    Simply takes first 3 valid numeric columns.
    """
    print("[LLM] Using fallback KPI selection")
    
    kpis = []
    for candidate in candidates[:3]:
        kpi = {
            "name": f"{candidate['column'].replace('_', ' ').title()}",
            "source_column": candidate['column'],
            "aggregation": "sum",
            "segment_by": candidate.get('segment_by_options', [None])[0],
            "time_column": candidate.get('time_column_options', [None])[0],
            "business_meaning": f"Total {candidate['column']}",
            "confidence": 0.5
        }
        kpis.append(kpi)
    
    return kpis
