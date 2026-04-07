"""
Phase 6F: Schema Intelligence Layer (DIL-Aware)

Bridges user language with dataset schema using deterministic,
score-based multi-signal evaluation (Rapidfuzz + Dataset Intelligence profile).

Resolution order: Score-based ranking.
"""

import re
from typing import Dict, List, Optional, Any, Tuple
from rapidfuzz import fuzz
from services.dataset_intelligence import DatasetIntelligence


class SchemaMapper:
    def __init__(self, df, kpi_candidates: List[Dict]):
        """
        Initialize SchemaMapper with dataframe.
        Leverages DatasetIntelligence directly to dynamically build profiles.
        """
        self.df = df
        self.kpi_candidates = kpi_candidates
        # Extract columns
        self.columns = list(df.columns)
        
        # Build comprehensive semantic profile dynamically
        # Enables us to rely on role_scores and semantic_types
        self.profile = DatasetIntelligence(df).build()
        
    def _compute_score(self, query_term: str, col_name: str, mode: str) -> float:
        # FIX 2: Normalize Column Name (LIGHTWEIGHT)
        def normalize_col(name):
            return re.sub(r'[^a-zA-Z0-9 ]', ' ', str(name)).lower().strip()
            
        query_normalized = normalize_col(query_term)
        col_normalized = normalize_col(col_name)

        # A. Name Similarity
        # Basic fuzzy ratio
        similarity = fuzz.ratio(query_normalized, col_normalized) / 100.0
        
        # Hard exact match override
        if query_normalized == col_normalized or query_normalized == col_normalized.replace(' ', ''):
            similarity = 1.0
        
        # B. Semantic Match
        semantic_type = self.profile[col_name].get("semantic_type", "unknown")
        role_scores = self.profile[col_name].get("role_scores", {})
        
        semantic_match = 0.0
        role_score = 0.0
        
        if mode == "kpi":
            if semantic_type == "kpi":
                semantic_match = 1.0
            role_score = role_scores.get("is_kpi", 0.0)
            
        elif mode == "dimension":
            if semantic_type in ["dimension", "date"]:
                semantic_match = 1.0
            role_score = role_scores.get("is_dimension", 0.0)
            
            # Identifier penalty
            if semantic_type == "identifier":
                role_score -= 0.5  # Penalize choosing ID as group-by
            
        score = (similarity * 0.4) + (semantic_match * 0.3) + (role_score * 0.3)
        return score

    def map_kpi(self, user_term: str) -> Tuple[Any, str]:
        if not user_term:
            return None, "no_term"
            
        scores = {}
        for col in self.columns:
            scores[col] = self._compute_score(user_term, col, mode="kpi")
            
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        top_col, top_score = ranked[0]
        
        print(f"[6F] KPI Resolve '{user_term}' -> Scores: {[(c, round(s, 2)) for c, s in ranked[:3]]}")
        
        if len(ranked) > 1:
            diff = ranked[0][1] - ranked[1][1]
            # FIX 3: Confidence-Based Soft Resolution
            # If top score is very high, trust it even with close neighbors
            if top_score > 0.85:
                return top_col, "resolved"
                
            if diff < 0.1 and top_score > 0.4:
                # FIX 1: Tie-breaker Logic
                # Prefer candidate with higher is_kpi role score or cleaner name
                c1, s1 = ranked[0]
                c2, s2 = ranked[1]
                
                role1 = self.profile[c1].get("role_scores", {}).get("is_kpi", 0.0)
                role2 = self.profile[c2].get("role_scores", {}).get("is_kpi", 0.0)
                
                if role1 > role2 + 0.1:
                    return c1, "resolved"
                
                # Check for "cleaner" name (less symbols/numbers)
                clean1 = len(re.sub(r'[^a-zA-Z]', '', c1)) / len(c1) if len(c1) > 0 else 0
                clean2 = len(re.sub(r'[^a-zA-Z]', '', c2)) / len(c2) if len(c2) > 0 else 0
                
                if clean1 > clean2 + 0.2:
                    return c1, "resolved"

                print(f"[6F] AMBIGUOUS KPI: '{user_term}' between {c1} and {c2}")
                return [r[0] for r in ranked[:3]], "ambiguous"
        
        if top_score < 0.4:
            return None, "unmapped"
            
        return top_col, "resolved"

    def map_dimension(self, user_term: str) -> Tuple[Any, str]:
        if not user_term:
            return None, "no_term"
            
        scores = {}
        for col in self.columns:
            scores[col] = self._compute_score(user_term, col, mode="dimension")
            
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        top_col, top_score = ranked[0]
        
        print(f"[6F] DIM Resolve '{user_term}' -> Scores: {[(c, round(s, 2)) for c, s in ranked[:3]]}")
        
        # Block invalid groupings explicitly
        if self.profile[top_col].get("semantic_type") == "identifier":
            print(f"[6F] INVALID_GROUPBY detected: '{user_term}' maps to identifier '{top_col}'")
            return None, "invalid_groupby"

        # Ambiguity detection
        if len(ranked) > 1:
            diff = ranked[0][1] - ranked[1][1]
            # FIX 3: Soft Resolution
            if top_score > 0.85:
                return top_col, "resolved"

            if diff < 0.1 and top_score > 0.4:
                # FIX 1: Tie-breaker (Dimension)
                c1, s1 = ranked[0]
                c2, s2 = ranked[1]
                
                role1 = self.profile[c1].get("role_scores", {}).get("is_dimension", 0.0)
                role2 = self.profile[c2].get("role_scores", {}).get("is_dimension", 0.0)
                
                if role1 > role2 + 0.1:
                    return c1, "resolved"

                print(f"[6F] AMBIGUOUS DIMENSION: '{user_term}' between {c1} and {c2}")
                return [r[0] for r in ranked[:3]], "ambiguous"
                
        if top_score < 0.4:
            return None, "unmapped"
            
        return top_col, "resolved"

    def map_intent(self, intent_dict: Dict[str, Any]) -> Dict[str, Any]:
        print(f"[TRACE:SCHEMA] Input: {intent_dict}")
        if intent_dict.get("_locked"):
            print(f"[TRACE:SCHEMA] LOCK ACTIVE: {intent_dict.get('_lock_source')}")
            return intent_dict.copy()

        result = intent_dict.copy()
        mapping_meta = {
            "kpi_source": None,
            "dimension_source": None,
            "filter_source": None,
            "confidence": 1.0,
            "ambiguous_candidates": {}
        }

        # Map KPI
        if intent_dict.get("kpi"):
            raw_kpi = intent_dict["kpi"]
            if isinstance(raw_kpi, str) and raw_kpi in self.columns:
                mapped_kpi, status = raw_kpi, "exact_column"
            else:
                mapped_kpi, status = self.map_kpi(raw_kpi)
            if status == "ambiguous":
                result["kpi"] = None
                mapping_meta["kpi_source"] = status
                mapping_meta["confidence"] = 0.0
                mapping_meta["ambiguous_candidates"]["kpi"] = mapped_kpi # it's the list
            elif mapped_kpi:
                result["kpi"] = mapped_kpi
                mapping_meta["kpi_source"] = status
            else:
                result["kpi"] = None
                mapping_meta["kpi_source"] = status
                mapping_meta["confidence"] = 0.3

        # Map kpi_1 (for COMPARE intent)
        if intent_dict.get("kpi_1"):
            raw_kpi_1 = intent_dict["kpi_1"]
            if isinstance(raw_kpi_1, str) and raw_kpi_1 in self.columns:
                mapped_kpi_1, status = raw_kpi_1, "exact_column"
            else:
                mapped_kpi_1, status = self.map_kpi(raw_kpi_1)
            if status == "ambiguous":
                result["kpi_1"] = None
                mapping_meta["confidence"] = 0.0
                mapping_meta["ambiguous_candidates"]["kpi_1"] = mapped_kpi_1
            elif mapped_kpi_1:
                result["kpi_1"] = mapped_kpi_1
            else:
                result["kpi_1"] = None

        # Map kpi_2 (for COMPARE intent)
        if intent_dict.get("kpi_2"):
            raw_kpi_2 = intent_dict["kpi_2"]
            if isinstance(raw_kpi_2, str) and raw_kpi_2 in self.columns:
                mapped_kpi_2, status = raw_kpi_2, "exact_column"
            else:
                mapped_kpi_2, status = self.map_kpi(raw_kpi_2)
            if status == "ambiguous":
                result["kpi_2"] = None
                mapping_meta["confidence"] = 0.0
                mapping_meta["ambiguous_candidates"]["kpi_2"] = mapped_kpi_2
            elif mapped_kpi_2:
                result["kpi_2"] = mapped_kpi_2
            else:
                result["kpi_2"] = None

        # Map dimension
        if intent_dict.get("dimension"):
            mapped_dim, status = self.map_dimension(intent_dict["dimension"])
            if status == "ambiguous":
                result["dimension"] = None
                mapping_meta["dimension_source"] = status
                mapping_meta["confidence"] = 0.0
                mapping_meta["ambiguous_candidates"]["dimension"] = mapped_dim
            elif mapped_dim:
                result["dimension"] = mapped_dim
                mapping_meta["dimension_source"] = status
            else:
                result["dimension"] = None
                mapping_meta["dimension_source"] = status
                if status == "invalid_groupby":
                    mapping_meta["confidence"] = 0.0
                elif status == "unmapped":
                    mapping_meta["confidence"] = min(mapping_meta["confidence"], 0.3)

        # Filters pass through as values
        if intent_dict.get("filter"):
            mapping_meta["filter_source"] = "user_value"

        result["mapping_meta"] = mapping_meta
        return result

def create_schema_mapper(df, kpi_candidates: List[Dict]) -> SchemaMapper:
    return SchemaMapper(df, kpi_candidates)
