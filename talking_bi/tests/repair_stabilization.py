import codecs
import re

# RECOVERY PATCH for Rule 1-6 Stabilization

def patch_schema_mapper():
    print("Repairing schema_mapper.py...")
    path = 'services/schema_mapper.py'
    content = codecs.open(path, 'r', 'utf-8').read()
    
    # Fix corrupted map_kpi indentation and Rule 1 logic
    # Also restore fuzzy match while we are at it per Rule 1 fallback
    
    new_map_kpi = '''    def map_kpi(self, user_term: str) -> Tuple[Optional[str], str]:
        """
        Map user KPI term to schema KPI.
        
        Resolution order:
        1. Exact match (case-insensitive)
        2. Normalized match
        3. Schema map lookup
        4. Target column contains check (fuzzy fallback)
        5. Else -> None
        """
        if not user_term:
            return None, "no_term"

        user_lower = user_term.lower().strip()
        user_normalized = self.normalize(user_term)
        
        # Rule 1: STRICT - if it exists in columns, ALWAYS resolve
        col_lower_map = {c.lower(): c for c in self.columns}
        if user_lower in col_lower_map:
            return col_lower_map[user_lower], "exact_column_match"

        # 1. Exact match against KPI candidates
        for kpi in self.kpi_candidates:
            name = kpi.get("name", "")
            if name.lower() == user_lower:
                return name, "exact_match"

        # 2. Normalized match KPI candidates
        for kpi in self.kpi_candidates:
            name = kpi.get("name", "")
            if self.normalize(name) == user_normalized:
                return name, "normalized_match"

        # 3. Schema map lookup
        normalized_columns = {self.normalize(c): c for c in self.columns}
        for canonical, aliases in self.schema_map.items():
            if user_lower == canonical.lower() or user_lower in [a.lower() for a in aliases]:
                for kpi in self.kpi_candidates:
                    kpi_name = kpi.get("name", "").lower()
                    kpi_source = kpi.get("source_column", "").lower()
                    if kpi_name == canonical.lower() or kpi_source == canonical.lower():
                        return kpi.get("name"), "schema_map"
                    for alias in aliases:
                        if kpi_name == alias.lower() or kpi_source == alias.lower():
                            return kpi.get("name"), "schema_map"
                
                # Check columns directly
                for col_norm, col_raw in normalized_columns.items():
                    if col_norm == canonical.lower() or col_norm in [a.lower() for a in aliases]:
                        return col_raw, "schema_map_column_fallback"
                        
                return canonical, "schema_map_default"

        # 4. Fallback Rule 1: fuzzy contain match
        matches = []
        for col in self.columns:
            if user_lower in col.lower():
                matches.append(col)
        
        if len(matches) == 1:
            return matches[0], "fuzzy_contain_match"
        elif len(matches) > 1:
            return None, "ambiguous"

        return None, "no_match"'''

    # Find the corrupted block and replace it
    # Search from 'def map_kpi' to the next 'def'
    content = re.sub(r'def map_kpi\(self, user_term: str\).*?def map_dimension', new_map_kpi + '\n\n    def map_dimension', content, flags=re.DOTALL)
    
    codecs.open(path, 'w', 'utf-8').write(content)

def patch_orchestrator():
    print("Repairing orchestrator.py...")
    path = 'services/orchestrator.py'
    content = codecs.open(path, 'r', 'utf-8').read()
    
    # Fix Rule 4 logic - ensure detector is called AFTER trend detect, and deterministic_intent is defined
    
    # Locate detector block
    old_block = '''            # Rule 4: Trend detection BEFORE parser
            normalized_query_lower = normalized_query.lower()
            if "trend" in normalized_query_lower or "over time" in normalized_query_lower:
                # Simple rule-based detector
                date_col = None
                from services.dataset_profiler import profile_dataset
                profile = profile_dataset(df)
                if profile.datetime_columns:
                    date_col = profile.datetime_columns[0]
                
                if date_col:
                    print(f"[ORCHESTRATOR] 📈 Trend detected, using dimension: {date_col}")
                    if not deterministic_intent:
                         # Merge with intent
                         if 'intent' in locals() and intent:
                             intent["intent"] = "SEGMENT_BY"
                             intent["dimension"] = date_col
                         elif deterministic_intent:
                             deterministic_intent["intent"] = "SEGMENT_BY"
                             deterministic_intent["dimension"] = date_col
            deterministic_intent = detector.detect(normalized_query)'''

    new_block = '''            # Rule 4: Trend detection
            deterministic_intent = detector.detect(normalized_query)
            
            normalized_query_lower = normalized_query.lower()
            if "trend" in normalized_query_lower or "over time" in normalized_query_lower:
                # Simple rule-based detector
                date_col = None
                from services.dataset_profiler import profile_dataset
                profile = profile_dataset(df)
                if profile.datetime_columns:
                    date_col = profile.datetime_columns[0]
                
                if date_col:
                    print(f"[ORCHESTRATOR] 📈 Trend detected, using dimension: {date_col}")
                    if deterministic_intent:
                        deterministic_intent["intent"] = "SEGMENT_BY"
                        deterministic_intent["dimension"] = date_col
                    else:
                        # Create minimal intent for parse stage or fallback
                        trend_init = {
                            "intent": "SEGMENT_BY",
                            "dimension": date_col,
                            "kpi": None,
                            "filter": None
                        }
                        deterministic_intent = trend_init'''
    
    content = content.replace(old_block, new_block)
    
    codecs.open(path, 'w', 'utf-8').write(content)

def patch_context_resolver():
    print("Repairing context_resolver.py...")
    path = 'services/context_resolver.py'
    content = codecs.open(path, 'r', 'utf-8').read()
    
    # Fix spacing/indentation in return results
    content = content.replace('context_used=context_used,\n                context_applied=context_applied,', 'context_used=context_used, context_applied=context_applied,')
    content = content.replace('context_used=context_used,\n                 context_applied=context_applied,', 'context_used=context_used, context_applied=context_applied,')
    
    codecs.open(path, 'w', 'utf-8').write(content)

if __name__ == "__main__":
    patch_schema_mapper()
    patch_orchestrator()
    patch_context_resolver()
    print("Repairs complete.")
