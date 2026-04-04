import codecs
import re

def fix_mapper_indent():
    path = 'services/schema_mapper.py'
    content = codecs.open(path, 'r', 'utf-8').read()
    
    # Redefine the target block correctly
    # Note: 8 spaces for body, 4 spaces for def
    
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

    # Re-apply correctly
    content = re.sub(r'[ ]*def map_kpi\(self, user_term: str\).*?def map_dimension', new_map_kpi + '\n\n    def map_dimension', content, flags=re.DOTALL)
    codecs.open(path, 'w', 'utf-8').write(content)
    print("Fixed indentation in schema_mapper.py")

if __name__ == "__main__":
    fix_mapper_indent()
