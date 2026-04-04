import codecs

filepath = 'services/schema_mapper.py'
content = codecs.open(filepath, 'r', 'utf-8').read()

old_block = '''        # 3. Schema map lookup
        for canonical, aliases in self.schema_map.items():
            # Check if user term matches canonical or any alias
            if user_lower == canonical.lower():
                # Find actual KPI name that matches this canonical
                for kpi in self.kpi_candidates:
                    kpi_name = kpi.get("name", "").lower()
                    kpi_source = kpi.get("source_column", "").lower()
                    if kpi_name == canonical.lower() or kpi_source == canonical.lower():
                        return kpi.get("name"), "schema_map"
                return canonical, "schema_map"

            # Check aliases
            for alias in aliases:
                if user_lower == alias.lower():
                    # Return canonical form
                    for kpi in self.kpi_candidates:
                        kpi_name = kpi.get("name", "").lower()
                        kpi_source = kpi.get("source_column", "").lower()
                        if (
                            kpi_name == canonical.lower()
                            or kpi_source == canonical.lower()
                        ):
                            return kpi.get("name"), "schema_map"
                    return canonical, "schema_map"'''

new_block = '''        # 3. Schema map lookup
        for canonical, aliases in self.schema_map.items():
            # Check if user term matches canonical or any alias
            if user_lower == canonical.lower() or user_lower in [a.lower() for a in aliases]:
                # If the dataset literally has a kpi matching aliases or canonical, return it
                for kpi in self.kpi_candidates:
                    kpi_name = kpi.get("name", "").lower()
                    kpi_source = kpi.get("source_column", "").lower()
                    if kpi_name == canonical.lower() or kpi_source == canonical.lower():
                        return kpi.get("name"), "schema_map"
                    for alias in aliases:
                        if kpi_name == alias.lower() or kpi_source == alias.lower():
                            return kpi.get("name"), "schema_map"
                
                # Check columns directly if KPI candidates are missing it
                for col_norm, col_raw in normalized_columns.items():
                    if col_norm == canonical.lower() or col_norm in [a.lower() for a in aliases]:
                        return col_raw, "schema_map_column_fallback"
                        
                return canonical, "schema_map_default"'''

content = content.replace(old_block, new_block)
codecs.open(filepath, 'w', 'utf-8').write(content)
print("Updated schema_map lookup logic.")
