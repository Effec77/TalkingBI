"""Test Phase 6G: Deterministic Intent Override"""

import sys

sys.path.insert(0, ".")

from services.deterministic_override import (
    detect_simple_segment,
    detect_simple_filter,
    apply_deterministic_override,
)
from services.schema_mapper import SchemaMapper
import pandas as pd

# Test data
df = pd.DataFrame({"revenue": [100, 200], "region": ["North", "South"]})
kpi_candidates = [{"name": "Revenue", "source_column": "revenue"}]

mapper = SchemaMapper(df, kpi_candidates)

# Test 6G
test_queries = ["by region", "region", "filter electronics", "electronics only"]

print("=== Phase 6G Tests ===")
for query in test_queries:
    result = apply_deterministic_override(query, mapper)
    if result:
        print(f"{query} -> 6G: {result['intent']}")
    else:
        print(f"{query} -> No 6G match (will use LLM)")
