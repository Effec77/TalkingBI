"""Quick test for 6E and 6F fixes"""

import sys

sys.path.insert(0, ".")

import pandas as pd
from services.query_normalizer import QueryNormalizer
from services.schema_mapper import SchemaMapper

df = pd.DataFrame(
    {"revenue": [100, 200], "amount": [50, 60], "product_category": ["A", "B"]}
)

kpi_candidates = [
    {"name": "Revenue", "source_column": "revenue"},
    {"name": "Amount", "source_column": "amount"},
]

# Test 6E
print("=== Testing 6E Normalizer ===")
normalizer = QueryNormalizer(list(df.columns), kpi_candidates)

test_queries = [
    "total amount",
    "revenue numbers",
    "product category",
]

for query in test_queries:
    norm, meta = normalizer.normalize(query)
    print(f"{query} -> {norm}")
    print(f"  modifications: {meta.get('modifications', [])}")

print()
print("=== Testing 6F SchemaMapper ===")
mapper = SchemaMapper(df, kpi_candidates)

test_terms = ["sales", "total_amount", "churn", "product category"]
for term in test_terms:
    result, source = mapper.map_kpi(term)
    print(f"{term} -> {result} ({source})")
