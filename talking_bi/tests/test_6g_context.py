"""Test 6G with context awareness"""

import sys

sys.path.insert(0, ".")

from services.deterministic_override import DeterministicIntentDetector
from services.schema_mapper import SchemaMapper
import pandas as pd

# Test data
df = pd.DataFrame({"revenue": [100, 200], "region": ["North", "South"]})
kpi_candidates = [{"name": "Revenue", "source_column": "revenue"}]

mapper = SchemaMapper(df, kpi_candidates)

# Test with NO context
print("=== Test 1: NO context ===")
detector1 = DeterministicIntentDetector(mapper, [])
result1 = detector1.detect("by region")
print(f"Result: {result1}")
print()

# Test WITH context (simulating after "show revenue")
print("=== Test 2: WITH context ===")
context = [{"intent": {"kpi": "Revenue", "dimension": None}}]
detector2 = DeterministicIntentDetector(mapper, context)
result2 = detector2.detect("by region")
print(f"Result: {result2}")
print()

# Test standalone dimension with context
print("=== Test 3: Standalone 'region' with context ===")
detector3 = DeterministicIntentDetector(mapper, context)
result3 = detector3.detect("region")
print(f"Result: {result3}")
