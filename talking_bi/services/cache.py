from typing import Any
llm_cache = {}
query_cache = {}

USE_CACHE = False

class CacheStats:
    query_cache_hits = 0
    llm_cache_hits = 0

stats = CacheStats()

def get_llm_key(query: str) -> int:
    return hash(query.lower().strip())

def get_query_key(query: str, dataset: str, context=None) -> int:
    # Rule 5 Phase 9C: Context-aware cache key
    return hash((query.lower().strip(), dataset, str(context)))
