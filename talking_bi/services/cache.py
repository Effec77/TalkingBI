llm_cache = {}
query_cache = {}

USE_CACHE = False

class CacheStats:
    query_cache_hits = 0
    llm_cache_hits = 0

stats = CacheStats()

def get_llm_key(query: str) -> int:
    return hash(query.lower().strip())

def get_query_key(query: str, dataset: str) -> int:
    return hash((query.lower().strip(), dataset))
