from dataclasses import dataclass
from typing import List, Dict


@dataclass(frozen=True)
class UploadedDataset:
    session_id: str
    filename: str
    columns: List[str]
    dtypes: Dict[str, str]
    shape: tuple
    sample_values: Dict[str, List[str]]
    missing_pct: Dict[str, float]
