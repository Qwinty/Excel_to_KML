from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass
class Config:
    """Application configuration."""
    input_dir: str = "input"
    xlsx_output_dir: str = "output/xlsx"
    kml_output_dir: str = "output/kml"
    single_kml_output_dir: str = "output/kml_single"
    header_rows_count: int = 5
    merge_columns: Tuple[int, int] = (1, 7)  # Columns A-G
    # None = auto-detect based on CPU count
    max_parallel_workers: Optional[int] = None


