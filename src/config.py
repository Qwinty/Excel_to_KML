from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


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

    # Projections / parsing
    proj4_path: str = "data/proj4.json"
    anomaly_threshold_km: float = 20.0

    # Excel parsing configuration
    # Map of semantic column keys to possible header names (synonyms)
    excel_columns: Dict[str, List[str]] = field(default_factory=lambda: {
        "name": ["№ п/п"],
        "coord": ["Место водопользования"],
        "organ": ["Уполномоченный орган"],
        "additional_name": ["Наименование водного объекта"],
        "goal": ["Цель водопользования"],
        "vid": ["Вид водопользования"],
        "owner": ["Наименование"],
        "inn": ["ИНН"],
        "start_date": ["Дата начала водопользования"],
        "end_date": ["Дата окончания водопользования", "Дата прекращения действия"],
    })
    # Which keys require exact header match
    excel_exact_match_keys: List[str] = field(default_factory=lambda: [
        "owner",
    ])
    # Heuristic to detect the first data row by scanning for markers in early rows
    excel_header_scan_min_row: int = 2
    excel_header_scan_max_row: int = 5
    excel_default_data_start_row: int = 6

    # KML styling
    kml_icon_scale: float = 1.0
    kml_label_scale: float = 0.8
    kml_line_width: int = 3
    kml_polygon_line_width: int = 3
    kml_polygon_alpha: int = 100  # 0..255
    # Business rules
    pipeline_skip_terms: List[str] = field(default_factory=lambda: [
        "Сброс сточных",
        "Забор (изъятие)",
    ])
