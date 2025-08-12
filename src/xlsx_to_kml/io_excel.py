from typing import List, Dict
import logging
from openpyxl.utils import get_column_letter
from src.config import Config

logger = logging.getLogger(__name__)


def find_column_index(sheet, target_names: List[str], exact_match: bool = False) -> int:
    """Находит индекс столбца для любого из заданных имен заголовков в строках 1-8."""
    target_names_lower = [str(name).lower().strip() for name in target_names]
    for row in sheet.iter_rows(min_row=1, max_row=8, values_only=True):
        for idx, cell in enumerate(row):
            if cell:
                cell_str_lower = str(cell).lower().strip()
                for target_name_lower in target_names_lower:
                    if (exact_match and cell_str_lower == target_name_lower) or \
                       (not exact_match and target_name_lower in cell_str_lower):
                        return idx
    return -1


def get_column_indices(sheet, config: Config | None = None) -> dict:
    """Получает индексы всех необходимых столбцов на основе конфигурации."""
    if config is None:
        config = Config()

    columns: Dict[str, List[str]] = config.excel_columns
    exact_match_keys = set(config.excel_exact_match_keys)

    indices: dict = {}
    for key, value in columns.items():
        exact = key in exact_match_keys
        indices[key] = find_column_index(sheet, value, exact_match=exact)

    original_names = {key: value[0] for key, value in columns.items()}
    for key, value in indices.items():
        if value == -1:
            logger.debug(
                f"Столбец '{original_names[key]}' (или его альтернативы) не найден.")

    return indices
