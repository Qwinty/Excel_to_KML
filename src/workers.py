import logging
from pathlib import Path
from typing import Any, Optional, Tuple

from openpyxl import load_workbook

from src.utils import setup_logging
from src.xlsx_to_kml import create_kml_from_coordinates, ConversionResult, get_transformers


def initialize_worker_logging() -> None:
    """Initializer for each worker process to set up its logging."""
    setup_logging(console_level=logging.ERROR)


def process_file_worker(
    xlsx_file_path: str,
    kml_file_path: str,
    xlsx_output_dir: str,
    kml_output_dir: str
) -> Tuple[bool, str, Optional[ConversionResult], Optional[str]]:
    """
    Worker function for parallel file processing.

    Returns:
        Tuple of (success, filename, conversion_result, error_message)
    """

    try:
        filename = Path(xlsx_file_path).name
        Path(kml_file_path).parent.mkdir(parents=True, exist_ok=True)
        workbook = load_workbook(
            filename=xlsx_file_path, data_only=True, read_only=True)
        # Load transformers lazily (cached per-process)
        transformers = None
        try:
            transformers = get_transformers()
        except Exception:
            # If transformers cannot be loaded, MSK parsing will return an error per-row; continue
            transformers = None
        conversion_result = create_kml_from_coordinates(
            workbook.active,
            output_file=kml_file_path,
            filename=filename,
            transformers=transformers
        )

        return True, filename, conversion_result, None

    except Exception as e:
        filename = Path(xlsx_file_path).name if xlsx_file_path else "Unknown"
        error_message = f"Error converting {filename}: {str(e)}"
        return False, filename, None, error_message
