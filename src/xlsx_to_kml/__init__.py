"""Public API for the xlsx_to_kml package.

This module exposes a minimal interface while keeping backward compatibility
for existing imports in the codebase.
"""

from .pipeline import create_kml_from_coordinates
from .parsing import parse_coordinates
from .models import ConversionResult, Point, ParseResult, ParseError

# Backward compatibility: keep commonly used helpers available at package level
from .projections import get_transformers, create_transformer  # noqa: F401
from .parsing import process_coordinates  # noqa: F401

__all__ = [
    "create_kml_from_coordinates",
    "parse_coordinates",
    "ConversionResult",
    "Point",
    "ParseResult",
]
