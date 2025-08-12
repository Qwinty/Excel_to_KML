from dataclasses import dataclass, field
from typing import List, Optional


class ParseError(Exception):
    """Raised when coordinate parsing fails with a user-facing reason."""
    pass


@dataclass(frozen=True)
class Point:
    """Typed representation of a geographic point."""
    name: str
    lon: float
    lat: float


@dataclass
class ConversionResult:
    """Result of converting a single file to KML."""
    filename: str
    total_rows: int = 0
    successful_rows: int = 0
    failed_rows: int = 0
    anomaly_rows: int = 0
    error_reasons: List[str] = field(default_factory=list)
    processing_time: float = 0.0
    anomaly_file_created: bool = False

    @property
    def success_rate(self) -> float:
        if self.total_rows == 0:
            return 0.0
        return (self.successful_rows / self.total_rows) * 100

    @property
    def failure_rate(self) -> float:
        if self.total_rows == 0:
            return 0.0
        return (self.failed_rows / self.total_rows) * 100


@dataclass(frozen=True)
class ParseResult:
    """Optional structured result for parsers; currently wraps a list of points.

    Not widely used yet, but reserved for future expansion.
    """
    points: List[Point]
    reason: Optional[str] = None


