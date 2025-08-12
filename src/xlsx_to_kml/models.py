from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional


class ParseError(Exception):
    """Raised when coordinate parsing fails with a user-facing reason."""
    pass


class WaterUsageType(Enum):
    """Enum for water usage types."""
    INTAKE = "забор"  # Забор воды
    DISCHARGE = "сброс"  # Сброс воды
    OTHER = "прочее"  # Прочие виды водопользования


def get_water_usage_type(goal_text: str) -> WaterUsageType:
    """Determine water usage type from goal text.

    Args:
        goal_text: Text from the "Цель водопользования" column

    Returns:
        WaterUsageType: The detected type of water usage
    """
    if not goal_text:
        return WaterUsageType.OTHER

    goal_lower = goal_text.lower()

    if "сброс" in goal_lower:
        return WaterUsageType.DISCHARGE
    elif "забор" in goal_lower:
        return WaterUsageType.INTAKE
    else:
        return WaterUsageType.OTHER


def generate_point_name(main_name: str, water_type: WaterUsageType, index: int, point_name: str = "") -> str:
    """Generate a point name based on water usage type.

    Args:
        main_name: Main identifier (№ п/п)
        water_type: Type of water usage
        index: Point index for multiple points
        point_name: Original point name from coordinates

    Returns:
        str: Generated point name
    """
    base_name = f"№ п/п {main_name}"

    if water_type == WaterUsageType.DISCHARGE:
        return f"{base_name} - сброс {index}"
    elif water_type == WaterUsageType.INTAKE:
        return f"{base_name} - забор {index}"
    else:
        if point_name:
            return f"{base_name} - {point_name}"
        else:
            return base_name


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
