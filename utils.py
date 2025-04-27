import logging
import math
import random
from datetime import datetime
from pathlib import Path


def generate_random_color() -> str:
    """Рандомный цвет в KML формате."""
    return f'{random.randint(0, 255):02x}{random.randint(0, 255):02x}{random.randint(0, 255):02x}'


def calculate_centroid(points):
    x_sum = sum(point[0] for point in points)
    y_sum = sum(point[1] for point in points)
    return x_sum / len(points), y_sum / len(points)


def calculate_angle(point, centroid):
    return math.atan2(point[1] - centroid[1], point[0] - centroid[0])


def sort_coordinates(coords):
    centroid = calculate_centroid(coords)
    return sorted(coords, key=lambda coord: calculate_angle(coord, centroid))


def setup_logging(output_dir=None):
    """Настраивает систему логирования"""
    # Check if the root logger already has handlers - if so, it's already configured
    if logging.root.handlers:
        return logging.getLogger(__name__)

    logs_dir = Path("logs")
    if output_dir:
        logs_dir = Path(output_dir) / "logs"
    logs_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = logs_dir / f"log_{timestamp}.log"
    error_warning_file = logs_dir / f"errors_warnings_{timestamp}.log"

    # Configure the root logger with all logs
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - [%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

    # Create a combined handler for errors and warnings
    error_warning_handler = logging.FileHandler(
        error_warning_file, encoding='utf-8')
    # This captures both WARNING and ERROR/CRITICAL
    error_warning_handler.setLevel(logging.WARNING)
    error_warning_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(name)s] %(message)s',
        '%Y-%m-%d %H:%M:%S'
    ))

    # Add the handler to the root logger
    logging.getLogger('').addHandler(error_warning_handler)

    return logging.getLogger(__name__)
