import logging
import math
import random
from datetime import datetime
from pathlib import Path


def generate_random_color() -> str:
    """Generate a random color in KML format."""
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

def setup_logging():
    """Настраивает систему логирования"""
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)

    log_file = logs_dir / f"separate_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)