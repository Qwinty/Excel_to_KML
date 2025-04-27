import logging
import math
import random
from datetime import datetime
from pathlib import Path
import colorlog  # Import colorlog


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
    """Настраивает систему логирования с цветным выводом в консоль."""
    # Check if the root logger already has handlers - if so, it's already configured
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        # Return the specific logger if already set up
        return logging.getLogger(__name__)

    # --- Configuration ---
    log_level = logging.DEBUG
    log_format_plain = '%(asctime)s - %(levelname)s - [%(name)s] %(message)s'
    log_format_color = (
        '%(log_color)s%(asctime)s - %(levelname)s - [%(name)s]%(reset)s %(message)s'
    )
    date_format = '%Y-%m-%d %H:%M:%S'

    # --- Directory Setup ---
    logs_dir = Path("logs")
    if output_dir:
        logs_dir = Path(output_dir) / "logs"
    logs_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = logs_dir / f"log_{timestamp}.log"
    error_warning_file = logs_dir / f"errors_warnings_{timestamp}.log"

    # --- Formatters ---
    plain_formatter = logging.Formatter(log_format_plain, date_format)
    colored_formatter = colorlog.ColoredFormatter(
        log_format_color,
        datefmt=date_format,
        reset=True,
        log_colors={
            'DEBUG':    'cyan',
            'INFO':     'green',
            'WARNING':  'yellow',
            'ERROR':    'red',
            'CRITICAL': 'red,bg_white',
        },
        secondary_log_colors={},
        style='%'
    )

    # --- Handlers ---
    # Console Handler (Colored)
    console_handler = colorlog.StreamHandler()
    console_handler.setFormatter(colored_formatter)
    console_handler.setLevel(log_level)  # Handle all levels in console

    # Main File Handler (Plain)
    main_file_handler = logging.FileHandler(log_file, encoding='utf-8')
    main_file_handler.setFormatter(plain_formatter)
    main_file_handler.setLevel(log_level)  # Log all levels to main file

    # Error/Warning File Handler (Plain)
    error_warning_handler = logging.FileHandler(
        error_warning_file, encoding='utf-8')
    error_warning_handler.setFormatter(plain_formatter)
    # Log only WARNING and above
    error_warning_handler.setLevel(logging.WARNING)

    # --- Root Logger Setup ---
    # Set root level to lowest level needed by any handler
    root_logger.setLevel(log_level)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(main_file_handler)
    root_logger.addHandler(error_warning_handler)

    # Return the specific logger for the calling module
    return logging.getLogger(__name__)
