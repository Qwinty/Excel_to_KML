from typing import List, Tuple
import simplekml
from src.config import Config
from src.utils import generate_random_color


def create_kml_point(kml, name: str, coords: Tuple[float, float], description: str, color: str | None = None, config: Config | None = None) -> None:
    if config is None:
        config = Config()
    if color is None:
        color = generate_random_color()
    point = kml.newpoint(name=name, coords=[coords])
    point.description = description
    point.style.iconstyle.color = color
    point.style.iconstyle.scale = config.kml_icon_scale
    point.style.labelstyle.scale = config.kml_label_scale


def create_kml_line(kml, name: str, coords: List[Tuple[float, float]], description: str, color: str | None = None, config: Config | None = None):
    if config is None:
        config = Config()
    if color is None:
        color = generate_random_color()
    line = kml.newlinestring(name=name, coords=coords)
    line.style.linestyle.color = color
    line.style.linestyle.width = config.kml_line_width
    line.description = description
    return line


def create_kml_polygon(kml, name: str, coords: List[Tuple[float, float]], description: str, color: str | None = None, config: Config | None = None):
    if config is None:
        config = Config()
    if color is None:
        color = generate_random_color()
    polygon = kml.newpolygon(name=name)
    polygon.outerboundaryis = coords  # type: ignore
    polygon.style.linestyle.color = color
    polygon.style.linestyle.width = config.kml_polygon_line_width
    polygon.style.polystyle.color = simplekml.Color.changealphaint(
        config.kml_polygon_alpha, color)
    polygon.description = description
    return polygon
