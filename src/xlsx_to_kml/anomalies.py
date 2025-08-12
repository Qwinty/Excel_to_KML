import math
from typing import List, Tuple

from .models import Point


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate the great circle distance between two points on earth (km)."""
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371  # Earth radius in kilometers
    return c * r


def detect_coordinate_anomalies(
    coordinates: List[Point], threshold_km: float = 20
) -> Tuple[bool, str | None, list[tuple[int, str, float, float]]]:
    """
    Detect anomalous coordinates in a sequence by looking for points that are
    significantly further away from the majority of other points.
    """
    if len(coordinates) < 3:
        return False, None, []

    distances: list[tuple[int, float]] = []
    anomalous_points: list[tuple[int, str, float, float]] = []

    for i, point_i in enumerate(coordinates):
        lon_i = point_i.lon
        lat_i = point_i.lat
        point_distances: list[float] = []
        for j, point_j in enumerate(coordinates):
            if i != j:
                dist = haversine_distance(lat_i, lon_i, point_j.lat, point_j.lon)
                point_distances.append(dist)

        avg_distance = sum(point_distances) / len(point_distances)
        distances.append((i, avg_distance))

    for idx, avg_dist in distances:
        if avg_dist > threshold_km:
            p = coordinates[idx]
            anomalous_points.append((idx, p.name, p.lon, p.lat))

    if anomalous_points:
        reason = "Обнаружены аномальные координаты, значительно удаленные от других"
        return True, reason, anomalous_points

    return False, None, []


