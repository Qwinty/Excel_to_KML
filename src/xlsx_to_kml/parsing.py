import logging
import json
import re
from typing import Dict, List, Tuple, Optional, cast
from functools import lru_cache

from pyproj import Transformer

from .models import Point, ParseError
from .anomalies import detect_coordinate_anomalies
from .projections import get_transformers
from src.config import Config

logger = logging.getLogger(__name__)


# Compiled regex patterns
MSK_COORD_PATTERN = re.compile(r'(\d+):\s*([-\d.]+)\s*м\.,\s*([-\d.]+)\s*м\.')
DMS_COORD_PATTERN = re.compile(
    r'(\d+)[°º]\s*(\d+)[\'′΄]\s*(\d+(?:[.,]\d+)?)[\"″′′˝]')
DMS_POINT_PATTERN = re.compile(r'(\d+)[:.]\s*(?=\d+[°º])')
ALT_POINT_PATTERN = re.compile(r'точка\s*(\d+)', re.IGNORECASE)


def looks_like_dms(coord_str: str) -> bool:
    return '°' in coord_str


def looks_like_msk(coord_str: str) -> bool:
    return ((' м.' in coord_str or ', м.' in coord_str or coord_str.endswith('м.')) and '°' not in coord_str)


def _should_prioritize_dms(coord_str: str) -> bool:
    return 'гск' in coord_str.lower()


def _validate_wgs84_range(lat: float, lon: float) -> bool:
    return -90 <= lat <= 90 and -180 <= lon <= 180


def parse_msk_coordinates(input_string: str, transformer: Transformer) -> List[Point]:
    matches = MSK_COORD_PATTERN.findall(input_string)
    if not matches:
        return []

    results: List[Point] = []
    for i, x_str, y_str in matches:
        try:
            x_val = float(x_str)
            y_val = float(y_str)
            if x_val == 0 and y_val == 0:
                continue
            lon, lat = transformer.transform(y_val, x_val)
            if not _validate_wgs84_range(lat, lon):
                reason = (
                    f"Координаты МСК вне допустимого диапазона WGS84 (lat={lat}, lon={lon}) после трансформации.")
                raise ParseError(reason)
            results.append(
                Point(name=f"точка {i}", lon=round(lon, 6), lat=round(lat, 6)))
        except Exception as e:
            reason = (
                f"Ошибка трансформации МСК координат: {e}. Исходные: x='{x_str}', y='{y_str}'.")
            raise ParseError(reason)

    return results


def _extract_dms_matches(coord_str: str) -> Tuple[List[Dict[str, object]], Dict[int, List[str]]]:
    parts = [p.strip() for p in coord_str.split(';') if p.strip()]
    all_dms_coords: List[Dict[str, object]] = []
    point_numbers_by_part: Dict[int, List[str]] = {}

    for idx, part in enumerate(parts):
        point_numbers = DMS_POINT_PATTERN.findall(part)
        if point_numbers:
            point_numbers_by_part[idx] = point_numbers
        coords_match = DMS_COORD_PATTERN.findall(part)
        for coord in coords_match:
            all_dms_coords.append(
                {'coord': coord, 'part': part, 'part_index': idx})

    return all_dms_coords, point_numbers_by_part


def _dms_tuple_to_decimal(d: Tuple[str, str, str]) -> float:
    return sum(float(x.replace(',', '.')) / (60 ** k) for k, x in enumerate(d))


def _derive_point_name(part_idx: int, pair_idx: int, mapping: Dict[int, List[str]], part_text: str) -> str:
    name = f"точка {pair_idx + 1}"
    part_point_numbers = mapping.get(part_idx, [])
    if pair_idx < len(part_point_numbers):
        name = f"точка {part_point_numbers[pair_idx]}"
    else:
        alt_match = ALT_POINT_PATTERN.search(part_text)
        if alt_match:
            name = f"точка {alt_match.group(1)}"
    return name


def _has_standalone_token(text: str, token: str) -> bool:
    """Возвращает True, если в тексте встречается токен как отдельное слово/метка.

    Учитываем кириллицу и латиницу. Например, ' ЗД' в координате
    должно считаться, а 'ЮЗД-25' — нет.
    """
    pattern = rf"(?<![A-Za-zА-Яа-яЁё]){re.escape(token)}(?![A-Za-zА-Яа-яЁё])"
    return re.search(pattern, text, flags=re.IGNORECASE) is not None


def parse_dms_coordinates(coord_str: str) -> List[Point]:
    all_dms_coords, point_numbers_by_part = _extract_dms_matches(coord_str)
    if not all_dms_coords:
        return []
    if len(all_dms_coords) % 2 != 0:
        reason = (
            f"Нечетное количество найденных ДМС координат ({len(all_dms_coords)}). Ожидается пара (широта, долгота).")
        raise ParseError(reason)

    result: List[Point] = []
    for j in range(0, len(all_dms_coords), 2):
        try:
            lat_info = all_dms_coords[j]
            lon_info = all_dms_coords[j + 1]
            lat_parts = cast(Tuple[str, str, str], lat_info['coord'])
            lon_parts = cast(Tuple[str, str, str], lon_info['coord'])
            lat = _dms_tuple_to_decimal(lat_parts)
            lon = _dms_tuple_to_decimal(lon_parts)

            combined_text = f"{cast(str, lat_info['part'])} {cast(str, lon_info['part'])}"
            if _has_standalone_token(combined_text, "ЮШ") or _has_standalone_token(combined_text, "S"):
                logger.debug(
                    f"  - ЮШ в строке. Преобразуем широту в отрицательную: {lat} -> {-lat}")
                lat = -lat
            if _has_standalone_token(combined_text, "ЗД") or _has_standalone_token(combined_text, "W"):
                logger.debug(
                    f"  - ЗД в строке. Преобразуем долготу в отрицательную: {lon} -> {-lon}")
                lon = -lon

            if not _validate_wgs84_range(lat, lon):
                reason = f"Координаты ДМС вне допустимого диапазона WGS84 (lat={lat}, lon={lon})."
                raise ParseError(reason)

            part_idx = cast(int, lat_info['part_index'])
            pair_idx = j // 2
            point_name = _derive_point_name(
                part_idx, pair_idx, point_numbers_by_part, cast(str, lat_info['part']))

            if lat != 0 or lon != 0:
                result.append(
                    Point(name=point_name, lon=round(lon, 6), lat=round(lat, 6)))
        except Exception as e:
            reason = f"Внутренняя ошибка при обработке пары ДМС: {e}."
            raise ParseError(reason)

    return result


def process_coordinates(input_string: str, transformer: Transformer, config: Config | None = None) -> List[Point]:
    if config is None:
        config = Config()
    results = parse_msk_coordinates(input_string, transformer)
    if not results:
        return []
    if len(results) >= 3:
        is_anomalous, a_reason, _ = detect_coordinate_anomalies(
            results, threshold_km=config.anomaly_threshold_km)
        if is_anomalous:
            raise ParseError(a_reason)
    return results


# --- SK-42 support ---

# PyProj pipeline for transforming SK-42 (Pulkovo 1942, Krassovsky) geographic
# coordinates (degrees) to WGS84 geographic coordinates (degrees).
_SK42_PIPELINE = (
    "+proj=pipeline +step +proj=axisswap +order=2,1 "
    "+step +proj=unitconvert +xy_in=deg +xy_out=rad "
    "+step +proj=push +v_3 +step +proj=cart +ellps=krass "
    "+step +proj=helmert +x=23.57 +y=-140.95 +z=-79.8 "
    "+rx=0 +ry=-0.35 +rz=-0.79 +s=-0.22 +convention=coordinate_frame "
    "+step +inv +proj=cart +ellps=WGS84 +step +proj=pop +v_3 "
    "+step +proj=unitconvert +xy_in=rad +xy_out=deg "
    "+step +proj=axisswap +order=2,1"
)


@lru_cache(maxsize=1)
def _get_sk42_transformer() -> Transformer:
    return Transformer.from_pipeline(_SK42_PIPELINE)


@lru_cache(maxsize=1)
def _load_objects_info(path: str = "data/objects_info.json") -> Dict[str, List[str]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return cast(Dict[str, List[str]], json.load(f))
    except FileNotFoundError:
        logger.warning(
            "Файл 'data/objects_info.json' не найден. SK-42 определение будет пропущено.")
        return {}
    except Exception as e:
        logger.warning(
            f"Не удалось загрузить 'data/objects_info.json' ({e}). SK-42 определение будет пропущено.")
        return {}


def _detect_system_key_for_string(coord_str: str) -> Optional[str]:
    """Возвращает ключ системы координат из objects_info.json, если найдено точное совпадение строки."""
    info = _load_objects_info()
    coord_trimmed = coord_str.strip()
    for system_key, entries in info.items():
        for s in entries:
            if s.strip() == coord_trimmed:
                return system_key
    return None


def transform_points_sk42_to_wgs84(points: List[Point]) -> List[Point]:
    transformer = _get_sk42_transformer()
    transformed: List[Point] = []
    for p in points:
        # В pipeline ожидается порядок (lat, lon) и возвращает (lat, lon)
        lat_wgs, lon_wgs = transformer.transform(p.lat, p.lon)
        if not _validate_wgs84_range(lat_wgs, lon_wgs):
            raise ParseError(
                f"Координаты после преобразования СК-42→WGS84 вне диапазона (lat={lat_wgs}, lon={lon_wgs}).")
        transformed.append(
            Point(name=p.name, lon=round(lon_wgs, 6), lat=round(lat_wgs, 6))
        )
    return transformed


def parse_coordinates(
    coord_str: str,
    transformers: Optional[dict[str, Transformer]] = None,
    proj4_path: str = "data/proj4.json",
    config: Config | None = None,
) -> List[Point]:
    if config is None:
        config = Config()
    if not coord_str or not isinstance(coord_str, str):
        logger.debug("Пустая или нестроковая строка координат")
        return []

    coord_str = coord_str.strip()
    logger.debug(f"1. Исходная строка после удаления пробелов: '{coord_str}'")

    if not coord_str:
        logger.debug(
            "Строка пуста после удаления пробелов. Возвращаем пустой результат.")
        return []

    logger.debug("2. Определение формата координат (детектор)...")

    # 2.a. Точное совпадение строки с любым объектом из objects_info.json → определяем систему
    system_key = _detect_system_key_for_string(coord_str)
    if system_key:
        logger.debug(
            f"  - Строка найдена в 'objects_info.json'. Система координат: '{system_key}'.")
        # Сейчас поддерживаем только СК-42
        if system_key.strip().upper() == "СК-42":
            logger.debug("    - Применяем преобразование СК-42→WGS84.")
            dms_points = parse_dms_coordinates(coord_str)
            if not dms_points:
                return []
            transformed_points = transform_points_sk42_to_wgs84(dms_points)
            if len(transformed_points) >= 3:
                is_anomalous, reason, _ = detect_coordinate_anomalies(
                    transformed_points, threshold_km=config.anomaly_threshold_km)
                if is_anomalous:
                    logger.warning(f"  - Детектор аномалий сообщил: {reason}")
                    raise ParseError(reason)
            logger.debug(
                f"7. Парсинг СК-42 успешно завершен. Найдено {len(transformed_points)} валидных координат.")
            return transformed_points
        else:
            logger.debug(
                "    - Для данной системы координат преобразование пока не настроено. Продолжаем обычный разбор.")

    if _should_prioritize_dms(coord_str):
        logger.debug("  - Обнаружен маркер 'гск'. Приоритет ДМС.")
    else:
        if looks_like_msk(coord_str):
            logger.debug("  - Кандидат на МСК-формат. Попытка парсинга МСК.")
            if transformers is None:
                try:
                    transformers = get_transformers(proj4_path)
                except Exception:
                    reason = "Не удалось загрузить описания проекций для МСК."
                    logger.warning(f"{reason} Строка: '{coord_str[:50]}'")
                    raise ParseError(reason)
            for key, transformer in transformers.items():
                if key in coord_str:
                    logger.debug(f"    - Найдена система координат: '{key}'.")
                    msk_points = parse_msk_coordinates(coord_str, transformer)
                    if msk_points and len(msk_points) >= 3:
                        is_anomalous, a_reason, _ = detect_coordinate_anomalies(
                            msk_points, threshold_km=config.anomaly_threshold_km)
                        if is_anomalous:
                            logger.warning(
                                f"  - Детектор аномалий сообщил: {a_reason}")
                            raise ParseError(a_reason)
                    return msk_points
            reason = "Обнаружены координаты 'м.', но не найдена известная система координат МСК в строке."
            logger.warning(f"{reason} Строка: '{coord_str[:50]}'")
            raise ParseError(reason)

    logger.debug("3. Проверка на наличие маркера ДМС ('°')...")
    if '°' not in coord_str:
        logger.debug(
            "  - Маркер '°' не найден. Предполагается, что в строке нет координат. Возвращаем пустой результат.")
        return []

    logger.debug("  - Маркер '°' найден. Начинается парсинг ДМС.")
    dms_points = parse_dms_coordinates(coord_str)

    if dms_points and len(dms_points) >= 3:
        is_anomalous, reason, _ = detect_coordinate_anomalies(
            dms_points, threshold_km=config.anomaly_threshold_km)
        if is_anomalous:
            logger.warning(f"  - Детектор аномалий сообщил: {reason}")
            raise ParseError(reason)

    logger.debug(
        f"7. Парсинг успешно завершен. Найдено {len(dms_points)} валидных координат.")
    return dms_points
