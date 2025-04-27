import glob
import json
import re
import math
from typing import List, Tuple, Optional
import simplekml
from openpyxl import load_workbook
import logging

from pyproj import CRS, Transformer

# Import necessary functions and the setup function from utils
from utils import generate_random_color, sort_coordinates, setup_logging

# Get the logger instance for this module
logger = logging.getLogger(__name__)


def create_transformer(proj4_str: str) -> Transformer:
    """Создает трансформер из заданной строки Proj4 в WGS84."""
    crs = CRS.from_proj4(proj4_str)
    return Transformer.from_crs(crs, "EPSG:4326", always_xy=True)


# Определяем строки Proj4 и создаем трансформеры
try:
    with open("data/proj4.json", "r", encoding="utf-8") as f:
        proj4_strings = json.load(f)
    # Создаем трансформеры
    transformers = {name: create_transformer(
        proj4) for name, proj4 in proj4_strings.items()}
except FileNotFoundError:
    logger.critical("Critical Error: Could not find 'data/proj4.json'. This file is required for coordinate transformations. Ensure it exists in the 'data' directory relative to the application.")
    print("[bold red]Критическая ошибка: Не найден файл 'data/proj4.json'.[/bold red]")
    print("[bold red]Этот файл необходим для преобразования координат. Убедитесь, что он находится в папке 'data' рядом с программой.[/bold red]")
    # Exit or raise a custom exception if the program cannot function without it
    raise SystemExit("Missing essential data file: data/proj4.json")
except json.JSONDecodeError:
    logger.critical(
        "Critical Error: Could not parse 'data/proj4.json'. Check the file format.")
    print("[bold red]Критическая ошибка: Не удалось прочитать файл 'data/proj4.json'. Проверьте формат файла.[/bold red]")
    raise SystemExit("Invalid format for essential data file: data/proj4.json")
except Exception as e:
    logger.critical(
        f"Critical Error: An unexpected error occurred while loading projection data: {e}", exc_info=True)
    print(
        f"[bold red]Критическая ошибка: Непредвиденная ошибка при загрузке данных проекций: {e}[/bold red]")
    raise SystemExit("Unexpected error loading projection data")


def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate the great circle distance between two points on earth (specified in decimal degrees)"""
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * \
        math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371  # Radius of earth in kilometers
    return c * r


def detect_coordinate_anomalies(coordinates, threshold_km=5):
    """
    Detect anomalous coordinates in a sequence by looking for points that are
    significantly further away from the majority of other points.

    Args:
        coordinates: List of tuples (name, lon, lat)
        threshold_km: Distance threshold in kilometers

    Returns:
        Tuple of (is_anomalous, reason, anomalous_points)
    """
    if len(coordinates) < 3:
        # Not enough points to detect anomalies
        return False, None, []

    distances = []
    anomalous_points = []

    # Calculate distance from each point to every other point
    for i, (name_i, lon_i, lat_i) in enumerate(coordinates):
        point_distances = []
        for j, (name_j, lon_j, lat_j) in enumerate(coordinates):
            if i != j:
                dist = haversine_distance(lat_i, lon_i, lat_j, lon_j)
                point_distances.append(dist)

        # Calculate the average distance to other points
        avg_distance = sum(point_distances) / len(point_distances)
        distances.append((i, avg_distance))

    # Find points that are much further away from others
    avg_of_avgs = sum(d for _, d in distances) / len(distances)

    for idx, avg_dist in distances:
        # If a point's average distance to others is much larger than overall average
        if avg_dist > threshold_km and avg_dist > 3 * avg_of_avgs:
            point_name, lon, lat = coordinates[idx]
            anomalous_points.append((idx, point_name, lon, lat))

    if anomalous_points:
        anomaly_details = ', '.join([f"точка {point_name} ({lat}, {lon})"
                                     for _, point_name, lon, lat in anomalous_points])
        reason = f"Обнаружены аномальные координаты, значительно удаленные от других: {anomaly_details}"
        return True, reason, anomalous_points

    return False, None, []


def process_coordinates(input_string, transformer) -> Tuple[Optional[List[Tuple[str, float, float]]], Optional[str]]:
    # Извлекаем координаты из строки
    coordinates = re.findall(
        r'(\d+):\s*([-\d.]+)\s*м\.,\s*([-\d.]+)\s*м\.', input_string)
    results = []
    for i, x_str, y_str in coordinates:
        try:
            x = float(x_str)
            y = float(y_str)
            if x == 0 and y == 0:
                logger.debug(f"Пропуск нулевых координат МСК: {x}, {y}")
                continue
            # Трансформация может вызвать исключение
            # Note: y, x order might be specific to the projection
            lon, lat = transformer.transform(y, x)

            # Проверка диапазона (Task 3)
            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                reason = f"Координаты МСК вне допустимого диапазона WGS84 ({lat=}, {lon=}) после трансформации."
                logger.warning(
                    f"{reason} Исходные: {x=}, {y=}. Строка: '{input_string}'")
                return None, reason  # Возвращаем ошибку для всей строки

            results.append((f"точка {i}", round(lon, 6), round(lat, 6)))
        except Exception as e:
            reason = f"Ошибка трансформации МСК координат: {e}. Исходные: {x_str}, {y_str}."
            logger.warning(f"{reason} Строка: '{input_string}'")
            return None, reason  # Ошибка трансформации делает всю строку аномальной

    if not results:
        # Если были найдены маркеры 'м.', но после фильтрации (0,0) не осталось валидных точек
        return [], None  # Успешный парсинг, но нет валидных данных

    # Check for anomalous coordinates
    is_anomalous, reason, anomalous_points = detect_coordinate_anomalies(
        results)
    if is_anomalous:
        return None, reason

    return results, None


def parse_coordinates(coord_str: str) -> Tuple[Optional[List[Tuple[str, float, float]]], Optional[str]]:
    """Парсит строку с координатами, проверяет их валидность и возвращает список кортежей (имя, долгота, широта) или ошибку.

    Returns:
        Tuple[Optional[List[Tuple[str, float, float]]], Optional[str]]:
            (список_координат, None) при успехе, (None, причина_ошибки) при ошибке.
            Список координат может быть пустым, если в строке не найдено валидных данных.
    """
    if not coord_str or not isinstance(coord_str, str):
        logger.debug("Пустая или нестроковая строка координат")
        # Не считаем это ошибкой формата, просто нет данных для парсинга.
        # Функция вызывается только если строка *потенциально* содержит координаты.
        # Возвращаем пустой список, а не ошибку.
        return [], None

    coord_str = coord_str.strip()
    if not coord_str:
        return [], None  # Строка состояла только из пробелов

    # Проверка на МСК координаты
    if ' м.' in coord_str or ', м.' in coord_str or coord_str.endswith('м.'):
        # Попытка найти МСК систему в строке
        found_msk = False
        for key, transformer in transformers.items():
            if key in coord_str:
                logger.debug(f"Обнаружена система координат МСК: {key}")
                return process_coordinates(coord_str, transformer)
        # Если 'м.' есть, но система не опознана
        reason = f"Обнаружены координаты 'м.', но не найдена известная система координат МСК в строке."
        logger.warning(f"{reason} Строка: '{coord_str[:50]}'")
        return None, reason

    # Проверка на ДМС координаты
    if '°' not in coord_str:
        # Если нет ни 'м.', ни '°', считаем формат некорректным для координат
        # Это может быть просто адрес или описание без координат
        # Не логируем как ошибку, просто возвращаем пустой список, т.к. координат нет.
        logger.debug(
            f"Строка не содержит маркеров координат ('м.', '°'): '{coord_str}'")
        return [], None  # Нет данных для парсинга

    # Парсинг ДМС
    parts = coord_str.split(';')
    result = []
    has_valid_dms = False  # Флаг, что хотя бы одни ДМС координаты были успешно распознаны

    for part in [p.strip() for p in parts if p.strip()]:
        # Reset name for each part - default to empty
        point_prefix = ""
        # Регулярное выражение для ДМС: ddd° mm' ss.s" - поддерживает и обычные апострофы/кавычки, и Unicode символы ′/′′
        coords_match = re.findall(
            r'(\d+)°\s*(\d+)[\'′]\s*(\d+(?:[.,]\d+)?)[\"″′′]', part)

        if not coords_match:
            # Часть строки не содержит ДМС, пропускаем её
            # logger.debug(f"Часть строки не содержит ДМС координат: '{part}'")
            continue

        # Ищем имя только если есть явные маркеры "точка" или "выпуск"
        if "выпуск" in part.lower():
            # Ищем точное совпадение с номером
            match = re.search(r'(выпуск\s+№?\s*\d+)', part, re.IGNORECASE)
            if match:
                point_prefix = match.group(1).strip()  # e.g., "выпуск №1"
        elif "точка" in part.lower():
            # Ищем точное совпадение с номером
            match = re.search(r'(точка\s*\d+)', part, re.IGNORECASE)
            if match:
                point_prefix = match.group(1).strip()  # e.g., "точка 1"
        # Если явных маркеров с номерами нет, point_prefix остается ""

        # Ожидаем 2 или кратное 2 количество совпадений (широта, долгота)
        if len(coords_match) % 2 != 0:
            reason = f"Нечетное количество найденных ДМС координат в части: {len(coords_match)}. Ожидается пара (широта, долгота)."
            logger.warning(f"{reason}")
            return None, reason

        if len(coords_match) >= 2:
            has_valid_dms = True  # Нашли хотя бы одну пару ДМС

            for i in range(0, len(coords_match), 2):
                try:
                    lat_parts = coords_match[i]
                    lon_parts = coords_match[i+1]

                    # Конвертируем широту и долготу в десятичные градусы
                    lat = sum(float(x.replace(',', '.')) / (60 ** j)
                              for j, x in enumerate(lat_parts))
                    lon = sum(float(x.replace(',', '.')) / (60 ** j)
                              for j, x in enumerate(lon_parts))

                    # Определяем знак
                    lat = -lat if "ЮШ" in part or "S" in part else lat
                    lon = -lon if "ЗД" in part or "W" in part else lon

                    # Проверка диапазона (Task 3)
                    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                        reason = f"Координаты ДМС вне допустимого диапазона WGS84 ({lat=}, {lon=})."
                        logger.warning(f"{reason}")
                        return None, reason  # Вся строка аномальна

                    # --- Revised Point Naming ---
                    # Start with prefix (or empty)
                    point_specific_name = point_prefix
                    # If there are multiple pairs in this part AND we extracted a prefix (e.g., "точка 1")
                    # append a sub-index (e.g., "точка 1.1", "точка 1.2")
                    if len(coords_match) > 2 and point_prefix:
                        point_specific_name = f"{point_prefix}.{i // 2 + 1}"
                    # If multiple pairs and NO prefix, use simple index (e.g., "т.1", "т.2")
                    elif len(coords_match) > 2 and not point_prefix:
                        point_specific_name = f"т.{i // 2 + 1}"
                    # If only one pair (len(coords_match) == 2), point_specific_name remains the prefix (or empty)

                    if lat != 0 or lon != 0:  # Пропускаем нулевые координаты
                        result.append((point_specific_name.strip(),
                                      round(lon, 6), round(lat, 6)))
                    else:
                        logger.debug(
                            f"Пропуск нулевых ДМС координат: {lat=}, {lon=}")

                except ValueError as e:
                    reason = f"Ошибка конвертации ДМС координат в числа: {e}."
                    logger.warning(
                        f"{reason} Часть строки: '{part}'. Вся строка: '{coord_str[:50]}'")
                    return None, reason  # Ошибка конвертации делает строку аномальной
                except Exception as e:
                    reason = f"Непредвиденная ошибка при обработке ДМС координат: {e}."
                    logger.warning(
                        f"{reason} Часть строки: '{part}'. Вся строка: '{coord_str[:50]}'")
                    return None, reason  # Любая ошибка делает строку аномальной

    # Если мы дошли сюда, парсинг ДМС прошел без фатальных ошибок
    # Если были найдены маркеры '°', но не распознано ни одной валидной пары ДМС
    # Или если были части с ДМС, но все они дали (0,0) и были отброшены
    if '°' in coord_str and not has_valid_dms:
        # Это странно: есть градусы, но нет валидных координат. Возможно, ошибка формата, которую regex не поймал.
        reason = "Обнаружен маркер '°', но не найдено валидных пар ДМС координат."
        logger.warning(f"{reason} Строка: '{coord_str[:50]}'")
        # Считать ли это ошибкой или просто пустым результатом? ТЗ 2.1.1 - вернуть маркер ошибки.
        return None, reason

    # Check for anomalous coordinates
    if result and len(result) >= 3:
        is_anomalous, reason, anomalous_points = detect_coordinate_anomalies(
            result)
        if is_anomalous:
            return None, reason

    # Успешный парсинг (возможно, с пустым результатом, если координаты были (0,0) или не найдены)
    return result, None


def find_column_index(sheet, target_name: str) -> int:
    """Находит индекс столбца для заданного имени заголовка в строках 1-4."""
    for row in sheet.iter_rows(min_row=1, max_row=4, values_only=True):
        for idx, cell in enumerate(row):
            if cell and target_name.lower() in str(cell).lower():
                return idx
    return -1


def get_column_indices(sheet) -> dict:
    """Получает индексы всех необходимых столбцов."""
    columns = {
        "coord": "Место водопользования",
        "name": "№ п/п",
        "organ": "Уполномоченный орган",
        "additional_name": "Наименование водного объекта",
        "goal": "Цель водопользования",
        "vid": "Вид водопользования",
        "owner": "Наименование",
        "start_date": "Дата начала водопользования",
        "end_date": "Дата окончания водопользования"
    }
    indices = {key: find_column_index(sheet, value)
               for key, value in columns.items()}

    for key, value in indices.items():
        if value == -1:
            print(f"Столбец '{columns[key]}' не найден.")

    return indices


def create_kml_point(kml, name: str, coords: Tuple[float, float], description: str, color: str) -> None:
    """Создает точку KML с заданными параметрами."""
    point = kml.newpoint(name=name, coords=[coords])
    point.description = description
    point.style.iconstyle.color = color
    point.style.iconstyle.scale = 1.0
    point.style.labelstyle.scale = 0.8


def create_kml_from_coordinates(sheet, output_file: str = "output.kml", sort_numbers: List[int] = None) -> None:
    """Создает KML-файл из листа с координатами."""
    kml = simplekml.Kml()
    indices = get_column_indices(sheet)

    # Default min_row value
    min_row = 5

    # Check rows 2..5 using iter_rows()
    for row in sheet.iter_rows(min_row=2, max_row=5):
        cell = row[indices["coord"]] if indices["coord"] != -1 else None
        value = cell.value

        if isinstance(value, str) and ('м.' in value or '"' in value):
            min_row = cell.row  # Get the actual row number (3 or 4)
            break  # Stop at first match

    # Используем определенное значение min_row в цикле
    for row_idx, row in enumerate(sheet.iter_rows(min_row=min_row, values_only=True), start=min_row):
        coords_str = row[indices["coord"]] if indices["coord"] != -1 else None
        if not isinstance(coords_str, str):
            continue
        main_name = row[indices["name"]
                        ] if indices["name"] != -1 else f"Row {row_idx}"
        logger.info(f"------------")

        # Вызываем обновленную функцию парсинга
        coords_array, error_reason = parse_coordinates(coords_str)

        # Если parse_coordinates вернула ошибку, пропускаем строку (она будет обработана как аномальная в другом модуле)
        if error_reason is not None:
            # Логирование уже произошло внутри parse_coordinates
            logger.warning(
                f"Строка {row_idx} (№ п/п {main_name}) пропущена из-за ошибки парсинга: {error_reason}")
            continue  # Переходим к следующей строке Excel

        # Если coords_array это пустой список, значит парсинг прошел успешно, но валидных координат не найдено.
        # В этом случае просто пропускаем создание KML геометрии для этой строки.
        if not coords_array:
            logger.debug(
                f"Строка {row_idx} (№ п/п {main_name}) не содержит валидных координат для KML.")
            continue  # Нет точек для добавления в KML

        logger.info(
            f"Строка {row_idx} (№ п/п {main_name}): Распознано {len(coords_array)} точек.")

        if coords_array:  # Убедимся еще раз, что список не пуст
            color = generate_random_color()

            # Подготавливаем описание
            desc = []
            for key, column_name in [
                ("organ", "Уполномоченный орган"),
                ("additional_name", "Наименование водного объекта"),
                ("goal", "Цель водопользования"),
                ("vid", "Вид водопользования"),
                ("coord", "Место водопользования"),
                ("owner", "Владелец"),
                ("start_date", "Дата начала водопользования"),
                ("end_date", "Дата окончания водопользования")
            ]:
                if indices[key] != -1 and row[indices[key]]:
                    # Форматируем даты без времени, если это даты начала или окончания водопользования
                    if key in ["start_date", "end_date"] and hasattr(row[indices[key]], "date"):
                        # Если это объект datetime, берем только дату
                        date_value = row[indices[key]].date()
                        desc.append(f"{column_name}: {date_value}")
                    elif key in ["start_date", "end_date"] and isinstance(row[indices[key]], str):
                        # Если это строка, обрезаем время, если оно есть
                        date_str = row[indices[key]].split(" ")[0]
                        desc.append(f"{column_name}: {date_str}")
                    else:
                        desc.append(f"{column_name}: {row[indices[key]]}")
            description = '\n'.join(desc)

            # Проверяем, есть ли 16-й столбец

            # Проверяем, есть ли более 3 точек и 16-й столбец не равен нулю или пуст
            if len(coords_array) > 3 and row[indices["goal"]] != "Сброс сточных вод":
                logger.debug(
                    f"Строка {row_idx} (№ п/п {main_name}): Создание полигона")
                # Создаем полигон
                polygon = kml.newpolygon(name=f"№ п/п {main_name}")

                # Сортируем координаты только если main_name есть в sort_numbers
                if (sort_numbers and int(main_name) in sort_numbers) or len(coords_array) == 4:
                    sorted_coords = sort_coordinates(
                        [(lon, lat) for _, lon, lat in coords_array])
                else:
                    sorted_coords = [(lon, lat)
                                     for _, lon, lat in coords_array]

                polygon.outerboundaryis = sorted_coords
                polygon.style.linestyle.color = color
                polygon.style.linestyle.width = 3
                polygon.style.polystyle.color = simplekml.Color.changealphaint(
                    100, color)
                polygon.description = description
            else:
                # Создаем линию, если есть несколько точек и условия выполнены
                if len(coords_array) > 2 \
                        and all(name.startswith("точка") for name, _, _ in coords_array) \
                        and row[indices["goal"]] != "Сброс сточных вод":
                    logger.debug(
                        f"Строка {row_idx} (№ п/п {main_name}): Создание линии")
                    line = kml.newlinestring(name=f"№ п/п {main_name}",
                                             coords=[(lon, lat) for _, lon, lat in coords_array])
                    line.style.linestyle.color = color
                    line.style.linestyle.width = 3
                    line.description = description
                else:
                    # Создаем отдельные точки, если линия не была создана
                    index = 1
                    for point_name, lon, lat in coords_array:
                        logger.debug(f"  Точка: {point_name} ({lat}, {lon})")
                        # print(f"{lat}, {lon}")
                        if row[indices["goal"]] == "Сброс сточных вод":
                            full_name = f"№ п/п {main_name} - сброс {index}"
                        else:
                            full_name = f"№ п/п {main_name} - {point_name}" if point_name else f"№ п/п {main_name}"
                        create_kml_point(
                            kml, full_name, (lon, lat), description, color)
                        index += 1

    kml.save(output_file)
