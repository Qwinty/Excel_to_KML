import glob
import json
import re
from typing import List, Tuple
import simplekml
from openpyxl import load_workbook

from pyproj import CRS, Transformer

from utils import *


def create_transformer(proj4_str: str) -> Transformer:
    """Создает трансформер из заданной строки Proj4 в WGS84."""
    crs = CRS.from_proj4(proj4_str)
    return Transformer.from_crs(crs, "EPSG:4326", always_xy=True)


# Определяем строки Proj4
proj4_strings = json.load(open("data/proj4.json", "r", encoding="utf-8"))

# Создаем трансформеры
transformers = {name: create_transformer(proj4) for name, proj4 in proj4_strings.items()}


def process_coordinates(input_string, transformer):
    # Извлекаем координаты из строки
    coordinates = re.findall(r'(\d+):\s*([-\d.]+)\s*м\.,\s*([-\d.]+)\s*м\.', input_string)
    results = []
    for _, x, y in coordinates:
        if float(x) == 0 and float(y) == 0:
            continue
        lon, lat = transformer.transform(float(y), float(x))
        results.append((f"точка {_}", round(lon, 6), round(lat, 6)))
    return results


def parse_coordinates(coord_str: str) -> List[Tuple[str, float, float]]:
    """Парсит строку с координатами и возвращает список кортежей (имя, долгота, широта)."""
    if not coord_str:
        print(f"Пропуск пустой строки")
        return []

    for key, transformer in transformers.items():
        # print(key)
        if key in coord_str:
            return process_coordinates(coord_str, transformer)

    if '°' not in coord_str:
        print(f"Пропуск строки без координат: '{coord_str}'")
        return []

    parts = coord_str.split(';')
    result = []

    for part in [p.strip() for p in parts]:
        name = ""
        coords = re.findall(r'(\d+)°\s*(\d+)\'\s*(\d+(?:[.,]\d+)?)"', part)

        if not coords:
            # print(f"Пропуск части без координат: '{part}'")
            continue

        if "выпуск" in part.lower():
            match = re.search(r'выпуск №\s*(\d+)', part, re.IGNORECASE)
            name = f"выпуск №{match.group(1)}" if match else part
        elif "точка" in part.lower():
            match = re.search(r'точка\s*(\d+)', part, re.IGNORECASE)
            name = f"точка {match.group(1)}" if match else part

        if len(coords) == 2:
            lat = sum(float(x.replace(',', '.')) / (60 ** i) for i, x in enumerate(coords[0]))
            lon = sum(float(x.replace(',', '.')) / (60 ** i) for i, x in enumerate(coords[1]))
            lat = -lat if "ЮШ" in part or "S" in part else lat
            lon = -lon if "ЗД" in part or "W" in part else lon

            # if lat > 100:
            #     lat = lat / 10
            # if lon > 100:
            #     lon = lon / 10

            if lat != 0 or lon != 0:
                result.append((name.strip(), round(lon, 6), round(lat, 6)))
        elif len(coords) > 2:
            for i in range(0, len(coords), 2):
                if i + 1 >= len(coords):
                    break  # Обрабатываем случай, когда количество координат нечетное

                # Получаем пары широты и долготы
                lat_deg, lat_min, lat_sec = coords[i]
                lon_deg, lon_min, lon_sec = coords[i + 1]

                # Конвертируем широту и долготу в десятичные градусы
                lat = int(lat_deg) + int(lat_min) / 60 + float(lat_sec.replace(',', '.')) / 3600
                lon = int(lon_deg) + int(lon_min) / 60 + float(lon_sec.replace(',', '.')) / 3600

                # Проверяем направление (по умолчанию предполагается северное/восточное полушарие, при необходимости корректируем для южного/западного)

                # Добавляем результат как (имя_точки, долгота, широта)
                point_name = f"точка {i // 2 + 1}"  # Каждая пара — новая "точка"
                if lat != 0 or lon != 0:
                    result.append((point_name, round(lon, 6), round(lat, 6)))

    return result


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
    }
    indices = {key: find_column_index(sheet, value) for key, value in columns.items()}

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

    # Проверяем ячейку E4 на наличие определенных символов
    cell_e4 = sheet['E4'].value
    min_row = 5  # значение по умолчанию

    if isinstance(cell_e4, str) and ('м.' in cell_e4 or '"' in cell_e4):
        min_row = 4

    # Используем определенное значение min_row в цикле
    for row in sheet.iter_rows(min_row=min_row, values_only=True):
        coords_str = row[indices["coord"]] if indices["coord"] != -1 else None
        if not isinstance(coords_str, str):
            continue
        main_name = row[indices["name"]] if indices[
                                                "name"] != -1 else f"Row {sheet.iter_rows(min_row=5, max_row=sheet.max_row).index(row) + 5}"
        print(f"------\n№ п/п {main_name} | Строка:", coords_str)
        coords_array = parse_coordinates(coords_str)
        print(f"Распознано {len(coords_array)} точек")

        if coords_array:
            color = generate_random_color()

            # Подготавливаем описание
            desc = []
            for key, column_name in [
                ("organ", "Уполномоченный орган"),
                ("additional_name", "Наименование водного объекта"),
                ("goal", "Цель водопользования"),
                ("vid", "Вид водопользования"),
                ("coord", "Место водопользования"),
                ("owner", "Владелец")
            ]:
                if indices[key] != -1:
                    desc.append(f"{column_name}: {row[indices[key]]}")
            description = '\n'.join(desc)

            # Проверяем, есть ли 16-й столбец

            # Проверяем, есть ли более 3 точек и 16-й столбец не равен нулю или пуст
            if len(coords_array) > 3:
                print("Создание полигона")
                # Создаем полигон
                polygon = kml.newpolygon(name=f"№ п/п {main_name}")

                # Сортируем координаты только если main_name есть в sort_numbers
                if (sort_numbers and int(main_name) in sort_numbers) or len(coords_array) == 4:
                    sorted_coords = sort_coordinates([(lon, lat) for _, lon, lat in coords_array])
                else:
                    sorted_coords = [(lon, lat) for _, lon, lat in coords_array]

                polygon.outerboundaries = sorted_coords
                polygon.style.linestyle.color = color
                polygon.style.linestyle.width = 3
                polygon.style.polystyle.color = simplekml.Color.changealphaint(100, color)
                polygon.description = description
                [print(f"{lat}, {lon}") for lon, lat in sorted_coords]
            else:
                # Создаем линию, если есть несколько точек и условия выполнены
                if len(coords_array) > 2 \
                        and all(name.startswith("точка") for name, _, _ in coords_array) \
                        and row[indices["goal"]] != "Сброс сточных вод":
                    line = kml.newlinestring(name=f"№ п/п {main_name}",
                                             coords=[(lon, lat) for _, lon, lat in coords_array])
                    line.style.linestyle.color = color
                    line.style.linestyle.width = 3
                    line.description = description
                else:
                    # Создаем отдельные точки, если линия не была создана
                    index = 1
                    for point_name, lon, lat in coords_array:
                        print(f"{lat}, {lon}")
                        if row[indices["goal"]] == "Сброс сточных вод":
                            full_name = f"№ п/п {main_name} - сброс {index}"
                        else:
                            full_name = f"№ п/п {main_name} - {point_name}" if point_name else f"№ п/п {main_name}"
                        create_kml_point(kml, full_name, (lon, lat), description, color)

    kml.save(output_file)