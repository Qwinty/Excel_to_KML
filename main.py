import glob
import math
import re
from typing import List, Tuple
import simplekml
from openpyxl import load_workbook
import random

from pyproj import CRS, Transformer


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


def create_moscow_transformer():
    moscow_cs = CRS.from_proj4(
        "+proj=tmerc +lat_0=55.66666666667 +lon_0=37.5 +k=1 +x_0=16.098 +y_0=14.512 +ellps=bessel +towgs84=316.151,78.924,589.650,-1.57273,2.69209,2.34693,8.4507 +units=m +no_defs")
    return Transformer.from_crs(moscow_cs, "EPSG:4326", always_xy=True)


def create_msk50_zone1_transformer():
    msk50_zone1_cs = CRS.from_proj4(
        "+proj=tmerc +lat_0=0 +lon_0=35.48333333333 +k=1 +x_0=1250000 +y_0=-5712900.566 +ellps=krass +towgs84=23.57,-140.95,-79.8,0,0.35,0.79,-0.22 +units=m +no_defs")
    return Transformer.from_crs(msk50_zone1_cs, "EPSG:4326", always_xy=True)


def create_msk50_zone2_transformer():
    msk50_zone2_cs = CRS.from_proj4(
        "+proj=tmerc +lat_0=0 +lon_0=38.48333333333 +k=1 +x_0=2250000 +y_0=-5712900.566 +ellps=krass +towgs84=23.57,-140.95,-79.8,0,0.35,0.79,-0.22 +units=m +no_defs")
    return Transformer.from_crs(msk50_zone2_cs, "EPSG:4326", always_xy=True)


def create_msk63_zone1_transformer():
    msk63_zone1_cs = CRS.from_proj4(
        "+proj=tmerc +lat_0=0 +lon_0=49.03333333333 +k=1 +x_0=1300000 +y_0=-5509414.70 +ellps=krass +towgs84=23.57,-140.95,-79.8,0,0.35,0.79,-0.22 +units=m +no_defs")
    return Transformer.from_crs(msk63_zone1_cs, "EPSG:4326", always_xy=True)


def create_msk73_zone1_transformer():
    msk73_zone1_cs = CRS.from_proj4(
        "+proj=tmerc +lat_0=0 +lon_0=46.05 +k=1 +x_0=1300000 +y_0=-5514743.504 +ellps=krass +towgs84=23.57,-140.95,-79.8,0,0.35,0.79,-0.22 +units=m +no_defs")
    return Transformer.from_crs(msk73_zone1_cs, "EPSG:4326", always_xy=True)


def create_msk73_zone2_transformer():
    msk73_zone2_cs = CRS.from_proj4(
        "+proj=tmerc +lat_0=0 +lon_0=49.05 +k=1 +x_0=2300000 +y_0=-5514743.504 +ellps=krass +towgs84=23.57,-140.95,-79.8,0,0.35,0.79,-0.22 +units=m +no_defs")
    return Transformer.from_crs(msk73_zone2_cs, "EPSG:4326", always_xy=True)


moscow_transformer = create_moscow_transformer()
msk50_zone1_transformer = create_msk50_zone1_transformer()
msk50_zone2_transformer = create_msk50_zone2_transformer()
msk63_zone1_transformer = create_msk63_zone1_transformer()
msk73_zone1_transformer = create_msk73_zone1_transformer()
msk73_zone2_transformer = create_msk73_zone2_transformer()


def process_coordinates(input_string, transformer):
    coordinates = re.findall(r'(\d+):\s*([-\d.]+)м\.,\s*([-\d.]+)м\.', input_string)
    results = []
    for _, x, y in coordinates:
        if float(x) == 0 and float(y) == 0:
            continue
        lon, lat = transformer.transform(float(y), float(x))
        results.append((f"точка {_}", round(lon, 6), round(lat, 6)))
    return results


def parse_coordinates(coord_str: str) -> List[Tuple[str, float, float]]:
    """Parse coordinate string and return list of (name, longitude, latitude) tuples."""
    if not coord_str:
        print(f"Skipping empty string")
        return []

    if "Московская СК" in coord_str:
        return process_coordinates(coord_str, moscow_transformer)

    if "МСК-50 зона 1" in coord_str:
        return process_coordinates(coord_str, msk50_zone1_transformer)

    if "МСК-50 зона 2" in coord_str:
        return process_coordinates(coord_str, msk50_zone2_transformer)

    if "МСК-63 зона 1" in coord_str:
        return process_coordinates(coord_str, msk63_zone1_transformer)

    if "МСК-73 зона 1" in coord_str:
        return process_coordinates(coord_str, msk73_zone1_transformer)

    if "МСК-73 зона 2" in coord_str:
        return process_coordinates(coord_str, msk73_zone2_transformer)

    if '°' not in coord_str:
        print(f"Skipping string without coordinates: '{coord_str}'")
        return []

    parts = coord_str.split(';')
    result = []

    for part in [p.strip() for p in parts]:
        name = ""
        coords = re.findall(r'(\d+)°\s*(\d+)\'\s*(\d+(?:[.,]\d+)?)"', part)

        if not coords:
            # print(f"Skipping part without coordinates: '{part}'")
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

            if lat > 100:
                lat = lat / 10
            if lon > 100:
                lon = lon / 10

            if lat != 0 or lon != 0:
                result.append((name.strip(), round(lon, 6), round(lat, 6)))
        elif len(coords) > 2:
            for i in range(0, len(coords), 2):
                if i + 1 >= len(coords):
                    break  # Handle the case where we have an odd number of coordinates

                # Get latitude and longitude pairs
                lat_deg, lat_min, lat_sec = coords[i]
                lon_deg, lon_min, lon_sec = coords[i + 1]

                # Convert latitude and longitude to decimal degrees
                lat = int(lat_deg) + int(lat_min) / 60 + float(lat_sec.replace(',', '.')) / 3600
                lon = int(lon_deg) + int(lon_min) / 60 + float(lon_sec.replace(',', '.')) / 3600

                # Check for direction (currently assuming Northern/Eastern hemisphere, adjust for South/West if needed)
                # For example, you could check for "ЮШ" or "ЗД" in the original string to set negative values if necessary

                # Append result as (point_name, longitude, latitude)
                point_name = f"точка {i // 2 + 1}"  # Each pair is a new "точка"
                if lat != 0 or lon != 0:
                    result.append((point_name, round(lon, 6), round(lat, 6)))

    return result


def find_column_index(sheet, target_name: str) -> int:
    """Find the column index for a given header name across rows 3 and 4."""
    for row in sheet.iter_rows(min_row=3, max_row=4, values_only=True):
        for idx, cell in enumerate(row):
            if cell and target_name.lower() == str(cell).lower():
                return idx
    return -1


def get_column_indices(sheet) -> dict:
    """Get indices for all required columns."""
    columns = {
        "coord": "Место водопользования, координаты",
        "name": "№ п/п",
        "organ": "Уполномоченный орган",
        "additional_name": "Наименование водного объекта, его код",
        "goal": "Цель водопользования",
        "vid": "Вид водопользования",
        "owner": "Наименование",
    }
    indices = {key: find_column_index(sheet, value) for key, value in columns.items()}

    for key, value in indices.items():
        if value == -1:
            print(f"Column '{columns[key]}' not found.")

    return indices


def create_kml_point(kml, name: str, coords: Tuple[float, float], description: str, color: str) -> None:
    """Create a KML point with given parameters."""
    point = kml.newpoint(name=name, coords=[coords])
    point.description = description
    point.style.iconstyle.color = color
    point.style.iconstyle.scale = 1.0
    point.style.labelstyle.scale = 0.8


def create_kml_from_coordinates(sheet, output_file: str = "output.kml", sort_numbers: List[int] = None) -> None:
    """Create KML file from worksheet with coordinates."""
    kml = simplekml.Kml()
    indices = get_column_indices(sheet)

    for row in sheet.iter_rows(min_row=5, values_only=True):
        coords_str = row[indices["coord"]] if indices["coord"] != -1 else None
        if not isinstance(coords_str, str):
            continue

        main_name = row[indices["name"]] if indices[
                                                "name"] != -1 else f"Row {sheet.iter_rows(min_row=5, max_row=sheet.max_row).index(row) + 5}"
        coords_array = parse_coordinates(coords_str)
        print(f"------\n№ п/п {main_name} | String:", coords_str)
        print(f"Parsed {len(coords_array)} points")

        if coords_array:
            color = generate_random_color()

            # Prepare description
            desc = []
            for key, column_name in [
                ("organ", "Уполномоченный орган"),
                ("additional_name", "Наименование водного объекта, его код"),
                ("goal", "Цель водопользования"),
                ("vid", "Вид водопользования"),
                ("coord", "Место водопользования, координаты"),
                ("owner", "Владелец")
            ]:
                if indices[key] != -1:
                    desc.append(f"{column_name}: {row[indices[key]]}")
            description = '\n'.join(desc)

            # Check if there are more than 3 points and the 16th column is not zero or empty
            if len(coords_array) > 3 and row[15] not in (0, None, ""):
                print("Creating polygon")
                # Create a polygon
                polygon = kml.newpolygon(name=f"№ п/п {main_name}")

                # Sort coordinates only if main_name is in sort_numbers
                if (sort_numbers and main_name in sort_numbers) or len(coords_array) == 4:
                    sorted_coords = sort_coordinates([(lon, lat) for _, lon, lat in coords_array])
                else:
                    sorted_coords = [(lon, lat) for _, lon, lat in coords_array]

                polygon.outerboundaryis = sorted_coords
                polygon.style.linestyle.color = color
                polygon.style.linestyle.width = 3
                polygon.style.polystyle.color = simplekml.Color.changealphaint(100, color)
                polygon.description = description
                [print(f"{lat}, {lon}") for lon, lat in sorted_coords]
            else:
                # Create a line if there are multiple points
                if len(coords_array) > 2 \
                        and all(name.startswith("точка") for name, _, _ in coords_array) \
                        and row[indices["goal"]] != "Сброс сточных вод":
                    line = kml.newlinestring(name=f"№ п/п {main_name}",
                                             coords=[(lon, lat) for _, lon, lat in coords_array])
                    line.style.linestyle.color = color
                    line.style.linestyle.width = 3
                    line.description = description

                # Create individual points
                index = 1
                for point_name, lon, lat in coords_array:
                    print(f"{lat}, {lon}")
                    if row[indices["goal"]] == "Сброс сточных вод":
                        full_name = f"№ п/п {main_name} - сброс {index}"
                    else:
                        full_name = f"№ п/п {main_name} - {point_name}" if point_name else f"№ п/п {main_name}"
                    create_kml_point(kml, full_name, (lon, lat), description, color)

    kml.save("output/" + output_file)


def choose_file() -> str:
    """Prompt user to choose an Excel file from the current directory."""
    files = glob.glob("*.xlsx")

    if not files:
        print("No Excel files found in the current directory.")
        return None
    for i, file in enumerate(files, 1):
        print(f"{i}. {file}")

    while True:
        try:
            return files[int(input("Choose a file number: ")) - 1]
        except (ValueError, IndexError):
            print("Invalid input. Please enter a valid number.")


def main():
    file_name = choose_file()
    if not file_name:
        return
    workbook = load_workbook(filename=file_name, data_only=True)

    # Specify which "№ п/п" values should have their coordinates sorted
    sort_numbers = []  # Add the specific numbers you want to sort
    filename = file_name.rsplit(".", 1)[0] + ".kml"
    create_kml_from_coordinates(workbook.active, output_file=filename, sort_numbers=sort_numbers)


if __name__ == '__main__':
    # test = parse_coordinates("""МСК-63 зона 1 г.о. Самара, Куйбышевского района, Самарской области, на левом берегу реки на 1 км от устья 1: 381631.8м., 1368949.26м.""")
    # print(test)
    main()
