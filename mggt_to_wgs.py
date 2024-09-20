import re
from pyproj import Transformer, CRS


def process_moscow_coordinates(input_string):
    def parse_coordinates(s):
        return re.findall(r'(\d+):\s*([-\d.]+)м\.,\s*([-\d.]+)м\.', s)

    def create_transformer():
        moscow_cs = CRS.from_proj4(
            "+proj=tmerc +lat_0=55.66666666667 +lon_0=37.5 +k=1 +x_0=16.098 +y_0=14.512 +ellps=bessel +towgs84=316.151,78.924,589.650,-1.57273,2.69209,2.34693,8.4507 +units=m +no_defs")
        return Transformer.from_crs(moscow_cs, "EPSG:4326", always_xy=True)

    coordinates = parse_coordinates(input_string)
    transformer = create_transformer()

    results = []
    for _, x, y in coordinates:
        lon,lat = transformer.transform(float(y), float(x))
        results.append((lat, lon))
        print(x,y, sep=', ')
    return results


# Example usage (can be removed when importing)
if __name__ == "__main__":
    input_string = """МСК-50 зона 2 Московская область, г.о. Красногорск, деревня Мякинино 1: 474430.22м., 2179569.51м.2: 474432.64м., 2179571.83м.3: 474430.13м., 2179596.9м.4: 474419.69м., 2179605.17м.5: 474401.72м., 2179615.6м.6: 474390.54м., 2179627.6м.7: 474385.7м., 2179639.39м.8: 474373.94м., 2179634.59м.9: 474387.66м., 2179611.56м.10: 474388.14м., 2179606.85м.11: 474399.92м., 2179587.07м."""
    transformed_coordinates = process_moscow_coordinates(input_string)

    print("Результат преобразования:")
    for lat, lon in transformed_coordinates:
        print(f"{lat:.9f}, {lon:.9f}")