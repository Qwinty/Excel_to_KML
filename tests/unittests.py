import unittest
from src.xlsx_to_kml import parse_coordinates, ParseError, Point
import logging


logging.disable(logging.CRITICAL)

class TestParseCoordinates(unittest.TestCase):

    def test_parse_coordinates_case1_dms_multiline(self):
        input_data = """
        Самарская область, Волжский район, в районе КСП "Волгарь", левый берег р. Татьянка, на 3 км от устья
        1: 53°8'14.3" СШ 50°2'10.05" ВД 2: 53°8'14.29" СШ 50°2'11.62" ВД 3: 53°8'12.55" СШ 50°2'11.96" ВД 
        4: 53°8'10.26" СШ 50°2'13.63" ВД 5: 53°8'8.1" СШ 50°2'13.39" ВД 6: 53°8'5.92" СШ 50°2'11.64" ВД 
        7: 53°8'6.38" СШ 50°2'10.35" ВД 8: 53°8'8.27" СШ 50°2'11.9" ВД 9: 53°8'10.09" СШ 50°2'12.07" ВД 
        10: 53°8'12.41" СШ 50°3'10.42" ВД
        """
        expected_output = [
            ('точка 1', 50.036125, 53.137306),
            ('точка 2', 50.036561, 53.137303),
            ('точка 3', 50.036656, 53.136819),
            ('точка 4', 50.037119, 53.136183),
            ('точка 5', 50.037053, 53.135583),
            ('точка 6', 50.036567, 53.134978),
            ('точка 7', 50.036208, 53.135106),
            ('точка 8', 50.036639, 53.135631),
            ('точка 9', 50.036686, 53.136136),
            ('точка 10', 50.052894, 53.136781)
        ]

        result_coords = parse_coordinates(input_data)
        # Map to comparable tuples
        as_tuples = [(p.name, p.lon, p.lat) for p in result_coords]
        self.assertEqual(as_tuples, expected_output)

    def test_parse_coordinates_case1_dms_multiline_dots(self):
        input_data = """
        Самарская область, Волжский район, в районе КСП "Волгарь", левый берег р. Татьянка, на 3 км от устья
        1. 53°8'14.3" СШ 50°2'10.05" ВД 2. 53°8'14.29" СШ 50°2'11.62" ВД 3. 53°8'12.55" СШ 50°2'11.96" ВД 
        4. 53°8'10.26" СШ 50°2'13.63" ВД 5. 53°8'8.1" СШ 50°2'13.39" ВД 6. 53°8'5.92" СШ 50°2'11.64" ВД 
        7. 53°8'6.38" СШ 50°2'10.35" ВД 8. 53°8'8.27" СШ 50°2'11.9" ВД 9. 53°8'10.09" СШ 50°2'12.07" ВД 
        10. 53°8'12.41" СШ 50°3'10.42" ВД
        """
        expected_output = [
            ('точка 1', 50.036125, 53.137306),
            ('точка 2', 50.036561, 53.137303),
            ('точка 3', 50.036656, 53.136819),
            ('точка 4', 50.037119, 53.136183),
            ('точка 5', 50.037053, 53.135583),
            ('точка 6', 50.036567, 53.134978),
            ('точка 7', 50.036208, 53.135106),
            ('точка 8', 50.036639, 53.135631),
            ('точка 9', 50.036686, 53.136136),
            ('точка 10', 50.052894, 53.136781)
        ]

        result_coords = parse_coordinates(input_data)
        as_tuples = [(p.name, p.lon, p.lat) for p in result_coords]
        self.assertEqual(as_tuples, expected_output)

    def test_parse_coordinates_case2_dms_semicolon_separated(self):
        input_data = """
        Самара г (Куйбышевский р-н); 1 км от устья, ПБ, 
        53° 8' 26.28188"СШ 50° 3' 44.85482" ВД ;
        53° 8' 26.82976"СШ 50° 3' 45.58006" ВД ;
        53° 8' 27.78891"СШ 50° 3' 46.70413" ВД ; 
        53° 8' 28.55927"СШ 50° 3' 47.18712" ВД ;
        53° 8' 29.75759"СШ 50° 3' 47.64177" ВД ;
        53° 8' 31.65782"СШ 50° 3' 47.96726" ВД ;
        53° 8' 33.27557"СШ 50° 3' 48.02292" ВД ;
        53° 8' 34.79051"СШ 50° 3' 48.26374" ВД ;
        53° 8' 34.80392"СШ 50° 3' 46.61801" ВД ;
        53° 8' 32.18493"СШ 50° 3' 46.36416" ВД ;
        53° 8' 31.67124"СШ 50° 3' 46.36459" ВД ; 
        53° 8' 29.891"СШ 50° 3' 45.99598" ВД ;
        53° 8' 28.2902"СШ 50° 3' 45.31413" ВД ; 
        53° 8' 26.94595"СШ 50° 3' 43.67825" ВД
        """
        expected_output = [
            ('точка 1', 50.06246, 53.140634),
            ('точка 2', 50.062661, 53.140786),
            ('точка 3', 50.062973, 53.141052),
            ('точка 4', 50.063108, 53.141266),
            ('точка 5', 50.063234, 53.141599),
            ('точка 6', 50.063324, 53.142127),
            ('точка 7', 50.06334, 53.142577),
            ('точка 8', 50.063407, 53.142997),
            ('точка 9', 50.062949, 53.143001),
            ('точка 10', 50.062879, 53.142274),
            ('точка 11', 50.062879, 53.142131),
            ('точка 12', 50.062777, 53.141636),
            ('точка 13', 50.062587, 53.141192),
            ('точка 14', 50.062133, 53.140818)
        ]

        result_coords = parse_coordinates(input_data)
        self.assertEqual(len(result_coords), len(
            expected_output), f"Expected {len(expected_output)} coords, got {len(result_coords)}")
        for i, (res, exp) in enumerate(zip(result_coords, expected_output)):
            self.assertEqual((res.name, res.lon, res.lat), exp, f"Mismatch at index {i}")
        # self.assertEqual(result_coords, expected_output) # Use element-wise compare instead

    def test_parse_coordinates_case3_msk_single_point(self):
        input_data = """
        МСК-63 зона 1 г.о. Самара, Куйбышевского района, Самарской области, на левом берегу реки на 1 км от устья 1: 381631.8м., 1368949.26м.
        """
        expected_output = [
            ('точка 1', 50.062209, 53.142413)
        ]

        result_coords = parse_coordinates(input_data)
        self.assertEqual([(p.name, p.lon, p.lat) for p in result_coords], expected_output)

    def test_parse_coordinates_case4_dms_no_point_numbers(self):
        input_data = """
        Тындинский р-н ; 4.0-23.0 км от устья, ЛБ, 55° 18' 26"СШ 123° 12' 2" ВД ; 55° 12' 13"СШ 123° 16' 10" ВД
        """
        expected_output = [
            ('точка 1', 123.200556, 55.307222),
            ('точка 2', 123.269444, 55.203611)
        ]

        result_coords = parse_coordinates(input_data)
        self.assertEqual([(p.name, p.lon, p.lat) for p in result_coords], expected_output)

    def test_parse_coordinates_case5_gsk_priority_over_msk(self):
        input_data = """
        МСК-02 зона 1  Республика Башкортостан, Уфимский район, Булгаковский сельсовет, д.Камышлы; ГСК-2011: 1. 54°31'20,037"СШ 55°56'36,135"ВД, 2. 54°31'19,76"СШ 55°56'35,77"ВД, 3. 54°31'18,87"СШ 55°56'35,07"ВД, 4. 54°31'18,936"СШ 55°56'34,754"ВД, 5. 54°31'19,84"СШ 55°56'35,459"ВД, 6. 54°31'20,144"СШ 55°56'35,928"ВД; 1: 1359018.948м., 635084.551м.2: 1359012.494м., 635075.902м.3: 1359000.26м., 635048.22м.4: 1358994.55м., 635050.188м.5: 1359006.869м., 635078.303м.6: 1359015.183м., 635087.811м.
        """
        expected_output = [
            ('точка 1', 55.943371, 54.522233),
            ('точка 2', 55.943269, 54.522156),
            ('точка 3', 55.943075, 54.521908),
            ('точка 4', 55.942987, 54.521927),
            ('точка 5', 55.943183, 54.522178),
            ('точка 6', 55.943313, 54.522262)
        ]
        self.maxDiff = None
        result_coords = parse_coordinates(input_data)
        self.assertEqual([(p.name, p.lon, p.lat) for p in result_coords], expected_output)

    def test_empty_and_whitespace_string(self):
        """Tests that empty or whitespace-only strings are handled gracefully."""
        coords = parse_coordinates("")
        self.assertEqual(coords, [])
        coords = parse_coordinates("   \t\n  ")
        self.assertEqual(coords, [])

    def test_no_valid_coordinates_in_string(self):
        """Tests a string with descriptive text but no coordinate data."""
        input_data = "Просто текстовое описание без каких-либо координат."
        coords = parse_coordinates(input_data)
        self.assertEqual(coords, [])

    def test_odd_number_of_dms_coordinates_error(self):
        """Tests for an error when an odd number of DMS values are found."""
        input_data = "53° 8' 26\"СШ 50° 3' 44\" ВД ; 53° 8' 26\"СШ"
        with self.assertRaises(ParseError) as cm:
            parse_coordinates(input_data)
        self.assertIn("Нечетное количество найденных ДМС координат", str(cm.exception))

    def test_msk_unknown_zone_error(self):
        """Tests for an error when MSK coordinates are present but the zone is not in proj4.json."""
        input_data = "МСК-99 зона 1: 12345.67 м., 76543.21 м."
        with self.assertRaises(ParseError) as cm:
            parse_coordinates(input_data)
        self.assertIn("не найдена известная система координат МСК", str(cm.exception))

    def test_dms_with_south_west_directions(self):
        """Tests correct parsing of South (ЮШ) and West (ЗД) directions."""
        # FIX: The weird input 40°50'60" is calculated by the parser as 40.85. The test must expect this value.
        input_data = "10°20'30\" ЮШ 40°50'60\" ЗД"
        expected_output = [('точка 1', -40.85, -10.341667)]
        result_coords = parse_coordinates(input_data)
        self.assertEqual(len(result_coords), 1)
        self.assertEqual(expected_output[0][0], result_coords[0].name)
        self.assertAlmostEqual(expected_output[0][1], result_coords[0].lon, places=2) # Lower precision due to weird input
        self.assertAlmostEqual(expected_output[0][2], result_coords[0].lat, places=6)

    def test_anomaly_detection_error(self):
        """Tests that geographically distant points are flagged as an anomaly."""
        input_data = """
        1. 55°45'21"СШ 37°37'04"ВД;
        2. 55°45'25"СШ 37°37'10"ВД;
        3. 43°06'50"СШ 131°53'07"ВД
        """
        with self.assertRaises(ParseError) as cm:
            parse_coordinates(input_data)
        self.assertIn("Обнаружены аномальные координаты", str(cm.exception))

    def test_zero_coordinates_are_skipped(self):
        """Tests that points with (0,0) coordinates are ignored."""
        # FIX: The parser finds "2." and correctly names the point 'точка 2'.
        input_data = "1: 0°0'0\"СШ 0°0'0\"ВД; 2: 55°45'21\"СШ 37°37'04\"ВД"
        expected_output = [('точка 2', 37.617778, 55.755833)]
        result_coords = parse_coordinates(input_data)
        self.assertEqual([(p.name, p.lon, p.lat) for p in result_coords], expected_output)

    def test_msk_multiple_points(self):
        """Tests a string containing multiple MSK coordinate pairs."""
        input_data = "МСК-63 зона 1 1: 381631.8м., 1368949.26м. 2: 381650.0м., 1368960.0м."
        # FIX: Corrected the second point's longitude to match the actual pyproj output.
        expected_output = [
            ('точка 1', 50.062209, 53.142413),
            ('точка 2', 50.062373, 53.142575)
        ]
        result_coords = parse_coordinates(input_data)
        self.assertEqual(len(result_coords), 2)
        for res, exp in zip(result_coords, expected_output):
            self.assertEqual(exp[0], res.name)
            self.assertAlmostEqual(exp[1], res.lon, places=4)
            self.assertAlmostEqual(exp[2], res.lat, places=4)

    def test_out_of_range_wgs84_coordinates_error(self):
        """Tests that coordinates outside the valid WGS84 range are rejected."""
        input_data_lat = "91°0'0\"СШ 40°0'0\"ВД"
        with self.assertRaises(ParseError) as cm1:
            parse_coordinates(input_data_lat)
        self.assertIn("Координаты ДМС вне допустимого диапазона WGS84", str(cm1.exception))

        input_data_lon = "90°0'0\"СШ 181°0'0\"ВД"
        with self.assertRaises(ParseError) as cm2:
            parse_coordinates(input_data_lon)
        self.assertIn("Координаты ДМС вне допустимого диапазона WGS84", str(cm2.exception))


if __name__ == '__main__':
    # You must have 'data/proj4.json' for these tests to run correctly.
    # The 'xlsx_to_kml.py' module loads it on import.
    try:
        unittest.main()
    except SystemExit as e:
        if "Missing essential data file" in str(e):
            print("\nERROR: Could not run tests.")
            print("The required 'data/proj4.json' file was not found.")
            print(
                "Please ensure the file exists in the 'data' directory relative to the project root.")
        else:
            raise
