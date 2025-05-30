import unittest
# Replace 'your_module' with the actual module name
from xlsx_to_kml import parse_coordinates


class TestParseCoordinates(unittest.TestCase):

    def test_parse_coordinates_case1(self):
        input_data = """
        Самарская область, Волжский район, в районе КСП "Волгарь", левый берег р. Татьянка, на 3 км от устья
        1: 53°8'14.3" СШ 50°2'10.05" ВД 2: 53°8'14.29" СШ 50°2'11.62" ВД 3: 53°8'12.55" СШ 50°2'11.96" ВД 
        4: 53°8'10.26" СШ 50°2'13.63" ВД 5: 53°8'8.1" СШ 50°2'13.39" ВД 6: 53°8'5.92" СШ 50°2'11.64" ВД 
        7: 53°8'6.38" СШ 50°2'10.35" ВД 8: 53°8'8.27" СШ 50°2'11.9" ВД 9: 53°8'10.09" СШ 50°2'12.07" ВД 
        10: 53°8'12.41" СШ 50°3'10.42" ВД
        """
        expected_output = [
            ('т.1', 50.036125, 53.137306),
            ('т.2', 50.036561, 53.137303),
            ('т.3', 50.036656, 53.136819),
            ('т.4', 50.037119, 53.136183),
            ('т.5', 50.037053, 53.135583),
            ('т.6', 50.036567, 53.134978),
            ('т.7', 50.036208, 53.135106),
            ('т.8', 50.036639, 53.135631),
            ('т.9', 50.036686, 53.136136),
            ('т.10', 50.052894, 53.136781)
        ]

        result_coords, error_reason = parse_coordinates(input_data)
        self.assertIsNone(
            error_reason, f"Expected no error, but got: {error_reason}")
        self.assertEqual(result_coords, expected_output)

    def test_parse_coordinates_case2(self):
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
            ('', 50.06246, 53.140634),
            ('', 50.062661, 53.140786),
            ('', 50.062973, 53.141052),
            ('', 50.063108, 53.141266),
            ('', 50.063234, 53.141599),
            ('', 50.063324, 53.142127),
            ('', 50.06334, 53.142577),
            ('', 50.063407, 53.142997),
            ('', 50.062949, 53.143001),
            ('', 50.062879, 53.142274),
            ('', 50.062879, 53.142131),
            ('', 50.062777, 53.141636),
            ('', 50.062587, 53.141192),
            ('', 50.062133, 53.140818)
        ]

        result_coords, error_reason = parse_coordinates(input_data)
        self.assertIsNone(
            error_reason, f"Expected no error, but got: {error_reason}")
        self.assertEqual(len(result_coords), len(
            expected_output), f"Expected {len(expected_output)} coords, got {len(result_coords)}")
        for i, (res, exp) in enumerate(zip(result_coords, expected_output)):
            self.assertEqual(res, exp, f"Mismatch at index {i}")
        # self.assertEqual(result_coords, expected_output) # Use element-wise compare instead

    def test_parse_coordinates_case3(self):
        input_data = """
        МСК-63 зона 1 г.о. Самара, Куйбышевского района, Самарской области, на левом берегу реки на 1 км от устья 1: 381631.8м., 1368949.26м.
        """
        expected_output = [
            ('точка 1', 50.062209, 53.142413)
        ]

        result_coords, error_reason = parse_coordinates(input_data)
        self.assertIsNone(
            error_reason, f"Expected no error, but got: {error_reason}")
        self.assertEqual(result_coords, expected_output)

    def test_parse_coordinates_case4(self):
        input_data = """
        Тындинский р-н ; 4.0-23.0 км от устья, ЛБ, 55° 18' 26"СШ 123° 12' 2" ВД ; 55° 12' 13"СШ 123° 16' 10" ВД
        """
        expected_output = [
            ('', 123.200556, 55.307222),
            ('', 123.269444, 55.203611)
        ]

        result_coords, error_reason = parse_coordinates(input_data)
        self.assertIsNone(
            error_reason, f"Expected no error, but got: {error_reason}")
        self.assertEqual(result_coords, expected_output)


if __name__ == '__main__':
    unittest.main()
