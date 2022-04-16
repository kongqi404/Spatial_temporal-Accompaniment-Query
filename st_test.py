import unittest

import shapely.geometry


class MyTestCase(unittest.TestCase):
    def test_read_text(self):
        self.assertIsInstance(shapely.geometry.Point,)


if __name__ == '__main__':
    unittest.main()
