import unittest

import shapely.geometry
from shapely.geometry import Polygon


class MyTestCase(unittest.TestCase):
    def test_boundary(self):
        coords = [(0, 0), (1, 1), (1, 0)]
        poly1 = shapely.geometry.Polygon(coords)
        poly2 = shapely.geometry.Point((2, 2))
        minx, miny, maxx, maxy = poly1.union(poly2).bounds
        print(minx, miny, maxx, maxy)
        self.assertIsInstance(shapely.geometry.box(minx, miny, maxx, maxy), shapely.geometry.Polygon)



if __name__ == '__main__':
    unittest.main()
