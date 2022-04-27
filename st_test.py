import unittest

import pyspark
import shapely.geometry

from partition import GlobalSpatialInfo


class MyTestCase(unittest.TestCase):

    def test_boundary(self):
        coords = [(0, 0), (1, 1), (1, 0)]
        poly1 = shapely.geometry.Polygon(coords)
        poly2 = shapely.geometry.Point((2, 2))
        minx, miny, maxx, maxy = poly1.union(poly2).bounds
        print(minx, miny, maxx, maxy)
        self.assertIsInstance(shapely.geometry.box(minx, miny, maxx, maxy), shapely.geometry.Polygon)

    def test_global_spatial_info(self):
        coords = [(0, 0), (1, 1), (1, 0)]
        poly = shapely.geometry.Polygon(coords)
        test_info = GlobalSpatialInfo()
        test_info.add_geom(poly)
        self.assertIsInstance(test_info.get_env(), shapely.geometry.Polygon)

    def test_extractor(self):
        pass


if __name__ == '__main__':
    unittest.main()
