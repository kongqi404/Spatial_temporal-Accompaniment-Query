import shapely.geometry


class STExtractor:

    @staticmethod
    def geom(row: (shapely.geometry.Polygon, (int, int))) -> shapely.geometry.Polygon:
        return row[0]

    @staticmethod
    def start_time(row: (shapely.geometry.GeometryCollection, (int, int))):
        return row[1][0]

    @staticmethod
    def end_time(row):
        return row[1][1]
