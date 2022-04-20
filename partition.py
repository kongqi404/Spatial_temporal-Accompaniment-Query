import pyspark
import shapely.geometry

from extractor import STExtractor


class GlobalSpatialInfo:
    def __init__(self):
        self.env = shapely.geometry.Polygon()
        self.count = 0

    def add_geom(self, geom):
        min_x, min_y, max_x, max_y = self.env.union(geom).bounds
        self.env = shapely.geometry.box(min_x, min_y, max_x, max_y)
        self.count += 1
        return self

    def combine(self, other):
        min_x, min_y, max_x, max_y = self.env.union(other.env).bounds
        self.env = shapely.geometry.box(min_x, min_y, max_x, max_y)
        self.count += other.count
        return self

    def get_env(self):
        return self.env

    def get_count(self):
        return self.count


class GlobalSTInfo(GlobalSpatialInfo):
    def __init__(self):
        super().__init__()
        self.start_time = None
        self.end_time = None

    def add_time(self, time_range):
        self.expand_time(time_range[0], time_range[1])
        return self

    def combine(self, other):
        if other.start_time is not None:
            self.expand_time(other.start_time, other.end_time)
        return self

    def get_time_range(self):
        return self.start_time, self.end_time

    def expand_time(self, start, end):
        assert not start.after(end)
        if self.start_time is None:
            self.start_time = start
            self.end_time = end
        else:
            if start.before(self.start_time):
                self.start_time = start
            if end.after(self.end_time):
                self.end_time = end


def do_statistic(rdd: pyspark.RDD, extractor: STExtractor) -> GlobalSTInfo:
    def add(global_info: GlobalSTInfo, row):
        return global_info.add_geom(row[0]).__class__, global_info.add_time(row[1])

    def combine(global_info: GlobalSTInfo, other: GlobalSTInfo):
        return global_info.combine(other)

    return rdd.aggregate(GlobalSTInfo, add, combine)
