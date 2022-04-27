from typing import Type

import pyspark
import shapely.geometry

import all_utils
from extractor import STExtractor


class GlobalSpatialInfo:
    def __init__(self):
        self.env = shapely.geometry.Polygon()
        self.count = 0

    def add_geom(self, geom):
        self.env = all_utils.transfer_bounds_to_box(self.env.union(geom).bounds)
        self.count += 1

    def combine(self, other):
        self.env = all_utils.transfer_bounds_to_box(self.env.union(other.env).bounds)
        self.count += other.count

    def get_env(self):
        return self.env

    def get_count(self):
        return self.count


class GlobalSTInfo(GlobalSpatialInfo):
    def __init__(self):
        super().__init__()
        self.start_time = None
        self.end_time = None

    def add_time(self, time_range: tuple):
        self.expand_time(time_range[0], time_range[1])

    def combine(self, other):
        if other.start_time is not None:
            self.expand_time(other.start_time, other.end_time)

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


def do_statistic(rdd: pyspark.RDD):
    """

    :param rdd: row:(geom,(start,end))
    :return:
    """
    def add(global_info, row):
        global_info.add_geom(row[0])
        global_info.add_time(row[1])
        return global_info

    def combine(global_info, other):
        global_info.combine(other)
        return global_info

    return rdd.aggregate(GlobalSTInfo, add, combine)
