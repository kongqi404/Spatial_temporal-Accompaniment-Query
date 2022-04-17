import heapq
import math

import shapely.geometry

import time_utils


class TRCBasedBins:
    def __init__(self, bin_num, k):
        self.k = k
        self.bin_num = bin_num
        self.total = None
        self.start_time = None
        self.end_time = None
        self.span_milli = None
        self.left_max_sums = []
        self.right_min_sums = []

    def build(self, time_ranges: list[(int, int)]):
        self.total = len(time_ranges)
        self.start_time = min(map(lambda x: x[0], time_ranges))
        self.end_time = max(map(lambda x: x[1], time_ranges))
        self.span_milli = math.ceil((self.end_time - self.start_time + 1) / self.bin_num)
        start_milli = self.start_time

        min_nums = [0] * self.bin_num
        for time_ in map(lambda x: x[0], time_ranges):
            index = int((time_ - start_milli) / self.span_milli)
            min_nums[index] += 1
        max_nums = [0] * self.bin_num
        for time_ in map(lambda x: x[1], time_ranges):
            index = int((time_ - start_milli) / self.span_milli)
            max_nums[index] += 1

        def fold_left(lis: list, res, op):
            for i in lis:
                res = op(res, i)
            return res


class GlobalNode:
    partition_id = -1

    def set_partition_id(self, _id):
        self.partition_id = _id

    def get_partition_id(self, _id):
        return self.partition_id


class QuadNode(GlobalNode):
    def __init__(self, env: shapely.geometry.Polygon) -> None:
        self.env = env
        self.centre = shapely.geometry.Point(env.centroid)
        self.sub_nodes = []

    def split(self, samples: list[shapely.geometry.Polygon], samples_queue: list) -> None:
        for index in range(0, 4):
            assert self.sub_nodes[index] is None
            self.sub_nodes[index] = QuadNode(self.create_sub_env(index))
            sub_env = self.sub_nodes[index].env
            sub_samples = filter(lambda x: sub_env.intersects(x), samples)
            heapq.heappush(samples_queue, (0, (self.sub_nodes[index], sub_samples)))

    def find_nearest_id(self, point: shapely.geometry.Point, _filter) -> int:
        if self.partition_id != -1:
            return self.partition_id if _filter(self) else -1
        if point.x > self.centre.x:
            if point.y > self.centre.y:
                sub_index = 3
            else:
                sub_index = 1
        else:
            if point.y > self.centre.y:
                sub_index = 2
            else:
                sub_index = 0
        nearest_id = self.sub_nodes[sub_index].find_nearest_id(point, _filter)
        if nearest_id == -1:
            for index in range(4):
                if index != sub_index:
                    nearest_id = self.sub_nodes[index].find_nearest_id(point, _filter)
                    if nearest_id != -1:
                        return nearest_id
            return -1
        else:
            return nearest_id

    def find_intersect_ids(self, query_env: shapely.geometry.Polygon, id_collector: list[int]) -> None:
        if self.env.intersects(query_env):
            if self.partition_id != -1:
                id_collector.append(self.partition_id)
            else:
                for sub_node in self.sub_nodes:
                    sub_node.find_intersect_ids(query_env, id_collector)

    def create_sub_env(self, index: int) -> shapely.geometry.Polygon:
        min_x = 0
        max_x = 0
        min_y = 0
        max_y = 0
        if index == 0:
            min_x, max_x, min_y, max_y = self.env.bounds[0], self.centre.x, self.env.bounds[1], self.centre.y
        elif index == 1:
            min_x, max_x, min_y, max_y = self.centre.x, self.env.bounds[2], self.env.bounds[1], self.centre.y
        elif index == 2:
            min_x, max_x, min_y, max_y = self.env.bounds[0], self.centre.x, self.centre.y, self.env.bounds[3]
        elif index == 3:
            min_x, max_x, min_y, max_y = self.centre.x, self.env.bounds[2], self.centre.y, self.env.bounds[3]
        return shapely.geometry.box(min_x, min_y, max_x, max_y)


class GlobalQuad:
    def __init__(self, spatial_bound) -> None:
        self.spatial_bound = spatial_bound
        self.root = QuadNode(self.spatial_bound)
        self.leaf_nodes = []

    def build(self, samples, sample_rate, beta, k) -> None:
        def comparator(x):
            return -len(x)

        max_num_per_partition = max(len(samples) // beta, math.ceil(4 * sample_rate * k))
        priority_queue = []
        heapq.heappush(priority_queue, (comparator(samples), (self.root, samples)))
        # priority_queue.put((comparator(samples), (self.root, samples)))
        while len(priority_queue) < beta:
            if len(priority_queue[0][1]) < max_num_per_partition:
                self.leaf_nodes = [node[0] for node in priority_queue]
                return
            else:
                max_node, max_samples = priority_queue[0][1]
                max_node.split(max_samples, priority_queue)
        self.leaf_nodes = map(lambda x: x[1][0], priority_queue)

    def find_nearest_id(self, query_centre: shapely.geometry.Point, time_filter) -> int:
        return self.root.find_nearest_id(query_centre, time_filter)

    def find_intersect_ids(self, query_env: shapely.geometry.Polygon, id_collector: list[int]):
        self.root.find_intersect_ids(query_env, id_collector)

    def assign_partition_id(self, base_id: int) -> int:
        for i in range(len(self.leaf_nodes)):
            self.leaf_nodes[i].set_partition_id(base_id + i)
        return base_id + len(self.leaf_nodes)

    def get_leaf_env(self, index: int) -> shapely.geometry.Polygon:
        return self.leaf_nodes[index].env


class GlobalRTree:
    """
    unfinished!!!!
    """

    def __init__(self, node_capacity: int):
        self.root = []
        self.leaf_nodes = []
        self.node_capacity = node_capacity

    def build(self, samples: list[shapely.geometry.Polygon]):
        assert len(samples) > 0
        self.leaf_nodes = []
        self.root = []


class TimePeriod:

    def __init__(self, period_start, period_end, density):

        self.density = density
        self.period_end = period_end
        self.period_start = period_start

    lower_partition_id = None
    upper_partition_id = None
    spatial_index = None

    def build_spatial_index(self, samples, global_bound, sample_rate, beta, k, is_quad_index):
        if is_quad_index:
            self.spatial_index = GlobalQuad(global_bound)
            self.spatial_index.build(samples, sample_rate, beta, k)
        else:
            self.spatial_index = GlobalRTree(max(10, len(samples) // beta))
            self.spatial_index.build(samples)

    def assign_partition_id(self, lower_id):
        self.lower_partition_id = lower_id
        self.upper_partition_id = self.spatial_index.assign_partition_id(lower_id)
        return self.upper_partition_id

    def contains_partition(self, point_id):
        return self.lower_partition_id <= point_id < self.upper_partition_id

    def get_partition_env(self, partition_id: int):
        return self.spatial_index.get_leaf_env(partition_id - self.lower_partition_id)

    def get_spatial_index(self):
        return self.spatial_index


class STBound:
    def __init__(self, env: shapely.geometry.Polygon, start_time, end_time):
        self.end_time = end_time
        self.start_time = start_time
        self.env = env

    def contains_point(self, time_refer_point: int) -> bool:
        return not time_refer_point < self.start_time or self.end_time < time_refer_point

    def contains_range(self, start: int, end: int) -> bool:
        return start > self.start_time and end < self.end_time

    def contains_points(self, x, y) -> bool:
        return self.env.contains(shapely.geometry.Point(x, y))

    def contains_env(self, other_env: shapely.geometry.Polygon) -> bool:
        return self.env.contains(other_env)


class STIndex:
    def __init__(self, global_env, global_range, alpha, beta, delta_milli, k, is_quad_index):
        self.is_quad_index = is_quad_index
        self.k = k
        self.delta_milli = delta_milli
        self.beta = beta
        self.alpha = alpha
        self.global_range = global_range
        self.global_env = global_env

    time_periods = []
    is_updated = False

    def is_update(self):
        return self.is_updated

    def update_bound(self, leaf_node_map: dict):
        if not self.is_updated:
            for period in self.time_periods:
                period.get_spatial_index.update_bound(leaf_node_map)

    def build(self, samples: list[STBound], sample_rate):
        min_time, max_time = self.global_range
        sorted_samples = sorted(samples, key=lambda a, b: a.start_time < b.start_time)
        avg = sum(map(lambda x: x.end_time - x.start_time, sorted_samples)) / len(samples)
        min_span_milli = max(2 * self.delta_milli, avg, (max_time - min_time) / self.alpha)
        min_sample_num = max(len(samples) // self.alpha, sample_rate * self.beta * self.k)

        time_span = 0
        sweep_line = period_start = min_time
        sample_holder = []
        for sample in sorted_samples:
            time_span += sample.start_time - sweep_line
            sweep_line = sample.start_time
            sample_holder.append(sample)
            if time_span >= min_span_milli and len(sample_holder) >= min_sample_num:
                density = len(sample_holder) / time_span
                period = TimePeriod(period_start, sweep_line, density)
                period.build_spatial_index(map(lambda x: x.env, sample_holder), self.global_env, sample_rate, self.beta,
                                           self.k, self.is_quad_index)
                self.time_periods.append(period)
                period_start = sweep_line
                sample_holder = filter(lambda x: x.end_time > sweep_line, sample_holder)
                time_span = 0

        time_span += max_time - sweep_line
        density = len(sample_holder) / time_span
        period = TimePeriod(period_start, sweep_line, density)
        period.build_spatial_index(map(lambda x: x.env, sample_holder), self.global_env, sample_rate, self.beta,
                                   self.k, self.is_quad_index)
        self.time_periods.append(period)
        base_id = 0
        for period in self.time_periods:
            base_id = period.assign_partition_id(base_id)
        partition_num_id = base_id
        return partition_num_id

    def get_partition_id(self, query_geom, query_range, time_bin_map) -> int:
        expand_query_range = time_utils.expand_time_range(query_range, delta_milli=self.delta_milli)
        partition_id = -1
        for period in self.get_time_periods(expand_query_range):
            if partition_id == -1:
                partition_id = period.get_spatial_index.get_partition_id(query_geom, expand_query_range, time_bin_map)
        return partition_id

    def get_partition_ids_s(self, geom, start, end) -> list[int]:
        query_range = (start, end)
        result = []
        if self.is_quad_index:
            for period in self.get_time_periods(query_range):
                result += period.get_spatial_index.get_partition_ids(geom)
        else:
            for period in self.get_time_periods(query_range):
                result.append(period.get_spatial_index.get_partition_id(geom))
        return result

    def get_partition_ids_r_second(self, geom, start, end, distance) -> list[int]:
        expand_query_range = time_utils.expand_time_range((start, end), self.delta_milli)
        result = []
        for period in self.get_time_periods(expand_query_range):
            result += period.get_spatial_index.get_partition_ids(geom)
        return result

    def get_time_periods(self, query_range):
        start_index = self.time_periods.index(next(p for p in self.time_periods if p.period_end > query_range[0]))
        end_index = self.time_periods.index(next(p for p in self.time_periods if p.period_end > query_range[1]),
                                            start_index)
        return self.time_periods[start_index:end_index]
