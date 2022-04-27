import heapq
import math
import queue

import shapely.geometry

import all_utils
import extractor
from extractor import STExtractor


# unfinished 4/20
class STBoundable:
    def __init__(self):
        self.min_time = None
        self.max_time = None
        self.envelope = None

    def get_min_time(self):
        return self.min_time

    def get_max_time(self):
        return self.max_time

    def get_bound(self):
        return self.envelope

    def centre_t(self):
        return (self.min_time + self.max_time) / 2

    # unfinished
    def centre_x(self):
        return self.envelope


class STRTreeNode(STBoundable):
    def __init__(self, level):
        super().__init__()
        self.level = level
        self.child_boundables = []
        self.bound = None
        self.min_time = None
        self.max_time = None

    def get_min_time(self):
        if self.min_time is None:
            self.min_time = min(map(lambda x: x.get_min_time(), self.child_boundables))
        return self.min_time

    def get_max_time(self):
        if self.max_time is None:
            self.max_time = max(map(lambda x: x.get_max_time(), self.child_boundables))
        return self.max_time

    def get_bound(self):
        if self.bound is None:
            self.bound = shapely.geometry.Polygon()
            assert len(self.child_boundables) > 0
            for sub_node in self.child_boundables:
                self.bound = all_utils.transfer_bounds_to_box(self.bound.union(sub_node.get_bound()).bounds)
        return self.bound

    def get_level(self):
        return self.level

    def __len__(self):
        return len(self.child_boundables)

    def is_empty(self):
        return len(self.child_boundables) > 0

    def add_child_boundable(self, child_node: STBoundable):
        assert self.bound is None
        self.child_boundables.append(child_node)

    def get_child_boundables(self):
        return self.child_boundables


class STItemBoundable(STBoundable):
    def __init__(self, item, _extractor: STExtractor):
        super().__init__()
        self.extractor = _extractor
        self.item = item

    def get_bound(self):
        return all_utils.transfer_bounds_to_box(self.extractor.geom(self.item).bounds)

    def get_item(self):
        return self.item

    def get_geom(self):
        return self.extractor.geom(self.item)

    def get_min_time(self):
        return self.extractor.start_time(self.item)

    def get_max_time(self):
        return self.extractor.end_time(self.item)


class STRTreeIndex:
    def __init__(self, _extractor: STExtractor, node_capacity=10):
        self.node_capacity = node_capacity
        self.extractor = _extractor
        self.root = None
        self.item_bound_ables = []
        self.empty = True
        self.built = False

    def insert(self, item):
        self.empty = False
        self.item_bound_ables.append(STItemBoundable(item, self.extractor))

    def build(self):
        if not self.built:
            self.root = self.create_node(0) if len(self.item_bound_ables) == 0 \
                else self.create_higher_levels(self.item_bound_ables, -1)
            self.item_bound_ables = None
            self.built = True

    def is_empty(self) -> bool:
        return self.empty

    @staticmethod
    def create_node(level: int) -> STRTreeNode:
        return STRTreeNode(level)

    def create_higher_levels(self, boundables: list, level: int) -> STRTreeNode:
        assert len(boundables) > 0
        parent_boundables = self.create_parent_boundables(boundables, level + 1)
        if len(parent_boundables) == 1:
            return parent_boundables[0]
        else:
            return self.create_higher_levels(parent_boundables, level + 1)

    def create_parent_boundables(self, child_boundables: list, new_level: int) -> list:
        assert len(child_boundables) > 0
        min_leaf_count = math.ceil(len(child_boundables) / self.node_capacity)
        slice_count = math.ceil(math.pow(min_leaf_count, 1 / 3))
        time_sort_iter = sorted(child_boundables)  # key is not set!!!!
        slice_capacity = math.ceil(len(child_boundables) / slice_count)
        parent_boundables = []
        # their can be refactored !!!
        it = 0
        for _ in range(slice_count):
            time_slice = []

            while it < len(time_sort_iter) is not None and len(time_slice) < slice_capacity:
                time_slice.append(time_sort_iter[it])
                it += 1
            vertical_slices = self.create_vertical_slices(time_slice, slice_count)
            for i in range(len(vertical_slices)):
                parent_boundables += self.create_parent_boundable_from_vertical_slice(vertical_slices[i], new_level)

    # this function can be refactored !!!
    @staticmethod
    def create_vertical_slices(child_boundables, slice_count: int):
        slice_capacity = math.ceil(len(child_boundables) / slice_count)
        its = sorted(child_boundables)
        it = 0
        slices = []
        for _ in range(slice_count):
            current_slice = []
            while it < len(its) and len(current_slice) < slice_capacity:
                current_slice.append(its[it])
                it += 1
            slices.append(current_slice)
        return slices

    def create_parent_boundable_from_vertical_slice(self, child_boundables: list, new_level):
        assert len(child_boundables) > 0
        parent_boundables = [self.create_node(new_level)]
        sorted_child_boundables = sorted(child_boundables)  # key is not set!!!!
        it = 0
        while it < len(sorted_child_boundables):
            if len(parent_boundables[-1]) == self.node_capacity:
                parent_boundables.append(self.create_node(new_level))
            parent_boundables[-1].add_child_boundable(sorted_child_boundables[it])
        return parent_boundables

    # unfinished!!!
    def nearest_neighbour(self, query_geom: shapely.geometry.Polygon, query_start, query_end, k, is_valid,
                          max_distance):
        if not self.built:
            self.build()
        query_extractor = STExtractor()
        distance_lower_bound = max_distance
        node_queue = []
        query_item = (query_geom, query_start, query_end)
        query_boundable = STItemBoundable()  # unfinished!!!
        heapq.heappush(node_queue, (self.distance(self.root, query_boundable), self.root))
        # node_queue.put((self.root, self.distance(self.root, query_boundable)))  # bug

        candidate_queue = []
        while len(node_queue) > 0 and node_queue[0][0] < distance_lower_bound:
            current_distance, nearest_node = heapq.heappop(node_queue)
            if isinstance(nearest_node, STItemBoundable):
                if is_valid(nearest_node.get_item()):
                    if len(candidate_queue) < k:
                        heapq.heappush(candidate_queue, (-current_distance, nearest_node))
                    else:
                        if -candidate_queue[0][0] > current_distance:
                            heapq.heappop(candidate_queue)
                            heapq.heappush(candidate_queue, (-current_distance, nearest_node))
                        distance_lower_bound = -candidate_queue[0][0]
            elif isinstance(nearest_node, STRTreeNode):
                for child_boundable in nearest_node.get_child_boundables():
                    child = child_boundable
                    if all_utils.is_intersects((query_start, query_end), (child.get_min_time(), child.get_max_time())):
                        dist = self.distance(child, query_boundable)
                        heapq.heappush(node_queue, (dist, child))

        return sorted(map(lambda x: (x[1].get_item(), -x[0]), candidate_queue), key=lambda x: x[1])

    @staticmethod
    def distance(one_boundable, other_boundable):
        if isinstance(one_boundable, STItemBoundable) and isinstance(other_boundable, STItemBoundable):
            return one_boundable.get_geom().distance(other_boundable.get_geom())
        else:
            return one_boundable.get_bound().distance(other_boundable.get_bound())

    # unused
    # @staticmethod
    # def build_queue(is_normal: bool):  # is a priority_queue  unfinished
    #     comparator = lambda x, y: x[1] - y[1] if is_normal else -(x[1] - y[1])
    #     return [], comparator


class STRtree:
    def __init__(self, _extractor: STExtractor, k: int, bin_num: int):
        self.bin_num = bin_num
        self.k = k
        self.extractor = _extractor
        self.is_built = False
        self.empty = True
        self.rtree = STRTreeIndex(self.extractor)

    def build(self, rows: list):
        for row in rows:
            self.rtree.insert(row)
            self.rtree.build()
            self.empty = len(rows) > 0
            self.is_built = True

    def nearest_neighbour(self, query_geom, query_start, query_end, k, is_valid, max_distance=1e10) -> list:
        return self.rtree.nearest_neighbour(query_geom, query_start, query_end, k, is_valid, max_distance)

    def is_empty(self):
        return self.empty


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
        for i in reversed(min_nums):
            self.right_min_sums.append(self.right_min_sums[-1] + i)
        for i in max_nums:
            self.left_max_sums.append(self.left_max_sums[-1] + i)

    def has_knn(self, query_range) -> bool:
        if query_range[0] > self.end_time or query_range[1] < self.start_time:
            return False
        min_than_start = self.left_max_sums[(query_range[0] - self.start_time) // self.span_milli] \
            if query_range[0] > self.start_time else 0
        max_than_end = self.right_min_sums[self.bin_num - (query_range[1] - self.start_time) // self.span_milli - 1] \
            if query_range[1] < self.end_time else 0
        return self.total - min_than_start - max_than_end >= self.k


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
            # unfinished!
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
        self.leaf_nodes: list[QuadNode] = []

    def update_bound(self, leaf_node_map: dict):
        pass

    def build(self, samples, sample_rate, beta, k) -> None:
        def comparator(x):
            return -len(x)

        max_num_per_partition = max(len(samples) // beta, math.ceil(4 * sample_rate * k))
        priority_queue = []
        # unfinished
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

    def update_bound(self, leaf_node_map: dict):
        pass


class TimePeriod:

    def __init__(self, period_start, period_end, density):

        self.density = density
        self.period_end = period_end
        self.period_start = period_start

    lower_partition_id: int = None
    upper_partition_id: int = None
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

    def get_spatial_index(self) -> GlobalQuad:
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

    time_periods: list[TimePeriod] = []
    is_updated = False

    def is_update(self):
        return self.is_updated

    def update_bound(self, leaf_node_map: dict):
        if not self.is_updated:
            for period in self.time_periods:
                period.get_spatial_index.update_bound(leaf_node_map)

    def build(self, samples: list[STBound], sample_rate):
        min_time, max_time = self.global_range
        sorted_samples = sorted(samples, key=lambda x: x.start_time)
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
        period.build_spatial_index(list(map(lambda x: x.env, sample_holder)), self.global_env, sample_rate, self.beta,
                                   self.k, self.is_quad_index)
        self.time_periods.append(period)
        base_id = 0
        for period in self.time_periods:
            base_id = period.assign_partition_id(base_id)
        partition_num_id = base_id
        return partition_num_id

    def get_partition_id(self, query_geom, query_range, time_bin_map) -> int:
        expand_query_range = all_utils.expand_time_range(query_range, delta_milli=self.delta_milli)
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
        expand_query_range = all_utils.expand_time_range((start, end), self.delta_milli)
        result = []
        for period in self.get_time_periods(expand_query_range):
            result += period.get_spatial_index.get_partition_ids(geom)
        return result

    def get_partition(self, partition_id: int):
        period: TimePeriod = filter(lambda x: x.contains_partition(partition_id), self.time_periods)[0]
        env = period.get_partition_env(partition_id)
        return STBound(env, period.period_start, period.period_end)

    def get_time_periods(self, query_range):
        start_index = self.time_periods.index(next(p for p in self.time_periods if p.period_end > query_range[0]))
        end_index = self.time_periods.index(next(p for p in self.time_periods if p.period_end > query_range[1]),
                                            start_index)
        return self.time_periods[start_index:end_index]
