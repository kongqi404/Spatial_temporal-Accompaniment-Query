
import math
import queue

import shapely.geometry

import time_utils

class GlobalNode:
    partition_id = -1
    def set_partition_id(self,id):
        self.partition_id = id
    def get_partition_id(self,id):
        return self.partition_id
class QuadNode(GlobalNode):
    def __init__(self,env:shapely.geometry.Polygon) -> None:
        self.center = env.centroid()
        self.sub_nodes = []
    
    
    
    def split(self,samples:list[shapely.geometry.Polygon],queue:queue.PriorityQueue)->None:
        for index in range(0,4):
            assert self.sub_nodes[index] is None
            self.sub_nodes[index] = QuadNode(self.create_sub_env(index))
            sub_env = self.sub_nodes[index].env
            sub_samples = filter(lambda x:sub_env.intersects(x),samples)
            queue.put(0,(self.sub_nodes[index],sub_samples))
    
    def find_nearest_id(self,point:shapely.geometry.Point,filter_)->int:
        if self.partition_id != -1:
            return self.partition_id if filter_(self) else -1
        if point.getX > self.centre.getX:
            if point.getY > self.centre.getY:
                sub_index = 3
            else:
                sub_index = 1
        else:
            if point.getY > self.centre.getY:
                sub_index = 2
            else:
                sub_index = 0
        nearest_id = self.sub_nodes[sub_index].find_nearest_id(point,filter_)
        if nearest_id == -1:
            for index in range(4):
                if index != sub_index:
                    nearest_id = self.sub_nodes[index].find_nearest_id(point,filter_)
                    
    
    def create_sub_env(self,index):
        pass
        

class GlobalQuad:
    def __init__(self,spatial_bound) -> None:
        self.spatial_bound = spatial_bound
        self.root = QuadNode(self.spatial_bound)
        self.leaf_nodes = []
    def build(self,samples,sample_rate,beta,k):
        comparator=lambda x,y:0-(len(x[1])-len(y[1]))
        max_num_per_partition = max(len(samples)//self.beta,math.ceil(4*self.sample_rate*k))
        priority_queue = queue.PriorityQueue()
        priority_queue.put((-len(samples),(self.root,samples)))
        while len(priority_queue)<beta:
            if len(priority_queue[0][1]) < max_num_per_partition:
                self.leaf_nodes = [node[0] for node in priority_queue]
                return
            else:
                max_node,max_samples = priority_queue.get()
                # max_node.split()
    
        
    
    


class GlobalRTree:
    pass


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
        end_index = self.time_periods.index(next(p for p in self.time_periods if p.period_end > query_range[1]), start_index)
        return self.time_periods[start_index:end_index]
        
