import time
import unittest
from math import sqrt, log

import pyspark.rdd
import shapely.geometry
from pyspark import StorageLevel, RDD

import extractor
from index import STIndex, STBound, TRCBasedBins
from partition import do_statistic
import all_utils


class STKnnJoin:
    def __init__(self, delta_milli, k, alpha, beta, bin_num, is_quad_index=True):
        """

        :param delta_milli:  delta
        :param k: k of knn
        :param alpha: alpha to divide spatial
        :param beta: beta to divide time
        :param bin_num:
        :param is_quad_index:
        """
        self.delta_milli = delta_milli
        self.k = k
        self.alpha = alpha
        self.beta = beta
        self.bin_num = bin_num
        self.is_quad_index = is_quad_index

    def join(self, left_rdd: RDD, right_rdd: RDD, left_extractor: extractor.STExtractor,
             right_extractor: extractor.STExtractor):
        """

        :param left_rdd:
        :param right_rdd:
        :param left_extractor:
        :param right_extractor:
        :return:
        """
        original_time = int(time.time())  # current time/s
        spark = left_rdd.context
        left_rdd.persist(StorageLevel.MEMORY_AND_DISK)
        left_global_info = do_statistic(left_rdd, left_extractor)
        left_time_range = all_utils.expand_time_range(left_global_info.get_time_range(), delta_milli=self.delta_milli)

        validate_right_rdd = right_rdd.filter(
            lambda row: all_utils.is_intersects(left_time_range, row[1])) \
            .persist(StorageLevel.MEMORY_AND_DISK)
        right_global_info = do_statistic(validate_right_rdd, right_extractor)
        # sample and build global index and partitioner
        global_bound = shapely.geometry.Polygon(right_global_info.get_env())
        global_range = right_global_info.get_time_range()
        global_index = STIndex(global_bound, global_range, self.alpha, self.beta, self.delta_milli,
                               self.k, self.is_quad_index)
        samples, sample_rate = sample(validate_right_rdd, right_extractor, right_global_info.get_count(), self.alpha,
                                      self.beta)
        partition_num = global_index.build(samples, sample_rate)

        # unused
        # partitioner = get_partitioner(partition_num)

        bc_global_index = spark.broadcast(global_index)

        # repartition right rdd
        partitioned_right_rdd = validate_right_rdd \
            .flatMap(lambda right_row: map(lambda x: (x, right_row),
                                           bc_global_index
                                           .value
                                           .get_partition_ids_s(right_extractor.geom(right_row),
                                                                right_extractor.start_time(
                                                                    right_row),
                                                                right_extractor.end_time(
                                                                    right_row)))) \
            .partitionBy(partition_num, lambda x: int(x)) \
            .map(lambda x: x[1]) \
            .mapPartitions(lambda row_iter: iter((pyspark.TaskContext.get().partitionId(), list(row_iter)))) \
            .filter(lambda x: x[1] is not None)

        partition_bound_accum = spark.accumulator((0, shapely.geometry.Polygon())) if not self.is_quad_index else None

        # 2022/4/19 unfinished!
        def time_map(partition_id: int, right_rows):
            bound = shapely.geometry.Polygon()
            time_bin = TRCBasedBins(self.bin_num, self.k)
            time_ranges = list(map(lambda x: (x[1][0], x[1][1]), right_rows))
            if not self.is_quad_index:
                for row in right_rows:
                    bound = all_utils.transfer_bounds_to_box(bound.union(right_extractor.geom(row)).bounds)
                partition_bound_accum.add((partition_id, bound))
            time_bin.build(time_ranges)
            return partition_id, time_bin

        time_bin_map = dict(partitioned_right_rdd.map(time_map).collect())

        bc_time_bin_map = spark.broadcast(time_bin_map)
        if not self.is_quad_index:
            global_index.update_bound(partition_bound_accum.value.__dict__)
            bc_global_index=spark.broadcast(global_index)

        def indexed_right(partition_id,right_rows):
            local_index = STRtree()
        indexed_right_rdd = partitioned_right_rdd.map()














# unused:
# def get_partitioner(partition_num: int) -> pyspark.rdd.Partitioner:
#     return pyspark.rdd.Partitioner(partition_num, lambda x: int(x))


def sample(rdd: pyspark.RDD, _extractor: extractor.STExtractor, total_num, alpha, beta):
    num_partitions = alpha * beta
    sample_size = total_num if total_num < 1000 else max(num_partitions * 2, total_num // 100)

    def num_std(s):
        if s < 6:
            return 12.0
        elif s < 16:
            return 9.0
        else:
            return 6.0

    def compute_fraction_for_sample_size(sample_size_lower_bound, total, with_replacement):
        """
        self implement the computeFractionForSampleSize method of org.apache.spark.util.random.SamplingUtils
        reference: https://spark.apache.org/docs/latest/api/java/index.html
        and
        https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/util/random/SamplingUtils.scala
        :param sample_size_lower_bound:sample size
        :param total:size of RDD
        :param with_replacement:whether sampling with replacement
        :return:a sampling rate that guarantees sufficient sample size with 99.99% success rate
        """
        if with_replacement:
            return max(sample_size_lower_bound + num_std(sample_size_lower_bound) * sqrt(sample_size_lower_bound),
                       1e-10) / total
        else:
            return min(1.0, max(1e-10, sample_size_lower_bound / total - log(1e-4) + sqrt(
                1e-4 ** 2 + 2 * 1e-4 * sample_size_lower_bound / total)))

    fraction = compute_fraction_for_sample_size(sample_size, total_num, False)
    samples = rdd.sample(withReplacement=False, fraction=fraction) \
        .map(lambda row: STBound(_extractor.geom(row).boundary, _extractor.start_time(row), _extractor.end_time(row))) \
        .collect()
    return samples, fraction
