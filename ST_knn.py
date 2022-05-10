
from math import sqrt, log
import pyspark.java_gateway
import pyspark.rdd
import shapely.geometry
from pyspark import StorageLevel, RDD
from shapely.geometry import Polygon

from index import STIndex, STBound, TRCBasedBins, STRtree, TimePeriod, PartitionDataSetS
from partition import do_statistic
import all_utils


class RowWithId:
    def __init__(self, _id: int, row):
        self.row = row
        self.id = _id

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return self.id.__hash__()


class ApproximateContainer:
    def __init__(self, duplicate_knn, partition_id, expand_dist):
        self.expand_dist = expand_dist
        self.partition_id = partition_id
        self.duplicate_knn = duplicate_knn


class LocalJoin:
    def __init__(self, delta_milli, k):
        self.k = k
        self.delta_milli = delta_milli

    def first_round_join(self, left_rows: list[RowWithId], local_index: STRtree, partition_bound: STBound,
                         partition_id: int):
        if len(left_rows) == 0 or local_index is None:
            return None

        def mapping(row_with_id: RowWithId):
            if row_with_id.id > 0:
                left_geom = row_with_id.row[0]
                left_start_time = row_with_id.row[1][0]
                left_end_time = row_with_id.row[1][1]
                left_time_range = all_utils.expand_time_range((left_start_time, left_end_time), self.delta_milli)
                is_valid = lambda right_row: all_utils.is_intersects(left_time_range, (
                    right_row[1][0], right_row[1][1]))
                candidate_with_dist = local_index.nearest_neighbour(left_geom, left_time_range[0], left_time_range[1],
                                                                    self.k, is_valid)

                # left_geom_env = all_utils.transfer_bounds_to_box(left_geom.bounds)
                max_dist = candidate_with_dist[-1][1]  if len(candidate_with_dist) == self.k else 1e10
                left_geom_env = all_utils.transfer_bounds_to_box(
                    all_utils.expand_envelop_by_dis(left_geom.bounds, max_dist))
                duplicated_candidates = list(filter(lambda right_row:
                                               self.is_reverse(partition_bound, left_geom_env,
                                                               all_utils.transfer_bounds_to_box(
                                                                   right_row[0][0].bounds),
                                                               left_time_range, (
                                                                   right_row[0][1][0],
                                                                   right_row[0][1][1])),
                                               candidate_with_dist))
                return row_with_id, ApproximateContainer(duplicated_candidates, partition_id, max_dist)
            else:
                return row_with_id, ApproximateContainer([], partition_id, 1e10)

        return list(map(mapping, left_rows))

    def second_round_join(self, left_rows: list[(RowWithId, ApproximateContainer)], local_index: STRtree,
                          partition_bound: STBound):
        if len(left_rows) == 0 or local_index is None:
            return None

        def mapping(row_with_id: RowWithId, container: ApproximateContainer):
            if container.partition_id != -1:
                return row_with_id, container.duplicate_knn
            else:
                left_geom = row_with_id.row[0]
                left_start_time = row_with_id.row[1][0]
                left_end_time = row_with_id.row[1][1]
                left_geom_env = all_utils.transfer_bounds_to_box(
                    all_utils.expand_envelop_by_dis(left_geom.bounds, container.expand_dist))
                left_time_range = all_utils.expand_time_range((left_start_time, left_end_time), self.delta_milli)
                is_valid = lambda right_row: \
                    all_utils.is_intersects(left_time_range,
                                            (right_row[1][0]
                                             , right_row[1][1]))
                candidates_with_dist = local_index.nearest_neighbour(left_geom, left_time_range[0], left_time_range[1],
                                                                     self.k, is_valid, container.expand_dist)
                duplicated_candidates = list(filter(lambda right_row:
                                                    self.is_reverse(partition_bound,
                                                                    left_geom_env,
                                                                    all_utils.transfer_bounds_to_box(
                                                                        right_row[0][0].bounds),
                                                                    left_time_range,
                                                                    (right_row[0][1][0],
                                                                     right_row[0][1][1])),
                                                    candidates_with_dist))
            return row_with_id, duplicated_candidates

        return list(map(lambda x: mapping(x[0], x[1]), left_rows))

    @staticmethod
    def is_reverse(partition_bound: STBound, left_geom_env: Polygon,
                   right_geom_env: Polygon, left_time_range: tuple, right_time_range: tuple) -> bool:
        time_refer_point = all_utils.time_refer_point(left_time_range, right_time_range)
        if time_refer_point is not None and partition_bound.contains_point(time_refer_point):
            intersection = left_geom_env.intersection(right_geom_env).bounds
            return intersection[3] - intersection[1] >= 0 and partition_bound.contains_points(intersection[0],
                                                                                              intersection[1])
        else:
            return False


# unused:
# def get_partitioner(partition_num: int) -> pyspark.rdd.Partitioner:
#     return pyspark.rdd.Partitioner(partition_num, lambda x: int(x))


def sample(rdd: pyspark.RDD, total_num, alpha, beta):
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
        .map(lambda row: STBound(all_utils.transfer_bounds_to_box(row[0].bounds), row[1][0], row[1][1])) \
        .collect()
    return samples, fraction


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

    def join(self, left_rdd: RDD, right_rdd: RDD):
        """

        :param left_rdd:
        :param right_rdd:
        :return:
        """
        spark = left_rdd.context
        left_rdd.persist(StorageLevel.MEMORY_AND_DISK)
        left_global_info = do_statistic(left_rdd)
        left_time_range = all_utils.expand_time_range(left_global_info.get_time_range(), delta_milli=self.delta_milli)

        validate_right_rdd = right_rdd.filter(
            lambda row: all_utils.is_intersects(left_time_range, row[1])) \
            .persist(StorageLevel.MEMORY_AND_DISK)

        right_global_info = do_statistic(validate_right_rdd)
        # sample and build global index and partitioner
        global_bound = right_global_info.get_env()
        global_range = right_global_info.get_time_range()
        global_index = STIndex(global_bound, global_range, self.alpha, self.beta, self.delta_milli,
                               self.k, self.is_quad_index)
        samples, sample_rate = sample(validate_right_rdd, right_global_info.get_count(), self.alpha,
                                      self.beta)
        partition_num = global_index.build(samples, sample_rate)
        # print(global_index.time_periods[0].period_start)
        part_s = PartitionDataSetS(global_index.time_periods)

        # bc_global_index = spark.broadcast(global_index)
        bc_part_s = spark.broadcast(part_s)

        def f_mp(iterator):
            yield pyspark.TaskContext.get().partitionId(), list(iterator)

        # partitioned_right_rdd = partitioned_right_rdd.map(lambda x: x[1]).mapPartitions(f_mp).filter(lambda x:len(x[1])>0)
        partitioned_right_rdd = validate_right_rdd \
            .flatMap(
            lambda right_row: map(lambda x: (x, right_row),
                                  bc_part_s.value.get_partition_ids_s(right_row[0], right_row[1][0], right_row[1][1]))) \
            .partitionBy(partition_num) \
            .map(lambda x: x[1]) \
            .mapPartitions(f_mp) \
            .filter(lambda x: len(x[1]) > 0)
        # print(list(filter(lambda x:x[0],partitioned_right_rdd.collect())))
        # print("--------------------------------------------------------------------------------------")


        # print(validate_right_rdd.flatMap(lambda right_row_flatmap: bc_global_index.value.get_partition_ids_s(right_row_flatmap[0], right_row_flatmap[1][0], right_row_flatmap[1][1])).collect())
        # print(validate_right_rdd.flatMap(v_f_map).collect())
        partition_bound_accum = spark.accumulator((0, shapely.geometry.Polygon())) if not self.is_quad_index else None

        def time_map(partition_id: int, right_rows):
            bound = shapely.geometry.Polygon()
            time_bin = TRCBasedBins(self.bin_num, self.k)
            time_ranges = list(map(lambda x: (x[1][0], x[1][1]), right_rows))
            if not self.is_quad_index:
                for row in right_rows:
                    bound = all_utils.transfer_bounds_to_box(bound.union(row[0]).bounds)
                partition_bound_accum.add((partition_id, bound))
            time_bin.build(time_ranges)
            return partition_id, time_bin

        time_bin_map = dict(partitioned_right_rdd.map(lambda x: time_map(x[0], x[1])).collect())

        bc_time_bin_map = spark.broadcast(time_bin_map)

        # if not self.is_quad_index:
        #     global_index.update_bound(partition_bound_accum.value.__dict__)
        #     bc_global_index = spark.broadcast(global_index)

        def indexed_right(partition_id, right_rows):
            local_index = STRtree( self.k, self.bin_num)
            local_index.build(right_rows)
            return partition_id, local_index

        # for x in partitioned_right_rdd.collect():
        #     print(indexed_right(x[0],x[1]))
        indexed_right_rdd = partitioned_right_rdd \
            .map(lambda x: indexed_right(x[0], x[1])) \
            .persist(StorageLevel.MEMORY_AND_DISK)
        # print(indexed_right_rdd.collect()[0][0])
        # print("----------------------------------------------------------")
        local_join = LocalJoin( self.delta_milli, self.k)

        def left_p_r(left_row, i_d):
            geom = left_row[0]
            start_time = left_row[1][0]
            end_time = left_row[1][1]
            index = bc_part_s.value
            # if not self.is_quad_index:
            #     assert index.is_updated
            partition_id = index.get_partition_id(geom, (start_time, end_time), bc_time_bin_map.value)
            if partition_id == -1:
                row_with_id = RowWithId(i_d, left_row)
                temp_partition_id = index.get_partition_ids_r_second(geom, start_time, end_time, 1e10)[0]
                yield temp_partition_id, row_with_id
            else:
                yield partition_id, RowWithId(-i_d, left_row)

        # left_partition_rdd: RDD = left_rdd.zipWithUniqueId()
        # for x in left_partition_rdd.collect():
        #     print(list(left_p_r(x[0],x[1])))
        # left_partition_rdd = spark.parallelize(
        #     [i for i in left_p_r(x[0], x[1])] for x in left_partition_rdd.collect()).flatMap(lambda x: x).partitionBy(
        #     partition_num, lambda x: int(x)).map(lambda x: x[1])
        left_partition_rdd: pyspark.RDD = left_rdd.zipWithUniqueId().filter(lambda x: x is not None).flatMap(
            lambda x: left_p_r(x[0], x[1])).partitionBy(partition_num, lambda x: int(x)).map(
            lambda x: x[1])

        # print(left_partition_rdd.collect())
        # print("-------------------------------------------------------------------------------")

        def zip_partitions(rdd1, rdd2, func):
            rdd1_num_partitions = rdd1.getNumPartitions()
            rdd2_num_partitions = rdd2.getNumPartitions()
            assert rdd1_num_partitions == rdd2_num_partitions, "rdd1 and rdd2 must have the same number of partitions"

            paired_rdd1 = rdd1.mapPartitionsWithIndex(lambda index, it: ((index, list(it)),))
            paired_rdd2 = rdd2.mapPartitionsWithIndex(lambda index, it: ((index, list(it)),))
            # print(paired_rdd2.collect())
            zipped_rdd = paired_rdd1.join(paired_rdd2, numPartitions=rdd1_num_partitions)\
                .flatMap(lambda x: func(x[1][0], x[1][1],x[0]))
            # print(zipped_rdd.collect())
            return zipped_rdd

        def zip_partitions_func1(left_rows: iter, right_index: iter,i_d):
            for i in right_index:
                partition_id, local_index = i
                partition_id=i_d
                index = part_s
                # if not self.is_quad_index:
                #     assert index.is_updated
                assert partition_id<partition_num
                # print(partition_id)
                # print(index.get_partition(partition_id))
                yield local_join.first_round_join(left_rows, local_index, index.get_partition(partition_id),
                                                  partition_id)
        # print("------------------------------------------------------------------------------------------")
        #test
        # print(list(zip_partitions_func1(left_partition_rdd.collect(),indexed_right_rdd.collect())))
        def l_r_r_func(row_with_id: RowWithId, container: ApproximateContainer):
            left_geom = row_with_id.row[0]
            start_time = row_with_id.row[1][0]
            end_time = row_with_id.row[1][1]
            index = bc_part_s.value
            # if not self.is_quad_index:
            #     assert index.is_updated
            partition_ids = index.get_partition_ids_r_second(left_geom, start_time, end_time, container.expand_dist)
            return map(lambda partition_id: (partition_id, (row_with_id, container)) if
            row_with_id.id > 0 and partition_id == container.partition_id else (
                partition_id, (row_with_id, ApproximateContainer([], -1, container.expand_dist))), partition_ids)

        left_repartition_rdd: RDD = zip_partitions(left_partition_rdd, indexed_right_rdd, zip_partitions_func1).filter(
            lambda x: x is not None).flatMap(lambda x: x)\
            .flatMap(lambda x: l_r_r_func(x[0], x[1])) \
            .partitionBy(partition_num, lambda x: int(x)).map(lambda x: x[1])
        def zip_partitions_func2(left_rows: iter, right_index: iter,i_d) -> iter:
            for i in right_index:
                partition_id, local_index = i
                partition_id=i_d
                yield local_join.second_round_join(left_rows, local_index,
                                                   bc_part_s.value.get_partition(partition_id))

        def second_local_join_map(row_with_id: RowWithId, candidate_iter):
            return row_with_id.row, sorted(candidate_iter, key=candidate_iter[1])[:self.k]

        res_rdd: RDD = zip_partitions(left_repartition_rdd, indexed_right_rdd, zip_partitions_func2) \
            .filter(lambda x: x is not None).flatMap(lambda x: x) \
            .groupByKey() \
            .map(lambda x: (
            x[0].row, list(x[1])))
        return res_rdd
