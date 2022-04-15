import time

import shapely.geometry
from pyspark import StorageLevel, RDD

import extractor
from partition import do_statistic
from time_utils import expand_time_range, is_intersects


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
        left_time_range = expand_time_range(left_global_info.get_time_range(), delta_milli=self.delta_milli)

        validate_right_rdd = right_rdd.filter(
            lambda row: is_intersects(left_time_range, row[1])) \
            .persist(StorageLevel.MEMORY_AND_DISK)
        right_global_info = do_statistic(validate_right_rdd, right_extractor)

        global_bound = shapely.geometry.Polygon(right_global_info.get_env())
        global_range = right_global_info.get_time_range()
        # global_index =  STIndex(globalBound, globalRange, alpha, beta, deltaMilli, k, isQuadIndex)
