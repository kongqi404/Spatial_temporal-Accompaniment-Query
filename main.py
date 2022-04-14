import time

import pyspark

from shapely import wkt

from ST_knn import STKnnJoin
from extractor import STExtractor

if __name__ == "__main__":
    spark = pyspark.sql.SparkSession.builder.appName("st_knn").getOrCreate()

    alpha = 200
    beta = 40
    bin_num = 200
    delta_milli = 30 * 60 * 1000
    k = 15
    left_path = "./r"
    right_path = "./s"
    store_path = "./res"
    st_knn_join = STKnnJoin(delta_milli, k, alpha, beta, bin_num)  # instance a class


    def mapping(line):
        sp = line.split("\t")
        return (wkt.loads(sp[0]), (int(time.mktime(time.strptime(sp[1], "%Y-%m-%d %H:%M:%S"))),
                                   int(time.mktime(time.strptime(sp[2], "%Y-%m-%d %H:%M:%S")))))


    def read_rdd(file_path):
        return spark.sparkContext.textFile(file_path).map(mapping)


    # read_rdd(left_path).repartition(1).saveAsTextFile(store_path)
    extractor_1 = STExtractor()
    extractor_2 = STExtractor()
    join_time = st_knn_join.join(read_rdd(left_path), read_rdd(right_path), extractor_1, extractor_2)
    # exec join operator
    spark.sparkContext.parallelize(list(join_time)).repartition(1).saveAsTextFile(store_path)
    # save result
    spark.stop()
    # end spark
