import time

import pyspark

from shapely import wkt

import all_utils
from ST_knn import STKnnJoin
from extractor import STExtractor

if __name__ == "__main__":
    spark = pyspark.sql.SparkSession.builder.appName("st_knn").getOrCreate()

    alpha = 200
    beta = 40
    bin_num = 200
    delta_milli = 30 * 60 *24
    k = 15
    left_path = "./r"
    right_path = "./s"
    store_path = "./res"
    st_knn_join = STKnnJoin(delta_milli, k, alpha, beta, bin_num)  # instance a class


    def mapping(line):
        sp = line.split("\t")
        return (wkt.loads(sp[0]),
                (int(time.mktime(time.strptime(sp[1], "%Y-%m-%d %H:%M:%S"))),
                 int(time.mktime(time.strptime(sp[2], "%Y-%m-%d %H:%M:%S")))))


    def read_rdd(file_path):
        return spark.sparkContext.textFile(file_path).map(mapping)


    # read_rdd(left_path).repartition(1).saveAsTextFile(store_path)
    extractor_1 = STExtractor()
    extractor_2 = STExtractor()
    current_time = time.time()
    join_rdd = st_knn_join.join(read_rdd(left_path), read_rdd(right_path), extractor_1, extractor_2)


    # exec join operator
    # spark.sparkContext.parallelize(list(join_time)).repartition(1).saveAsTextFile(store_path)
    # save result
    def res_mapping(line):
        second = []
        for i in line[1]:
            if len(i) > 0:
                second.append((i[0][0][0].wkt,i[0][0][1],i[0][1]))
        return None if len(second)==0 else ((line[0][0].wkt,line[0][1]),second)

    res=join_rdd.map(res_mapping).filter(lambda x:x is not None)
    print(res.count())
    print(f"all time :{time.time()-current_time}")
    res.repartition(1).saveAsTextFile(store_path)
    spark.stop()
    # end spark
