import time

import pyspark
from shapely import wkt
from ST_knn import STKnnJoin


if __name__ == "__main__":
    spark = pyspark.sql.SparkSession.builder.appName("st_knn").getOrCreate()

    alpha = 200
    beta = 40
    bin_num = 200
    delta_milli = 60 * 60 *24
    k = 15
    left_path = "hdfs://127.0.0.1:9000/user/hadoop/r"
    right_path = "hdfs://127.0.0.1:9000/user/hadoop/s"
    store_path = "hdfs://127.0.0.1:9000/user/hadoop/res"
    st_knn_join = STKnnJoin(delta_milli, k, alpha, beta, bin_num)  # instance a class


    def mapping(line):
        sp = line.split("\t")
        return (wkt.loads(sp[0]),
                (int(time.mktime(time.strptime(sp[1], "%Y-%m-%d %H:%M:%S"))),
                 int(time.mktime(time.strptime(sp[2], "%Y-%m-%d %H:%M:%S")))))


    def read_rdd(file_path):
        return spark.sparkContext.textFile(file_path).map(mapping)


    # read_rdd(left_path).repartition(1).saveAsTextFile(store_path)
    current_time = time.time()
    join_rdd = st_knn_join.join(read_rdd(left_path), read_rdd(right_path))


    # exec join operator
    # spark.sparkContext.parallelize(list(join_time)).repartition(1).saveAsTextFile(store_path)
    # save result
    def res_mapping(line):
        second = []
        for i in line[1]:
            if len(i) > 0:
                second.append((i[0][0][0],i[0][0][1],i[0][1]))
        return None if len(second)==0 else ((line[0][0],line[0][1]),second)
    # def res_mapping(line):
    #     second = []
    #     for i in line[1]:
    #         if len(i) > 0:
    #             second.append(i[0][0][0].wkt)
    #     return None if len(second)==0 else (line[0][0].wkt,second)
    res=join_rdd.map(res_mapping).filter(lambda x:x is not None)
    print(res.count())
    print(f"all time :{time.time()-current_time}")
    res.repartition(1).saveAsPickleFile(store_path)
    spark.stop()
    # end spark
