import time

import pyspark
import pickle
from shapely import wkt
from ST_knn import STKnnJoin


class Sample:
    def __init__(self) -> None:
        self._spark = pyspark.sql.SparkSession.builder.appName("st_knn").getOrCreate()
        store_path = "hdfs://127.0.0.1:9000/user/hadoop/res"
        self._rdd:pyspark.RDD = self._spark.sparkContext.pickleFile(store_path)
    def sample(self,num:int=10)->list:
        return self._rdd.takeSample(False,num)
    def __del__(self):
        self._spark.stop()