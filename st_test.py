import random
import time
import unittest

import pyspark
from shapely import wkt

class MyTestCase(unittest.TestCase):
    def test_read_text(self):
        spark = pyspark.sql.SparkSession.builder.appName("st_knn").getOrCreate()
        def mapping(line):
            sp = line.split("\t")
            return (wkt.loads(sp[0]), (int(time.mktime(time.strptime(sp[1], "%Y-%m-%d %H:%M:%S"))),
                                   int(time.mktime(time.strptime(sp[2], "%Y-%m-%d %H:%M:%S")))))
        def read_rdd(file_path):
            return spark.sparkContext.textFile(file_path).map(mapping)
        read_rdd("./r").repartition(1).saveAsTextFile("./res")
if __name__ == '__main__':
    unittest.main()
