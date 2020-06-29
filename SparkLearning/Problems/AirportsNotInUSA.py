__author__ = "ResearchInMotion"

import findspark
findspark.init()
import sys

sys.path.insert(0, '.')
from pyspark import SparkContext, SparkConf
from commons.Utils import Utils
sparkconf = SparkConf().setAppName("Test").setMaster("local[*]")
sparkcont = SparkContext(conf=sparkconf)



logs = sparkcont.setLogLevel("ERROR")

data = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/airports.text")
pairRDD = data.map(lambda line : (Utils.COMMA_DELIMITER.split(line)[1],
                   Utils.COMMA_DELIMITER.split(line)[3]))
airportsNotInUSA = pairRDD.filter(lambda keyvalue : keyvalue[1] != "\"United States\"")
airportsNotInUSA.coalesce(1).saveAsTextFile("/Users/sahilnagpal/PycharmProjects/spark-python/output/airportsNotInUSA")