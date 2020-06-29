__author__ = "ResearchInMotion"

import findspark
findspark.init()

from pyspark import SparkContext , SparkConf

sparkconf = SparkConf().setMaster("local[*]").setAppName("PairRDD")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("ERROR")

data = [("Sahil",25),("Manju",58),("Vimal",60),("Nikki",23)]
rddData = sparkcont.parallelize(data)

rddData.coalesce(1).saveAsTextFile("/Users/sahilnagpal/PycharmProjects/spark-python/output/pairRDD")