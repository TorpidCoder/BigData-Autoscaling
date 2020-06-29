__author__ = "ResearchInMotion"

import findspark
findspark.init()

from pyspark import SparkConf , SparkContext
sparkconf = SparkConf().setAppName("Test").setMaster("local[*]")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("ERROR")

data = ["Sahil 25","Nikki 23","Manju 58","Vimal 60"]
rddData = sparkcont.parallelize(data)

pairRDD = rddData.map(lambda s : (s.split(" ")[0],s.split(" ")[1]))
pairRDD.coalesce(1).saveAsTextFile("/Users/sahilnagpal/PycharmProjects/spark-python/output/pairRDDRegularRDD")