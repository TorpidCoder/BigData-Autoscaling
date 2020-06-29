__author__ = "ResearchInMotion"

import findspark
findspark.init()

from pyspark import SparkContext , SparkConf
sparkconf = SparkConf().setMaster("local[*]").setAppName("test")
sparkcont = SparkContext(conf=sparkconf)
sparkcont.setLogLevel("WARN")



data = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/prime_nums.text")
actualdata = data.flatMap(lambda line:line.split("\t"))
onlynumbers = actualdata.filter(lambda line : line)
intdata = onlynumbers.map(lambda line : int(line)).reduce(lambda x,y:x+y)

print(intdata)
