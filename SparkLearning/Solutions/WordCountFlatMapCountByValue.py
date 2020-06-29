__author__ = "ResearchInMotion"

import findspark
findspark.init()
from pyspark import SparkContext , SparkConf
sparkconf =SparkConf().setAppName("Airports in USA").setMaster("local[*]")
sparkcont = SparkContext(conf=sparkconf)

logs = sparkcont.setLogLevel("ERROR")


data = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/word_count.text")
words = data.flatMap(lambda line : line.split(" ")).map(lambda vals : (vals,1)).reduceByKey(lambda a, b : a+b).sortBy(lambda line : line[1])
for keys , values in words.collect():
    print("{} {}".format(keys,values))