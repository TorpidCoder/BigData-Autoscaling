__author__ = "ResearchInMotion"

import findspark
findspark.init()

from pyspark import  SparkContext , SparkConf
sparkconf = SparkConf().setMaster("local[*]").setAppName("test")
sparkcont = SparkContext(conf=sparkconf)
sparkcont.setLogLevel("ERROR")

data = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/word_count.text")
requiredData = data.flatMap(lambda line : line.split(" "))
newrequiredData = requiredData.map(lambda word : (word,1))
countvals = newrequiredData.reduceByKey(lambda x , y : (x + y))
print(countvals.take(5))
