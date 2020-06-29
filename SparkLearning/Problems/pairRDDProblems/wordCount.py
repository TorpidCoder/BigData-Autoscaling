__author__ = "ResearchInMotion"

import findspark
import sys
findspark.init()

from pyspark import SparkContext , SparkConf
sparkconf = SparkConf().setAppName("Word Count").setMaster("local[*]")
sparkcont = SparkContext(conf=sparkconf)
sparkcont.setLogLevel("ALL")

inputData = sys.argv[1]


data = sparkcont.textFile(inputData)
wordcount = data.flatMap(lambda values : values.split(" ")).map(lambda vals : (vals,1)).reduceByKey(lambda a,b :a+b).sortBy(lambda a : a[1])

for obj in wordcount.collect():
    print(obj)

