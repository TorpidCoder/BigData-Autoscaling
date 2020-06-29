__author__ = "ResearchInMotion"

import findspark
findspark.init()

from pyspark import SparkContext , SparkConf
sparkconf = SparkConf().setMaster("local[*]").setAppName("Test")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("ERROR")

data = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/word_count.text")
wordsdata = data.flatMap(lambda line : line.split(" "))
wordmap = wordsdata.map(lambda word : (word,1))

countwords = wordmap.reduceByKey(lambda x,y:x+y)

for words , count in countwords.collect():
    print("{} : {}".format(words,count))

