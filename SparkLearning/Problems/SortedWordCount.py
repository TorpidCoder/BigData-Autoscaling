__author__ = "ResearchInMotion"

import findspark
findspark.init()

from pyspark import SparkContext ,SparkConf
sparkconf = SparkConf().setAppName("Test").setMaster("local[*]")
sparkcont = SparkContext(conf=sparkconf)
sparkcont.setLogLevel("ERROR")

data = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/word_count.text")
sepWords = data.flatMap(lambda values : values.split(" "))
wordcount = sepWords.map(lambda v : (v,1)).reduceByKey(lambda a,b : a+b)
sortedwordCount = wordcount.sortBy(lambda key :key[1],ascending=False)

for word , count in sortedwordCount.collect():
    print("{}----{}".format(word,count))
