__author__ = "ResearchInMotion"

import findspark
findspark.init()

from pyspark import SparkContext ,SparkConf
sparkconf = SparkConf().setMaster("local[*]").setAppName("Turiya")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("ERROR")


data = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/word_count.text")
wordsonly = data.flatMap(lambda line : line.split(" "))
wordwithvalues = wordsonly.map(lambda line : (line,1))
wordcount = wordwithvalues.reduceByKey(lambda a,b : a+b).sortBy(lambda line : line[1])

for values in wordcount.collect():
    print(values)