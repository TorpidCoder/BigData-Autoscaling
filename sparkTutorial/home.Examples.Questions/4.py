__author__ = "ResearchInMotion"

from pyspark import SparkContext,SparkConf
from operator import add


sparkconf = SparkConf().setAppName("word Count").setMaster("local")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("ERROR")

file = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/sampleFile.txt")
totalWord = file.flatMap(lambda x :x.split(" ")).count()
print("The total count is : ",totalWord)
wordCount = file.flatMap(lambda x :x.split(" ")).map(lambda x : (x,1)).reduceByKey(add)
wordCount.saveAsTextFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/output")