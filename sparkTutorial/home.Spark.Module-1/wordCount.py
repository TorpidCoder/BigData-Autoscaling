__author__ = "ResearchInMotion"

from pyspark import SparkConf,SparkContext
from operator import add

sparkconf =SparkConf().setAppName("wordCount").setMaster("local")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("ERROR")

file = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/data/sampleFile.txt")
word = file.flatMap(lambda d : d.split(" ")).map(lambda x:(x,1)).reduceByKey(add)
word.saveAsTextFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/Results/wordCountResult/")