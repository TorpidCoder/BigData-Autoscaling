__author__ = "ResearchInMotion"

from pyspark import SparkConf,SparkContext
import re
sparkconf = SparkConf().setAppName("Question 2").setMaster("local")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("ERROR")



data = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/sampleFile.txt")
groupbydata = data.groupBy(lambda x : x[0:3]).saveAsTextFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/output")