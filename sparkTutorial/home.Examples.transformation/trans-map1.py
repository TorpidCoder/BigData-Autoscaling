__author__ = "ResearchInMotion"

from pyspark import SparkContext , SparkConf


sparkconf = SparkConf().setMaster("local").setAppName("map example")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("ERROR")

def square(num):
    return num*num

file = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/file.txt")
elements = file.flatMap(lambda x:x.split(",")).map(lambda x:int(x)).map(square)
elements.saveAsTextFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/output")