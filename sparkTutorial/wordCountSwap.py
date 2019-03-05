__author__ = "ResearchInMotion"

from pyspark import SparkConf,SparkContext
from operator import add

sparkconf = SparkConf().setAppName("word Count").setMaster("local")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("ALL")

def swap(xy):
    x,y=xy
    return y,x

file = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/sampleFile.txt")
words = file.flatMap(lambda x : x.split()).map(lambda x:(x,1)).reduceByKey(add)
swapdata = words.map(swap).sortByKey()
swapdata.saveAsTextFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/output")
