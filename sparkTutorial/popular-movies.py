__author__ = "ResearchInMotion"

from pyspark import SparkConf,SparkContext
from operator import add

def swap(xy):
    x,y=xy
    return (y,x)


sparkconf = SparkConf().setAppName("Popular Movies").setMaster("local")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("ALL")



file = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/u.data")
movie = file.map(lambda x:(int(x.split()[1]),1)).reduceByKey(add)

swapMovie = movie.map(swap)
result = swapMovie.sortByKey()
result.saveAsTextFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/output")
