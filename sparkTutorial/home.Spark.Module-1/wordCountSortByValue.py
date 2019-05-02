__author__ = "ResearchInMotion"


from pyspark import SparkConf,SparkContext
from operator import add

sparkconf = SparkConf().setAppName("Word Count Swap").setMaster("local")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("ALL")

def swap(xy):
    x,y=xy
    return y,x


file = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/data/sampleFile.txt")
words = file.flatMap(lambda x:x.split(" "))
wordCount = words.map(lambda x:(x,1)).reduceByKey(add)
swapwordcount = wordCount.map(swap).sortByKey()

swapwordcount.saveAsTextFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/Results/swapWordCount/")
