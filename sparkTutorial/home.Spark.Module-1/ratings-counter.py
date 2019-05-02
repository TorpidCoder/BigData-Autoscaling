__author__ = "ResearchInMotion"

from pyspark import SparkConf,SparkContext
from operator import add
import collections

sparkconf =SparkConf().setAppName("wordCount").setMaster("local")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("ERROR")

file = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/data/u.data")
ratings = file.flatMap(lambda x:x.split()[2])
countofRatings = ratings.countByValue()

result = collections.OrderedDict(sorted(countofRatings.items()))

for keys , values in result.items():
    print(keys,"-->",values)

