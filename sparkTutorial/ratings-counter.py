__author__ = "ResearchInMotion"

from pyspark import SparkConf , SparkContext

import collections

sparkcof = SparkConf().setMaster("local").setAppName("Ratings Counter")

sparkcon = SparkContext(conf=sparkcof)

log = sparkcon.setLogLevel("All")

file = sparkcon.textFile("/Users/sahilnagpal/Downloads/ml-100k/u.data")

ratings = file.map(lambda x : x.split()[2])

result = ratings.countByValue()

sortedResult = collections.OrderedDict(sorted(result.items()))

for key , value in sortedResult.items():
    print(key,"-->",value)