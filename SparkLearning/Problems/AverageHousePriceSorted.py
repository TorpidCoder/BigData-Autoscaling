__author__ = "ResearchInMotion"

import findspark
findspark.init()

from pyspark import SparkConf , SparkContext
sparkconf = SparkConf().setAppName("Test").setMaster("local[*]")
sparkcont = SparkContext(conf=sparkconf)
sparkcont.setLogLevel("ERROR")

def requiredCol(line):
    field = line.split(",")
    price = float(field[2])
    bedroom = field[3]
    return bedroom, price


data = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/RealEstate.csv")
requiredData = data.map(requiredCol)
averageData = requiredData.mapValues(lambda v : (v,1)).reduceByKey(lambda a,b : (a[0]+b[0],a[1]+b[1])).mapValues(lambda v : v[0]/v[1])
sortedAverageData = averageData.sortByKey(ascending=False)
sortedAverageData.coalesce(1).saveAsTextFile("/Users/sahilnagpal/PycharmProjects/spark-python/output/AverageHousePriceSorted")
