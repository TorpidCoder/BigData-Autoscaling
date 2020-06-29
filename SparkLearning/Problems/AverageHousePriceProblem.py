__author__ = "ResearchInMotion"

import findspark
findspark.init()

from pyspark import SparkContext , SparkConf
sparkconf = SparkConf().setAppName("Test").setMaster("local[*]")
sparkcont = SparkContext(conf=sparkconf)
sparkcont.setLogLevel("ERROR")

def requiredColumn(line):
    field = line.split(",")
    price = float(field[2])
    bedroom = field[3]
    return  bedroom , price

data = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/RealEstate.csv")
requireddata = data.map(requiredColumn)
price_avg = requireddata.mapValues(lambda v : (v,1)).reduceByKey(lambda a,b : (a[0]+b[0],a[1]+b[1])).mapValues(lambda v :v[0]/v[1])
price_avg.coalesce(1).saveAsTextFile("/Users/sahilnagpal/PycharmProjects/spark-python/output/AverageHousePrice")
#requireddata.coalesce(1).saveAsTextFile("/Users/sahilnagpal/PycharmProjects/spark-python/output/requireddata")