__author__ = "ResearchInMotion"

import findspark
findspark.init()
import sys
from pyspark import SparkConf,SparkContext


#spark conf and spark cont
sparkconf = SparkConf().setAppName("Average House Price Problem").setMaster("local[*]")
sparkcont = SparkContext(conf=sparkconf)

#logs
sparkcont.setLogLevel("ERROR")

def requiredCol(lines):
    field = lines.split(",")
    bedroom = field[3]
    price = float(field[2])
    return bedroom,price


#reading the data file
data = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/RealEstate.csv")
actualdata = data.map(requiredCol)
averagehousePrice =actualdata.mapValues(lambda val : (val,1)).reduceByKey(lambda a,b : (a[0]+b[0],a[1]+b[1])).mapValues(lambda v : v[0]/v[1])
for values in averagehousePrice.collect():
    print(values)


