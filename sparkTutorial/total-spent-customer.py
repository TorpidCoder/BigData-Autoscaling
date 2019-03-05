__author__ = "ResearchInMotion"

from pyspark import SparkConf,SparkContext

sparkconf = SparkConf().setAppName("total customer spent").setMaster("local")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("ALL")

def parseline(line):
    field = line.split(",")
    customerID = int(field[0])
    price = float(field[2])
    return (customerID,price)


file = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/customer-orders.csv")
meanData = file.map(parseline)
averagePrice= meanData.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y : (x[0] + y[0] , x[1] , y[1]))
averagePriceValues = averagePrice.mapValues(lambda x : x[0]/x[1]).sortByKey()
averagePriceValues.saveAsTextFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/output2")
