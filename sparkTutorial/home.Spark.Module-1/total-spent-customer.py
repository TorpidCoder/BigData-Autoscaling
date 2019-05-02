__author__ = "ResearchInMotion"

from pyspark import SparkConf,SparkContext

sparkconf = SparkConf().setAppName("total customer spent").setMaster("local")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("ERROR")

def parseLine(line):
    field = line.split(",")
    customerID= int(field[0])
    spending = float(field[2])
    return customerID,spending

file = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/customer-orders.csv")
impfield = file.map(parseLine)
spend= impfield.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
averageSpending = spend.mapValues(lambda x:x[0]/x[1])
averageSpending.saveAsTextFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/Results/averageSpending/")
