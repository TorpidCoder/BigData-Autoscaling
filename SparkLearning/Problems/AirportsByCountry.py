__author__ = "ResearchInMotion"

import findspark
import sys
findspark.init()
from commons.Utils import Utils

from pyspark import SparkContext , SparkConf
sparkconf = SparkConf().setMaster("local[1]").setAppName("test")
sparkcont = SparkContext(conf=sparkconf)
sparkcont.setLogLevel("ERROR")

data = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/airports.text")
requiredData = data.map(lambda airport : (Utils.COMMA_DELIMITER.split(airport)[3],Utils.COMMA_DELIMITER.split(airport)[1]))

airportbycountry = requiredData.groupByKey()

for country , airportName in airportbycountry.collectAsMap().items():
    print("{}---{}".format(country,list(airportName)))
