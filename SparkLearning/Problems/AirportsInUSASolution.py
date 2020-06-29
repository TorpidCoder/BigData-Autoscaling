__author__ = "ResearchInMotion"

import findspark
findspark.init()

from pyspark import SparkContext , SparkConf
sparkconf = SparkConf().setAppName("Airports in USA").setMaster("local[*]")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("ERROR")


def certainColumns(lines):
    line = lines.split(",")
    airportName = line[1]
    airportCity = line[2]
    countryName = line[3]
    return airportName,airportCity,countryName


data = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/airports.text")
mainData = data.map(certainColumns)
OnlyUSA = mainData.filter(lambda value : value[2] == '"United States"')
OnlyUSA.coalesce(1).saveAsTextFile("/Users/sahilnagpal/PycharmProjects/spark-python/output/airportinUSA")

