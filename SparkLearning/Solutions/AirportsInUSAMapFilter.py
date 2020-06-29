__author__ = "ResearchInMotion"

import findspark
findspark.init()
from pyspark import SparkContext , SparkConf
sparkconf =SparkConf().setAppName("Airports in USA").setMaster("local[*]")
sparkcont = SparkContext(conf=sparkconf)

logs = sparkcont.setLogLevel("ERROR")

def requiredfield(line):
    field = line.split(",")
    nameofAirport = field[1]
    cityofAirport = field[2]
    country = field[3]
    return nameofAirport , cityofAirport , country

data = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/airports.text")
actualdata = data.map(requiredfield).filter(lambda line : line[2] == '"United States"').saveAsTextFile("/Users/sahilnagpal/PycharmProjects/spark-python/output/testoutput")
