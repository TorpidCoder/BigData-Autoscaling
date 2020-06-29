__author__ = "ResearchInMotion"

import findspark
findspark.init()
from pyspark import SparkContext , SparkConf
sparkconf =SparkConf().setAppName("Airports by Latitude").setMaster("local[*]")
sparkcont = SparkContext(conf=sparkconf)

logs = sparkcont.setLogLevel("ERROR")

def requiredfield(line):
    field = line.split(",")
    nameofAirport = field[1]
    latitude = float(field[6])
    return nameofAirport , latitude

data = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/airports.text")
latmorethan40 = data.map(requiredfield).filter(lambda line : line[1] > 40).collect()

for values in latmorethan40:
    print(values)