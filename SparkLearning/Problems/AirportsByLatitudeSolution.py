__author__ = "ResearchInMotion"

import findspark
findspark.init()

from pyspark import SparkContext , SparkConf

sparkconf = SparkConf().setMaster("local[*]").setAppName("Airports by local")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("ERROR")

def columnChecks(line):
    cols = line.split(",")
    airportName = cols[1]
    aiportLatitude = float(cols[6])
    return airportName,aiportLatitude


data = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/airports.text")
columndata = data.map(columnChecks).filter(lambda x : x[1]>40)
columndata.coalesce(1).saveAsTextFile("/Users/sahilnagpal/PycharmProjects/spark-python/output/airport2")