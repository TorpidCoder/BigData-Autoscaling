__author__ = "ResearchInMotion"

import findspark
findspark.init()

from pyspark import SparkContext , SparkConf
sparkconf = SparkConf().setMaster("local[*]").setAppName("test")
sparkcont = SparkContext(conf=sparkconf)
sparkcont.setLogLevel("All")

def lat_longs(lines):
    fields = lines.split(",")
    nameofAirport = fields[1]
    city = fields[2]
    country = fields[3]
    return nameofAirport , city , country

data = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/airports.text")
filteredData = data.map(lat_longs).filter(lambda line : line[2] =='"United States"')


for values in  filteredData.take(5):
    print(values)