__author__ = "ResearchInMotion"

from pyspark import SparkContext , SparkConf


sparkconf = SparkConf().setAppName("Airports By Latitude").setMaster("local[*]")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("WARN")


def lat_longs(lines):
    fields = lines.split(",")
    airportname = fields[1]
    latitude = float(fields[6])
    return airportname,latitude

data = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/airports.text")
latitudes_only = data.map(lat_longs)
greater_latitude = latitudes_only.filter(lambda line : line[1] > 40 )
greater_latitude.saveAsTextFile("/Users/sahilnagpal/PycharmProjects/spark-python/output/latitudedata")
