__author__ = "ResearchInMotion"

from pyspark import SparkContext , SparkConf


sparkconf = SparkConf().setAppName("Airports By Latitude").setMaster("local[4]")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("WARN")


def lat_longs(lines):
    fields = lines.split(",")
    nameofAirport = fields[1]
    city = fields[2]
    country = fields[3]
    return nameofAirport , city , country





data = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/airports.text")
latitudes_only = data.map(lat_longs)
greater_latitude = latitudes_only.filter(lambda line : line[2] == '"United States"' )
greater_latitude.saveAsTextFile("/Users/sahilnagpal/PycharmProjects/spark-python/output/unitedstatesairport")
