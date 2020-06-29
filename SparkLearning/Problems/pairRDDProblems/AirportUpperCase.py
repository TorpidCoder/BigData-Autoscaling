__author__ = "ResearchInMotion"

import findspark
findspark.init()

from pyspark import SparkContext ,SparkConf
sparkconf = SparkConf().setMaster("local[*]").setAppName("Turiya")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("ERROR")

def lat_longs(lines):
    fields = lines.split(",")
    nameofAirport = fields[1]
    country = fields[3]
    return nameofAirport , country

data = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/airports.text")

upperCaseValues = data.map(lat_longs)
newvals = upperCaseValues.mapValues(lambda country : country.upper())
print(newvals.take(5))