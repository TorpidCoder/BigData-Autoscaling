__author__ = "ResearchInMotion"

from pyspark import SparkContext , SparkConf


sparkconf = SparkConf().setAppName("Airports By Latitude").setMaster("local[*]")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("WARN")

def valueCols(lines):
    fields = lines.split("\t")
    hostname = fields[0]
    return hostname


data = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/nasa_19950701.tsv")
hostdata = data.map(valueCols)
data2 = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/nasa_19950801.tsv")
hostdata2 = data2.map(valueCols)
actualhost = hostdata.intersection(hostdata2)
for vals in actualhost.collect():
    print(vals)


