__author__ = "ResearchInMotion"

import findspark
findspark.init()

from pyspark import SparkContext , SparkConf
sparkconf = SparkConf().setMaster("local[*]").setAppName("Same host problem")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("ERROR")

def hostname(line):
    lines = line.split("\t")
    hostname = lines[0]
    return hostname

file1 = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/nasa_19950701.tsv")
file2 = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/nasa_19950801.tsv")

file1hostdata = file1.map(hostname)
file2hostdata = file2.map(hostname)

actualhostname = file1hostdata.intersection(file2hostdata)

actualhostnamerdd = sparkcont.parallelize(actualhostname.collect())
actualhostnamerdd.coalesce(1).saveAsTextFile("/Users/sahilnagpal/PycharmProjects/spark-python/output/hostnames/samehost/")

