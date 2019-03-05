__author__ = "ResearchInMotion"

from pyspark import SparkContext,SparkConf

sparkconf = SparkConf().setMaster("local").setAppName("Minimum Temperture Location")
sparkcont = SparkContext(conf=sparkconf)

logs = sparkcont.setLogLevel("ALL")

def ParseLine(line):
    field = line.split(",")
    loc = field[0]
    entityID = field[2]
    temp = field[3]
    return (loc,entityID,temp)

file = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/1800.csv")
parseLine = file.map(ParseLine)
filteredLine = parseLine.filter(lambda x : "TMAX" in x[1])
usedField = filteredLine.map(lambda x : (x[0],x[2]))
minTemp = usedField.reduceByKey(lambda x , y : min(x,y))

minTemp.saveAsTextFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/output")
