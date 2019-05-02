__author__ = "ResearchInMotion"


from pyspark import SparkConf,SparkContext
from operator import add

sparkconf =SparkConf().setAppName("wordCount").setMaster("local")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("ERROR")

def ParseLine(line):
    field = line.split(",")
    loc = field[0]
    entityID = field[2]
    temp = field[3]
    return (loc,entityID,temp)


file = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/data/1800.csv")
data = file.map(ParseLine)
filterline = data.filter((lambda x: "TMAX" in x[1]))
usedField = filterline.map(lambda x : (x[0],x[2]))
minTemp = usedField.reduceByKey(lambda x , y : min(x,y))

minTemp.saveAsTextFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/Results/minimumTemp_MAX")