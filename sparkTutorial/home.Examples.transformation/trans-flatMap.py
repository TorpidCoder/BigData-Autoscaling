__author__ = "ResearchInMotion"

from pyspark import SparkConf,SparkContext


sparkconf = SparkConf().setAppName("flatmap").setMaster("local")
sparkcont = SparkContext(conf=sparkconf)

logs = sparkcont.setLogLevel("ERROR")

def upper(line):
    return line.upper()

file = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/sampleFile.txt")
splitNewLine = file.flatMap(lambda x :x.split(" "))
upperWord = splitNewLine.map(upper)

upperWord.saveAsTextFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/output2")