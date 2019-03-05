__author__ = "ResearchInMotion"
from pyspark import SparkConf,SparkContext
import re
sparkconf = SparkConf().setAppName("Question 2").setMaster("local")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("ERROR")

def replacecharacter(line):
    mystring = line.replace('\r\n',' ')
    return mystring

avoidingWords = ['am','is','a','to']
file= sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/sampleFile.txt")
filteredlines = file.flatMap(lambda x :x.split(" ")).filter(lambda values : values not in avoidingWords)
filteredlines.saveAsTextFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/output")

