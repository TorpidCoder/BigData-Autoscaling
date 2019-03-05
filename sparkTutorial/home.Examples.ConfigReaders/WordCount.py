__author__ = "ResearchInMotion"

import configparser
from pyspark import SparkContext,SparkConf
from operator import add

config = configparser.RawConfigParser()
configfilepath = "/Users/sahilnagpal/PycharmProjects/sparkTutorial/home.Examples.ConfigReaders/configfile.txt"
config.read(configfilepath)

master = config.get("wordCount","master")
appname = config.get("wordCount","appname")
inputfile = config.get("wordCount","inputfile")
outputfile = config.get("wordCount","outputfile")
print("arguements --",master,"--",appname)

sparkconf = SparkConf().setAppName(appname).setMaster(master)
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("ERROR")
print("Spark Started")
file = sparkcont.textFile(inputfile)
wordsOnly = file.flatMap(lambda x:x.split(" "))
wordCount = wordsOnly.count()
print("The total count is ",wordCount)
countwords = wordsOnly.map(lambda x:(x,1)).reduceByKey(add)
countwords.saveAsTextFile(outputfile)


