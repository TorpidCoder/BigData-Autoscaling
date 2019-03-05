__author__ = "ResearchInMotion"

from pyspark import SparkConf,SparkContext
sparkconf = SparkConf().setAppName("Question 1").setMaster("local")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("ERROR")

file=sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/sampleFile.txt")

def lowercase(line):
    return line.lower()


wordsSplit =file.flatMap(lambda x:x.split(" ")).map(lowercase)
print(wordsSplit.take(5))