__author__ = "ResearchInMotion"

from pyspark import SparkConf,SparkContext
from operator import add

sparkconf = SparkConf().setAppName("Word Count Swap").setMaster("local")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("ERROR")

file = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/data/Book.txt")
word = file.flatMap(lambda x:x.split())
wordcount = word.countByValue()

for word , count in wordcount.items():
    cleanword = word.encode("ascii","ignore")
    if cleanword:
        print(cleanword.decode()," ",str(count))