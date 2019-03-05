__author__ = "ResearchInMotion"
from pyspark import SparkContext, SparkConf
from operator import add

sparkConf = SparkConf().setAppName("wordCount").setMaster("local")
sparkCont = SparkContext(conf=sparkConf)
log = sparkCont.setLogLevel("ERROR")

print("Spark Program Started")

line = sparkCont.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/sampleFile.txt")
count= line.flatMap(lambda x : x.split()).map(lambda x : (x,1)).reduceByKey(add)
out = count.collect()
outSort = sorted(out, key=lambda word:word[1])
for vals in outSort:
    print(vals)