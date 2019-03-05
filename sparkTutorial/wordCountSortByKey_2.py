__author__ = "ResearchInMotion"
from pyspark import SparkContext, SparkConf
from operator import add
import os

os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python3"
os.environ["SPARK_HOME"] = "/Users/sahilnagpal/Downloads/spark-2.4.0-bin-hadoop2.7/bin/"

sparkConf = SparkConf().setAppName("wordCount").setMaster("local")
sparkCont = SparkContext(conf=sparkConf)
log = sparkCont.setLogLevel("ERROR")

print("Spark Program Started")

line = sparkCont.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/sampleFile.txt")
count = line.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).reduceByKey(add).sortByKey()
count.saveAsTextFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/output")