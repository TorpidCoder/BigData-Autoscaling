from pyspark import SparkContext, SparkConf
from operator import add

sparkConf = SparkConf().setAppName("wordCount").setMaster("local")
sparkCont = SparkContext(conf=sparkConf)
log = sparkCont.setLogLevel("ALL")

print("Spark Program Started")

line = sparkCont.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/sampleFile.txt")
rdd = line.flatMap(lambda x : x.split())
newrdd = rdd.map(lambda x : (x,1)).reduceByKey(add)

newrdd.saveAsTextFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/output")



