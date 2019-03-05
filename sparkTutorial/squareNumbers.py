__author__ = "ResearchInMotion"

from pyspark import SparkConf , SparkContext

sparkConf = SparkConf().setAppName("square of numbers").setMaster("local")

sparkCont = SparkContext(conf=sparkConf)

logs = sparkCont.setLogLevel("ERROR")

rdd1 = sparkCont.parallelize([1,2,3,4,5])
rdd2 = rdd1.map(lambda x :x*x)

values = rdd2.collect()

print(values)

#Another way to do the same thing

def sqaure(x):
    return x*x

rdd3 = rdd1.map(sqaure)

values2 = rdd3.collect()
countcheck = rdd3.countByValue()

print(values2)
print(countcheck)

