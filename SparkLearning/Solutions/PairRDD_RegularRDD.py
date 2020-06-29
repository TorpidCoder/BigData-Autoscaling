__author__ = "ResearchInMotion"


import findspark
findspark.init()

from pyspark import  SparkContext , SparkConf
sparkconf = SparkConf().setMaster("local[*]").setAppName("test")
sparkcont = SparkContext(conf=sparkconf)
sparkcont.setLogLevel("ERROR")


data = ["sahil 26" , "aardhana 27"]
rdddata = sparkcont.parallelize(data)
pairrdd = rdddata.map(lambda line : (line.split(" ")[0],line.split(" ")[1])).collect()
for key , value in pairrdd:
    print(key,value)