__author__ = "ResearchInMotion"

import findspark
findspark.init()

from pyspark import SparkContext , SparkConf

sparkconf = SparkConf().setMaster("local[*]").setAppName("Product of Numbers")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("ERROR")

data = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/prime_nums.text")
splitdata = data.flatMap(lambda number : number.split("\t"))
normaldata = splitdata.filter(lambda number : number)
intdata = normaldata.map(lambda numbers : int(numbers))

# getting the product here
product = intdata.reduce(lambda a,b : a+b)
print(product)