__author__ = "ResearchInMotion"

from pyspark.sql import SparkSession
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
import csv
from pyspark import SparkConf
from pyspark.context import SparkContext


spark = SparkSession.builder.appName("Read CSV").getOrCreate()


sparkcont = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
logs = sparkcont.setLogLevel("ERROR")

#Read the csv

sqlc= SQLContext(sparkContext=sparkcont)
data = sqlc.read.format("com.databricks.spark.csv").option("header","true").load("/Users/sahilnagpal/PycharmProjects/sparkSQL/Data/sampleData.csv")
data.show(100)

print("==============SELECT PARTICULAR COLUMN================")

data.select('last_name').show(100)

print("===============GROUP BY================================")

data.groupBy('gender').count().show()




