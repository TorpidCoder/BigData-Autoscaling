__author__ = "ResearchInMotion"

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkConf
from pyspark.context import SparkContext

spark = SparkSession.builder.appName("Basic Operations").getOrCreate()

sc = SparkContext.getOrCreate(SparkConf().setAppName("Basic Operations").setMaster("local"))

logs = sc.setLogLevel("ERROR")

sqlc = SQLContext(sparkContext=sc)

data = sqlc.read.format("com.databricks.spark.csv").option("header","true").load("/Users/sahilnagpal/PycharmProjects/sparkSQL/Data/appl_stock.csv")

count = data.count()

print(count)

data.printSchema()

print("------using of Filter statement-------")

data.filter('close<500').show()

print("-------count of filter columns --------")

print(data.filter('close<500').count())

print("-------select other columns in filter----------")

data.filter('close<500').select(['Open','Volume']).show(10)

print("----------query-----------")

data.filter('low = 197.16').select(['Date']).show()

