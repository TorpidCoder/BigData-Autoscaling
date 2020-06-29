__author__ = "ResearchInMotion"

import findspark
findspark.init()

from pyspark.sql import SparkSession

session = SparkSession.builder.appName("Test Name").master("local[*]").getOrCreate()

dataFrameReader = session.read

data = dataFrameReader.option("header","True").option("inferschema",value=True).csv("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/RealEstate.csv")

print("****Print Schema****")
data.printSchema()

print("****Query****")
avgPrice = data.groupBy("Location").avg("PriceSQFt").withColumnRenamed("avg(PriceSQFt)", "avgvalues").orderBy("avgvalues").show()
