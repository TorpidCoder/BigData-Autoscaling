__author__ = "ResearchInMotion"

import findspark
findspark.init()

from pyspark.sql import SparkSession
from lib.utils import get_spark_app_config
import sys

config = get_spark_app_config()
spark = SparkSession \
    .builder \
    .config(conf=config) \
    .getOrCreate()

data = spark.read \
    .format("csv") \
    .option("header","true")\
    .option("inferSchema","true")\
    .load(sys.argv[1])

updatedData=data.withColumnRenamed("State Name","StateName")
updatedData=updatedData.withColumnRenamed("Product Name","ProductName")
updatedData=updatedData.withColumnRenamed("Date Received","DateReceived")
updatedData=updatedData.withColumnRenamed("Date Sent to Company","DateSenttoCompany")


print("Complaints recieved in the State of the New York")

updatedData.filter("StateName == 'NY'").select("ProductName","Issue","StateName" , "Complaint ID").show()

print("Complaints recieved in the State of the New York and California")

updatedData.filter("StateName IN ('NY','CA')")\
    .select("ProductName","Issue","StateName" , "Complaint ID").show()

print("Word Credit in Product Field")

updatedData.createOrReplaceTempView("updatedData")
spark.sql("select * from updatedData where ProductName like 'Cre%' limit 10 ").show()

print("Word Late in Issue Field")
spark.sql("select * from updatedData where Issue like 'La%' limit 10 ").show()

print("Complaints Count on Same Day")
print(updatedData.filter("DateReceived == DateSenttoCompany").count())
