__author__ = "ResearchInMotion"


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("aggs").getOrCreate()

data = spark.read.csv("/Users/sahilnagpal/PycharmProjects/sparkSQL/Data/sales_info.csv", inferSchema=True,header=True)

data.show()

print("========SCHEMA==========")

data.printSchema()

print("=========GROUP BY BY COMPANY=========")

data.groupBy('Company').count().show()

print("=======AVERGAE SALES BY COMPANY========")

data.groupBy('Company').mean().show()

print("========SUM OF ALL SALES========")

data.agg({'Sales':'sum'}).show()

data.agg({'Sales':'max'}).show()

#by company wise

group_data = data.groupBy('Company')

group_data.agg({'Sales':'sum'}).show()
