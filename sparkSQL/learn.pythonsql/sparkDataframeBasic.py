__author__ = "ResearchInMotion"


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Basic").getOrCreate()

data = spark.read.json('/Users/sahilnagpal/PycharmProjects/sparkSQL/people.json').cache()

data.show()

print("-------------------------")

data.describe().show()

print("--------------------------")

data.columns

print("--------------------------")

data.printSchema()


