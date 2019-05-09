__author__ = "ResearchInMotion"

from pyspark.sql.functions import countDistinct,avg,stddev,format_number

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("aggs").getOrCreate()

data = spark.read.csv("/Users/sahilnagpal/PycharmProjects/sparkSQL/Data/sales_info.csv", inferSchema=True,header=True)

data.show()


print("=======Averga eSales=======")

data.select(avg('Sales').alias("AVERAGE SALES")).show()

print("=======STD DEV=========")

data.select(stddev('Sales')).show()


#format number

std_data = data.select(stddev('Sales').alias("AVERAGE SALES"))


std_data.select(format_number('AVERAGE SALES',2).alias('std')).show()


print("======ORDER BY ======")

data.orderBy('Sales').show()

#descending

data.orderBy(data['Sales'].desc()).show()

