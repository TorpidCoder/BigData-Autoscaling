__author__ = "ResearchInMotion"

from pyspark.sql import SparkSession

from pyspark.sql.functions import (dayofmonth,hour,dayofyear,month,year,weekofyear,format_number)


spark = SparkSession.builder.appName("Time and Date").getOrCreate()

data = spark.read.csv("/Users/sahilnagpal/PycharmProjects/sparkSQL/Data/appl_stock.csv" , header=True , inferSchema=True)

data.show(10)

print("====only two columns=======")

data.select(['Date','Open']).show(10)

print("======DATE FUNCTIONS==========")

print("Day of the month")

data.select(dayofmonth(data['Date'])).show(10)

print("Hour of the day")

data.select(hour(data['Date'])).show(10)

print("month")

data.select(month(data['Date'])).show(10)

print("AVERAGE CLOSING PRICE")

#creating the year column

# data.withColumn("Year",year(data['Date'])).show(10)

new_data = data.withColumn("Year",year(data['Date']))

result = new_data.groupBy("Year").mean().select(["Year","avg(Close)"])

result.show()

