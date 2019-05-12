__author__ = "ResearchInMotion"

from pyspark.sql import SparkSession
from pyspark.sql.functions import format_number

spark = SparkSession.builder.appName("Spark Project").getOrCreate()

#loading the walmart data
print("Loading the walmart data")
data = spark.read.csv("/Users/sahilnagpal/PycharmProjects/sparkSQL/Data/walmart_stock.csv",header=True,inferSchema=True)

print("------------------------------")


print("Schema Printing")

data.printSchema()

print("------------------------------")

print("First Five Columns")

data.show(5)

print("------------------------------")


print("Describe Data")

data.describe().show()

print("------------------------------")

print("Starting of bonus questions")

print("Casting and formating numbers ")

result = data.describe()

result.select(result['summary'],
                format_number(result['Open'].cast('float'),2).alias("Open"),
                format_number(result['High'].cast('float'), 2).alias("High"),
                format_number(result['Low'].cast('float'), 2).alias("Low"),
                format_number(result['Close'].cast('float'), 2).alias("Close"),
                result['Volume'].cast("int").alias('Volume')).show(10)



print("------------------------------")

print("creating a new data frame with HV ratio")

data2 = data.withColumn("HVRatio",data['High']/data['Volume'])
data2.select('HVRatio').show(10)

data2.show(10)

print("------------------------------")

print("HVRatio is on peak HVRatioday")

max_HV = data2.agg({'HVRatio':"max"}).show()

# results = data2.createOrReplaceTempView("datatemplate")
#
# print("Template created")
#
# query1 = spark.sql("select MAX(HVRatio) from datatemplate")
#
# query1.show()HVRatio

print(data2.orderBy(data2['High'].desc()).head(1)[0])

print("------------------------------")

print("Mean of close colum")

from pyspark.sql.functions import mean,max , min

data2.select(mean("Close")).show()


print("------------------------------")

print("Max and Min of Volume column")


max_Volume = data2.agg({'Volume':"max"}).show()
min_Volume = data2.agg({'Volume':"min"}).show()

data2.select(max('Volume'),min('Volume')).show()

print("------------------------------")

print("days Close lower than 60")

print(data2.filter('Close<60').count())


print("------------------------------")

print("percentage High greater than 80")

print(data2.filter(data2['High']>80).count()/data2.count()*100)




