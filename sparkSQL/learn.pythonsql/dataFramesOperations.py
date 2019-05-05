__author__ = "ResearchInMotion"


from pyspark.sql import SparkSession

from pyspark.sql import SQLContext

from pyspark import SparkConf

from pyspark.context import SparkContext

spark = SparkSession.builder.getOrCreate()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local").setAppName("Basic Operation"))

logs = sc.setLogLevel("ERROR")

sqlc = SQLContext(sparkContext=sc)

data = sqlc.read.format("com.databricks.spark.csv").option("header","true").load("/Users/sahilnagpal/PycharmProjects/sparkSQL/Data/sampleData.csv")


#printing the schema
print("--------------SCHEMA--------------")
data.printSchema()

print("----------INFORMATION-----------")
data.describe().show()

print("-----------TOP TWO ROWS----------")

print(data.head(2)[0])

print("=========SELECTED SPECIFIC COLUMN========")

data.select(['first_name','last_name']).show(20)

print("===========NORMAL DATA==============")

data.show(10)
print("========creating new column with same name but with new ID===========")

data.withColumn('newID',data['id']*2).show(10)

print("=========Rename a column======================")

data.withColumnRenamed('email','mailid').show(10)

print("========FOR SPARK SQL QUERIES==========")

data.createOrReplaceTempView('mainData')

results = spark.sql("select count(*) as Count from mainData group by gender")

results.show()
