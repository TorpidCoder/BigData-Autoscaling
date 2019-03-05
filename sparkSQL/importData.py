__author__ = "ResearchInMotion"

from pyspark.sql import *

spark = SparkSession.builder.appName("sql").getOrCreate()
fifa_dataset = spark.read.csv("/Users/sahilnagpal/Downloads/Spark/SparkSQL/fifa-world-cup/WorldCupMatches.csv",header=True,inferSchema=True)
fifa_dataset.createOrReplaceTempView('MatchesData')
# fifa_dataset.show()


#create file which consist of output / print the output data into a file

# groupByCityData = spark.sql("Select * from MatchesData ")
# groupByCityData.coalesce(1).write.csv("/Users/sahilnagpal/PycharmProjects/sparkSQL/myresults.csv")

fifa_dataset.printSchema()

#print column names and count

print(fifa_dataset.columns)

print(fifa_dataset.count())

print(len(fifa_dataset.columns))


#Fetching the whole data

# spark.sql("Select * from MatchesData").show()


fifa_dataset.select('Away Team Name').show()



