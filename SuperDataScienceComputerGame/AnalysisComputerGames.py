__author__ = "ResearchInMotion"

import findspark
findspark.init()

from pyspark.sql import SparkSession
from lib.utils import get_spark_app_config
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import StringType

import sys

config = get_spark_app_config()
spark = SparkSession \
    .builder \
    .config(conf=config) \
    .getOrCreate()

consoleDates = spark.read \
    .format("csv") \
    .option("header","true")\
    .option("inferSchema","true")\
    .load(sys.argv[1])

consoleGames = spark.read \
    .format("csv") \
    .option("header","true")\
    .option("inferSchema","true")\
    .load(sys.argv[2])

consoleDates.printSchema()
consoleDates.show()
consoleGames.printSchema()
consoleGames.show()

#creating a UDF
def splitUDF(row):
    if '-' in row:
        yyyy, mm, dd = row.split("-")
    else:
        pass
    return [dd,mm,yyyy]

Sum_NA_Sales = consoleGames.agg(sum("NA_Sales").alias("Sum_NA_Sales")).collect()
Sum_EU_Sales = consoleGames.agg(sum("EU_Sales").alias("Sum_EU_Sales")).collect()
Sum_JP_Sales = consoleGames.agg(sum("JP_Sales").alias("Sum_JP_Sales")).collect()
Sum_Other_Sales = consoleGames.agg(sum("Other_Sales").alias("Sum_Other_Sales")).collect()

Sum_NA_Sales = Sum_NA_Sales[0][0]
Sum_EU_Sales = Sum_EU_Sales[0][0]
Sum_JP_Sales = Sum_JP_Sales[0][0]
Sum_Other_Sales = Sum_Other_Sales[0][0]

result = Sum_NA_Sales /(Sum_EU_Sales+Sum_JP_Sales+Sum_Other_Sales)
print("Percentage of North America: ",result*100)

print("Console Games Ordered by Platform name and Year")
consoleGames.createOrReplaceTempView("consoleGames")
spark.sql("select * from consoleGames order by Platform DESC , Year ASC").show()

print("Each game title extract first four letters")
consoleGames.withColumn("Name",substring("Name",1,4)).select("Name","Publisher").show()

print("Platforms before Christmas")
# datSplitterUDF = udf(lambda row : splitUDF(row),ArrayType(StringType()))
# consoleDates\
# .select(datSplitterUDF(consoleDates.FirstRetailAvailability).alias("dt"))\
# .withColumn('day',col('dt').getItem(0).cast('int'))\
# .withColumn('month',col('dt').getItem(1).cast('int'))\
# .withColumn('year',col('dt').getItem(2).cast('int'))\
# .show()

print("Longetivity in Descending order")
consoleDates.withColumn("daysDiff",datediff("Discontinued","FirstRetailAvailability")).orderBy(col("daysDiff")).show()

