__author__ = "ResearchInMotion"

import findspark
findspark.init()

from pyspark.sql import SparkSession
from lib.utils import get_spark_app_config
from pyspark.sql.functions import col
from pyspark.sql.functions import min
import sys

conf = get_spark_app_config()
spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .getOrCreate()

soccer_venue = spark \
    .read \
    .format("csv") \
    .option("inferSchema","true")\
    .option("header","true")\
    .load("/Users/sahilnagpal/Downloads/Sahil/soccer_venue.txt")


print("The Soccer Venue Table")
soccer_venue.printSchema()
soccer_venue.show()

print("Write a query in SQL to find the number of venues for EURO cup 2016.")
print(soccer_venue.select("venue_id").count())

player_mast = spark \
    .read \
    .format("csv") \
    .option("inferSchema","true")\
    .option("header","true")\
    .load("/Users/sahilnagpal/Downloads/Sahil/player_mast.txt")

print("The Player Mast Table")
player_mast.printSchema()
player_mast.show()

print("Write a query in SQL to find the number countries participated in the EURO cup 2016.")
print(player_mast.select("team_id").distinct().count())
print("The SQL Way to do the same")
player_mast.createOrReplaceTempView("player_mast")
spark.sql("select count(distinct(team_id)) as Teams from player_mast").show()

goal_details = spark \
    .read \
    .format("csv") \
    .option("inferSchema","true")\
    .option("header","true")\
    .load("/Users/sahilnagpal/Downloads/Sahil/goal_details.txt")

print("The Goal Details Table")
goal_details.printSchema()
goal_details.show()

print("Write a query in SQL to find the number goals scored in EURO cup 2016 within normal play schedule.")
print(goal_details.where("goal_schedule = 'NT'").count())


match_mast = spark \
    .read \
    .format("csv") \
    .option("inferSchema","true")\
    .option("header","true")\
    .load("/Users/sahilnagpal/Downloads/Sahil/match_mast.txt")

print("The Goal Details Table")
match_mast.printSchema()
match_mast.show()

print("Write a query in SQL to find the number of matches ended with a result.")
print(match_mast.where("results != 'DRAW'").count())

print("Write a query in SQL to find the number of matches ended with draws.")
print(match_mast.where("results = 'DRAW'").count())

print("Write a query in SQL to find the date when did Football EURO cup 2016 begin.")
print(match_mast.select(min("play_date")).first())

print("Write a query in SQL to find the number of self-goals scored in EURO cup 2016.")
print(goal_details.where("goal_type = 'O'").count())


penalty_shootout = spark \
    .read \
    .format("csv") \
    .option("inferSchema","true")\
    .option("header","true")\
    .load("/Users/sahilnagpal/Downloads/Sahil/penalty_shootout.txt")

print("The Goal Details Table")
penalty_shootout.printSchema()
penalty_shootout.show()

print("Write a query in SQL to find the number of matches got a result by penalty shootout.")
print(penalty_shootout.where("score_goal = 'Y'").count())

print("Write a query in SQL to find the number of matches were decided on penalties in the Round of 16.")
print(match_mast.where((col("decided_by") == 'P') & (col("play_stage") == 'R')).count())

print("Write a query in SQL to find the number of goal scored in every match within normal play schedule.")
print(goal_details.where("goal_schedule = 'NT'").count())





