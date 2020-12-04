__author__ = "ResearchInMotion"

import findspark
findspark.init()

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, max, dense_rank

spark = SparkSession \
    .builder \
    .appName("Football Queries") \
    .master("local[*]") \
    .getOrCreate()



match_details = spark.read\
    .format("csv")\
    .option("inferSchema","true")\
    .option("header","true")\
    .load("/Users/sahilnagpal/Downloads/Sahil/match_details.txt")

match_details.printSchema()
print("Match Details Data")
match_details.show()

soccer_country = spark.read\
    .format("csv")\
    .option("inferSchema","true")\
    .option("header","true")\
    .load("/Users/sahilnagpal/Downloads/Sahil/soccer_country.txt")

soccer_country.printSchema()
print("Soccer Country Data")
soccer_country.show()

print("Write a query in SQL to find the teams played the first match of EURO cup 2016.")

soccer_country.withColumnRenamed("country_name","countryname")
express = match_details.team_id == soccer_country.country_id
match_details.join(soccer_country,express).where("match_no==1").select(col("country_name")).show()

print("Write a query in SQL to find the teams played the first match of EURO cup 2016.- SQL")

soccer_country.createOrReplaceTempView("soccer_country")
match_details.createOrReplaceTempView("match_details")
spark.sql("select country_abbr , country_name from soccer_country INNER JOIN match_details ON soccer_country.country_id == match_details.team_id where match_details.match_no ==1").show()


print("Write a query in SQL to find the winner of EURO cup 2016.")

express = match_details.team_id == soccer_country.country_id
match_details.join(soccer_country,express).where((col("play_stage")=='F')& (col("win_lose")=='W')).select(col("country_name")).show()

print("Write a query in SQL to find the winner of EURO cup 2016.- SQL")
match_details.createOrReplaceTempView("match_details")
spark.sql("select country_name from soccer_country INNER JOIN match_details ON soccer_country.country_id == match_details.team_id where match_details.play_stage == 'F' AND match_details.win_lose == 'W'").show()


match_mast = spark \
    .read \
    .format("csv") \
    .option("inferSchema","true")\
    .option("header","true")\
    .load("/Users/sahilnagpal/Downloads/Sahil/match_mast.txt")

print("The Goal Details Table")
match_mast.printSchema()
match_mast.show()

print("Write a query in SQL to find the match with match no, play stage, goal scored, and the audience which was the heighest audience match.")

match_mast.createOrReplaceTempView("match_mast")
spark.sql("select * from match_mast where audence = (select MAX(audence) from match_mast)").show()

print("Write a query in SQL to find the match no in which Germany played against Poland")
express = match_details.team_id == soccer_country.country_id
match_details.join(soccer_country,express).where("team_id in (1213,1208)").select("match_no").show()

print("Write a query in SQL to find the match no in which Germany played against Poland - SQL")
match_details.createOrReplaceTempView("match_details")
soccer_country.createOrReplaceTempView("soccer_country")
spark.sql("select match_no from match_details LEFT JOIN soccer_country ON match_details.team_id == soccer_country.country_id where soccer_country.country_id in (1208,1213) group by match_details.match_no having count(match_no) = 2").show()

print("Write a query in SQL to find the match no, play stage, date of match, number of gole scored, and the result of the match where Portugal played against Hungary. ")

express = match_details.team_id == soccer_country.country_id
join1 = match_details.join(soccer_country,express).where("team_id in (1214,1209)")
join1 = join1.withColumnRenamed("match_no","matchno")
join1 = join1.withColumnRenamed("play_stage","playstage")
join1 = join1.withColumnRenamed("goal_score","goalscore")
express2 = join1.matchno == match_mast.match_no
match_mast.join(join1,express2).select("play_date").show()

print("Write a query in SQL to display the list of players scored number of goals in every matches. ")

goal_details = spark \
    .read \
    .format("csv") \
    .option("inferSchema","true")\
    .option("header","true")\
    .load("/Users/sahilnagpal/Downloads/Sahil/goal_details.txt")

print("The Goal Details Table")
goal_details.printSchema()
goal_details.show()

player_mast = spark \
    .read \
    .format("csv") \
    .option("inferSchema","true")\
    .option("header","true")\
    .load("/Users/sahilnagpal/Downloads/Sahil/player_mast.txt")

print("The Goal Details Table")
player_mast.printSchema()
player_mast.show()

express = player_mast.player_id == goal_details.player_id
player_mast.join(goal_details,express).select("match_no","player_name").show()

print("Write a query in SQL to display the list of players scored number of goals in every matches. -SQL ")
goal_details.createOrReplaceTempView("goal_details")
player_mast.createOrReplaceTempView("player_mast")
spark.sql("select player_name from player_mast LEFT JOIN goal_details ON player_mast.player_id == goal_details.player_id where goal_details.goal_type in ('N','P')").show()


print("Write a query in SQL to find the 2nd highest stoppage time which had been added in 2nd half of play.")
match_mast.createOrReplaceTempView("match_mast")
spark.sql("select MAX(stop2_sec) from match_mast where stop2_sec < (select MAX(stop2_sec) from match_mast)").show()


print("Write a query in SQL to find the match no, date of play and the 2nd highest stoppage time which have been added in the 2nd half of play.")
newmatch_mast = Window().partitionBy(col("stop2_sec")).orderBy("stop2_sec")
match_mast.withColumn("result",dense_rank().over(newmatch_mast)).filter("result=2").show()

print("Write a query in SQL to find the team which was defeated by Portugal in EURO cup 2016 final.")
soccer_country.createOrReplaceTempView("soccer_country")
match_details.createOrReplaceTempView("match_details")
spark.sql("select country_name from soccer_country INNER JOIN match_details ON soccer_country.country_id == match_details.team_id WHERE match_details.play_stage = 'F' AND match_details.team_id != 1214").show()


print("Write a query in SQL to find the team which was defeated by Portugal in EURO cup 2016 final.")
player_mast.createOrReplaceTempView("player_mast")
spark.sql("select playing_club,count(player_name) as count from player_mast group by playing_club order by count").show()
player_mast.groupBy("playing_club").count().select("playing_club","count").orderBy("count").show()


print("Write a query in SQL to find the player and his jersey number Who scored the first penalty of the tournament.")
player_mast.createOrReplaceTempView("player_mast")
goal_details.createOrReplaceTempView("goal_details")
spark.sql("select jersey_no from player_mast LEFT JOIN goal_details ON player_mast.player_id == goal_details.player_id where goal_details.match_no = 1 and goal_details.goal_type  = 'P'").show()











