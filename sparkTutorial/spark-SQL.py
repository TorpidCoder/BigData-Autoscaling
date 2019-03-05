__author__ = "ResearchInMotion"


from pyspark.sql import SparkSession
from pyspark.sql import Row

import collections

sparksession = SparkSession.builder.appName("SparkSQL").getOrCreate()

def mapper(line):
    field = line.split(",")
    return Row(ID=int(field[0]), name=str(field[1].encode("utf-8")), age=int(field[2]), numFriends=int(field[3]))

lines = sparksession.sparkContext.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/fakefriends.csv")
logs = sparksession.sparkContext.setLogLevel("ERROR")
people = lines.map(mapper)


schemaPeople = sparksession.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

teenagers = sparksession.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")



schemaPeople.groupBy("age").count().orderBy("age").show()

sparksession.stop()