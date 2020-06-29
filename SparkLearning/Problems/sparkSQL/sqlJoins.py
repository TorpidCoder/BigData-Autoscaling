__author__ = "ResearchInMotion"


import findspark
findspark.init()

from pyspark.sql import SparkSession , functions as f

session = SparkSession.builder.appName("Test").master("local[*]").getOrCreate()

makerspace = session.read.option("header" , "true").csv("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/uk-makerspaces-identifiable-data.csv")

postalcode = session.read.option("header","true").csv("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/uk-postcode.csv").withColumn("PostCode" , f.concat_ws("",f.col("PostCode"),f.lit(" ")))

makerspace.select("Name of makerspace" , "Postcode").show(10,False)

