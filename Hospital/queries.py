__author__ = "ResearchInMotion"

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct

spark = SparkSession \
    .builder \
    .master("local[*]")\
    .appName("Hospital Data Queries ")\
    .getOrCreate()

nurse = spark.read.format("json")\
    .option("inferSchema","true")\
    .load("/Users/sahilnagpal/Desktop/Spark/Spark-Programming-In-Python/HospitalData/Nurse.json")

nurse.printSchema()
nurse.show()

physician = spark.read.format("json")\
    .option("inferSchema","true")\
    .load("/Users/sahilnagpal/Desktop/Spark/Spark-Programming-In-Python/HospitalData/Physician.json")

physician.printSchema()
physician.show()

department = spark.read.format("json")\
    .option("inferSchema","true")\
    .load("/Users/sahilnagpal/Desktop/Spark/Spark-Programming-In-Python/HospitalData/Department.json")

department.printSchema()
department.show()

appointment = spark.read.format("csv")\
    .option("inferSchema","true")\
    .option("header","true")\
    .load("/Users/sahilnagpal/Desktop/Spark/Spark-Programming-In-Python/HospitalData/Appointment.csv")

appointment.printSchema()
appointment.show()


room = spark.read.format("json")\
    .option("inferSchema","true")\
    .option("header","true")\
    .load("/Users/sahilnagpal/Desktop/Spark/Spark-Programming-In-Python/HospitalData/Rooms.json")

room.printSchema()
room.show()


print("Query-1")
nurse.select("EmployeeID","Name","Position","SSN","Registered").where("Registered == 0").show()
print("Query-2")
nurse.select("EmployeeID","Name","Position","SSN","Registered").where(col("Position") == "Head Nurse").show()
print("Query-3")
expr = physician.EmployeeID == department.Head
physician = physician.withColumnRenamed("Name","PhyName")
physician.join(department,expr).select("PhyName").show()
print("Query-4")
appointment.createOrReplaceTempView("appointment")
spark.sql("select Patient,count(AppointmentID) as Count from appointment group by Patient HAVING Count > 1").show()
appointment.groupBy("Patient").agg(countDistinct("AppointmentID")).where("count(AppointmentID) > 1").show()
print("Query-5")
room.select("BlockFloor","BlockCode").where(col("Number") == 212).show()
print("Query-6")
print(room.where(col("unavailable")==0).count())
print("Query-7")
print(room.where(col("unavailable")==1).count())
print("Query-8")

