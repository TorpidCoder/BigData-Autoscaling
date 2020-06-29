__author__ = "ResearchInMotion"

import findspark
findspark.init()

from pyspark.sql import SparkSession

AGE_MIDPOINT = "age_midpoint"
SALARY_MIDPOINT = "salary_midpoint"
SALARY_MIDPOINT_BUCKET = "salary_midpoint_bucket"

session = SparkSession.builder.appName("Test App").master("local[*]").getOrCreate()
dataFrameReader = session.read

data = dataFrameReader.option("header","true").option("inferschema" , value=True).\
    csv("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/2016-stack-overflow-survey-responses.csv")

print("****Printing the schema****")
data.printSchema()

newData = data.select("country","occupation",AGE_MIDPOINT , SALARY_MIDPOINT)

print("****Show New Data****")
newData.show(10)

print("****Records from Albania****")
countryRecord = newData.filter(newData["country"]=="Albania").show(10,False)

print("****Records from Albania****")
countryRecord = newData.filter(newData["country"]=="Albania").show(10,False)

print("****Count of Unique Occupation****")
occupationCount = newData.groupBy("occupation")
occupationCount.count().show(10,False)

print("****Print records with average mid age less than 20****")
avgAge20 = newData.filter(newData[AGE_MIDPOINT] < 20).show(10,False)

print("****Print the result by salary middle point in descending order****")
salaryOrder = newData.orderBy(newData[SALARY_MIDPOINT]).show()

print("****Group by country and aggregate by average salary middle point****")
groupCountry = newData.groupBy(SALARY_MIDPOINT)
AvgSalary = groupCountry.avg(SALARY_MIDPOINT).show(10,False)


