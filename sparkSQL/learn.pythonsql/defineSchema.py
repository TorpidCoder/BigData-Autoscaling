__author__ = "ResearchInMotion"


from pyspark.sql.types import (StructField,StringType,
                               IntegerType,StructType)

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("schema builder").getOrCreate()


data_schema = [StructField('age',IntegerType(),True),
               StructField('name',StringType(),True)]

final_struct = StructType(fields=data_schema)

data = spark.read.json(path="/Users/sahilnagpal/PycharmProjects/sparkSQL/people.json",schema=final_struct)

data.printSchema()

data.show()

data.describe().show()

print(type(data['age']))

data.select('age').show()



