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

ownersdata = spark\
    .read\
    .format("csv")\
    .option("inferSchema","true")\
    .option("header","true")\
    .load(sys.argv[1])
ownersdata.createOrReplaceTempView("ownersdata")

petsdata = spark\
    .read\
    .format("csv")\
    .option("inferSchema","true")\
    .option("header","true")\
    .load(sys.argv[2])
petsdata.createOrReplaceTempView("petsdata")

proceduredetaildata = spark\
    .read\
    .format("csv")\
    .option("inferSchema","true")\
    .option("header","true")\
    .load(sys.argv[3])
proceduredetaildata.createOrReplaceTempView("proceduredetaildata")

procedurehistorydata = spark\
    .read\
    .format("csv")\
    .option("inferSchema","true")\
    .option("header","true")\
    .load(sys.argv[4])
procedurehistorydata.createOrReplaceTempView("procedurehistorydata")

ownersdata.show()
petsdata.show()
proceduredetaildata.show()
procedurehistorydata.show()

print("Pet Names and Owner Name Side by Side")
ownersdata = ownersdata.withColumnRenamed("Name","OwnerName")
petsdata = petsdata.withColumnRenamed("Name","PetName")
exprr = ownersdata.OwnerID == petsdata.OwnerID
ownersdata.join(petsdata,exprr).select("OwnerName","PetName").show()


print("Pet Names and Procedure")
exprr = petsdata.PetID == procedurehistorydata.PetID
petsdata.join(procedurehistorydata,exprr).select("PetName","ProcedureType").show()


print("Procedure Type")
proceduredetaildata=proceduredetaildata.withColumnRenamed("ProcedureType","ProcedureTypeDetail")
exprr = proceduredetaildata.ProcedureSubCode == procedurehistorydata.ProcedureSubCode
proceduredetaildata.join(procedurehistorydata,exprr).select("Description","ProcedureTypeDetail").show()

print("Procedure Type")
proceduredetaildata=proceduredetaildata.withColumnRenamed("ProcedureType","ProcedureTypeDetail")
exprr = proceduredetaildata.ProcedureSubCode == procedurehistorydata.ProcedureSubCode
proceduredetaildata.join(procedurehistorydata,exprr).select("Description","ProcedureTypeDetail").show()


print("Price and Owner")
spark.sql("select Name from ownersdata as OD INNER JOIN petsdata as PD ON OD.OwnerID = PD.OwnerID INNER JOIN procedurehistorydata as PHD ON OD.PetID = PHD.PetID INNER JOIN proceduredetaildata as PDD ON PDD.ProcedureType = PHD.ProcedureType").show()
