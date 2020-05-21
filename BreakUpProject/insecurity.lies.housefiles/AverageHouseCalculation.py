__author__ = "ResearchInMotion"

from pyspark import SparkContext , SparkConf
import sys
import findspark
import csv

findspark.init()

sparkconf = SparkConf().setAppName("Average House Problem").setMaster("local[*]")
sparkcont = SparkContext(conf=sparkconf)
sparkcont.setLogLevel("ERROR")

#reading the data using arguement
inputdata = str(sys.argv[1])
outputdata = str(sys.argv[2])

#fetch the right column
def requiredColumn(line):
    field = line.split(",")
    price = float(field[2])
    bedroom = field[3]
    return  bedroom , price

def toCSVLine(data):
  return ','.join(str(d) for d in data)

data = sparkcont.textFile(inputdata)
requireddata = data.map(requiredColumn)
price_avg = requireddata.mapValues(lambda v : (v,1)).reduceByKey(lambda a,b : (a[0]+b[0],a[1]+b[1])).mapValues(lambda v :v[0]/v[1])
price_avg_csv = price_avg.map(toCSVLine)
price_avg_csv.coalesce(1).saveAsTextFile(outputdata)

with open("/Users/sahilnagpal/PycharmProjects/BreakUpProject/whatsappchats.outputdata/file.csv", "w") as csv_file:
    for line in price_avg_csv.collect():
        csv_file.write(line)
        csv_file.write('\n')



