__author__ = "ResearchInMotion"

import sys
sys.path.insert(0, '.')
from AvgCount import AvgCount
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


if __name__ == "__main__":
    conf = SparkConf().setAppName("averageHousePriceSolution").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    inputpath = str(sys.argv[1])
    outputpath = str(sys.argv[2])

    lines = sc.textFile(inputpath)
    cleanedLines = lines.filter(lambda line: "Bedrooms" not in line)
    housePricePairRdd = cleanedLines.map(lambda line: \
    ((int(float(line.split(",")[3]))), AvgCount(1, float(line.split(",")[2]))))

    housePriceTotal = housePricePairRdd.reduceByKey(lambda x, y: \
        AvgCount(x.count + y.count, x.total + y.total))

    housePriceAvg = housePriceTotal.mapValues(lambda avgCount: avgCount.total / avgCount.count)

    sortedHousePriceAvg = housePriceAvg.sortByKey()

    sortedHousePriceAvg.coalesce(1).saveAsTextFile(outputpath)


    # inputpath - /Users/sahilnagpal/PycharmProjects/BreakUpProject/whatsappchats.inputdata/RealEstate.csv
    # outputpath - /Users/sahilnagpal/PycharmProjects/BreakUpProject/whatsappchats.outputdata/actualoutput


