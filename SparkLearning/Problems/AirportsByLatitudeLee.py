__author__ = "ResearchInMotion"

import sys

sys.path.insert(0, '.')
from pyspark import SparkContext, SparkConf
from commons.Utils import Utils



def splitComma(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    return "{}, {}".format(splits[1], splits[6])


if __name__ == "__main__":
    conf = SparkConf().setAppName("airports").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    logs = sc.setLogLevel("ALL")

    airports = sc.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/airports.text")

    airportsInUSA = airports.filter(lambda line: float(Utils.COMMA_DELIMITER.split(line)[6]) > 40)

    airportsNameAndCityNames = airportsInUSA.map(splitComma)

    airportsNameAndCityNameschecked = airportsNameAndCityNames.flatMap(lambda x : x[0]+ x[1])

    airportsNameAndCityNames.saveAsTextFile("/Users/sahilnagpal/PycharmProjects/spark-python/output/airports_by_latitude.text")

input("Press ctrl+c to exit")