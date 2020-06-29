__author__ = "ResearchInMotion"

from pyspark import SparkContext, SparkConf

def isNotHeader(line: str):
    return not (line.startswith("host") and "bytes" in line)

if __name__ == "__main__":
    conf = SparkConf().setAppName("unionLogs").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    julyFirstLogs = sc.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/nasa_19950701.tsv")
    augustFirstLogs = sc.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/nasa_19950801.tsv")

    aggregatedLogLines = julyFirstLogs.union(augustFirstLogs)

    cleanLogLines = aggregatedLogLines.filter(isNotHeader)
    sample = cleanLogLines.sample(withReplacement = True, fraction = 0.1)

    for vals in sample.collect():
        print(vals)