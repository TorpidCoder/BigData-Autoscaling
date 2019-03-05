__author__ = "ResearchInMotion"

from pyspark import SparkContext,SparkConf

sparkconf = SparkConf().setAppName("Friends By Age").setMaster("local")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("ALL")


def parse(line):
    field = line.split(",")
    age = int(field[2])
    number = int(field[3])
    return (age,number)


line = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/fakefriends.csv")
rdd = line.map(parse)
totalsbyAge = rdd.mapValues(lambda x :(x,1)).reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1]))
averagesbyAge = totalsbyAge.mapValues(lambda x : (x[0]/x[1]))
# averagesbyAge.saveAsTextFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/output")

result = averagesbyAge.collect()

for vals in result:
    print(vals)


