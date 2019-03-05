__author__ = "ResearchInMotion"

from pyspark import SparkContext,SparkConf
from operator import add

sparkconf = SparkConf().setMaster("local").setAppName("Most powerfull super hero")
sparcont = SparkContext(conf=sparkconf)
logs =sparcont.setLogLevel("ERROR")

def CountCoOcuurences(line):
    elements = line.split()
    return (int(elements[0]) , len(elements)-1)

def parseNames(line):
    field = line.split('\"')
    return (int(field[0]),field[1].encode("utf8"))

def swap(xy):
    x,y=xy
    return y,x

names = sparcont.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/Marvel-names.txt")
namesRDD = names.map(parseNames)

lines = sparcont.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/Marvel-graph.txt")
linesRDD = lines.map(CountCoOcuurences)

totalFriendsByCharacter = linesRDD.reduceByKey(add)
flipped = totalFriendsByCharacter.map(swap)

mostPopular = flipped.max()

mostPopularName= namesRDD.lookup(mostPopular[1])[0]

print(str(mostPopularName) + " is the most popular superhero, with " + (str(mostPopular[0]) + " co-appearances."))


