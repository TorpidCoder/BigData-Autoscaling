__author__ = "ResearchInMotion"

from pyspark import SparkConf,SparkContext


sparkconf =SparkConf().setAppName("wordCount").setMaster("local")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("ERROR")

def parse(line):
    field = line.split(",")
    age = int(field[2])
    number = int(field[3])
    return (age,number)


file = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/data/fakefriends.csv")
friends = file.map(parse)

totalsfriend = friends.mapValues(lambda x : (x,1)).reduceByKey(lambda x,y:(x[0] + y[0] , x[1] + y[1]))
averageByFriends = totalsfriend.mapValues(lambda x : (x[0]/x[1]))



averageByFriends.saveAsTextFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/Results/FriendsByAge")
