__author__ = "ResearchInMotion"


from pyspark import SparkConf,SparkContext
from operator import add


sparkconf = SparkConf().setAppName("popular movies").setMaster("local")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("ALL")

def loadMovies():
    moviesName ={}
    with open("/Users/sahilnagpal/PycharmProjects/sparkTutorial/u.item", encoding="ISO-8859-1") as file:
        for line in file:
            field = line.split("|")
            moviesName[int(field[0])]=field[1]
    return moviesName


def swap(xy):
    x,y=xy
    return (y,x)


nameDict = sparkcont.broadcast(loadMovies())

data = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/u.data")
movies = data.map(lambda x :(int(x.split()[1]),1)).reduceByKey(add)
moviesSwap = movies.map(swap)
sortedMovies = moviesSwap.sortByKey()

sortedMovieswithNames = sortedMovies.map(lambda countMovie : (nameDict.value[countMovie[1]],countMovie[0]))


sortedMovieswithNames.saveAsTextFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/output")
