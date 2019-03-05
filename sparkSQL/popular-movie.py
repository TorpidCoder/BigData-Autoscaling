from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def loadMovieNames():
    movieNames = {}
    with open("/Users/sahilnagpal/PycharmProjects/sparkTutorial/u.ITEM",encoding = "ISO-8859-1") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

# Create a SparkSession (the config bit is only for Windows!)
spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# Load up our movie ID -> name dictionary
nameDict = loadMovieNames()

# Get the raw data
lines = spark.sparkContext.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/u.data")
# Convert it to a RDD of Row objects
movies = lines.map(lambda x: Row(movieID =int(x.split()[1])))
# Convert that to a DataFrame
movieDataset = spark.createDataFrame(movies)

# Some SQL-style magic to sort all movies by popularity in one line!
topMovieIDs = movieDataset.groupBy("movieID").count().orderBy("count", ascending=False).cache()

# Show the results at this point:

#|movieID|count|
#+-------+-----+
#|     50|  584|
#|    258|  509|
#|    100|  508|

topMovieIDs.coalesce(1).write.csv("/Users/sahilnagpal/PycharmProjects/sparkSQL/myresults.csv")












