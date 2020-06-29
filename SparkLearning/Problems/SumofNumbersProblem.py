__author__ = "ResearchInMotion"


from pyspark import SparkContext , SparkConf






sparkconf = SparkConf().setAppName("Airports By Latitude").setMaster("local[*]")
sparkcont = SparkContext(conf=sparkconf)
logs = sparkcont.setLogLevel("WARN")


data = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/prime_nums.text")
numberData = data.flatMap(lambda line : line.split("\t"))
numberonly = numberData.filter(lambda number : number)
intnumbers = numberonly.map(lambda number : int(number))

addnumbers = intnumbers.reduce(lambda x , y : x+y)
print(addnumbers)






