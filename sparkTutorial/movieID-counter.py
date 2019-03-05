__author__ = "ResearchInMotion"


from pyspark import SparkContext,SparkConf
import collections
sparkConf = SparkConf().setMaster("local").setAppName("MovieID counter")
sparkCont = SparkContext(conf=sparkConf)

lines = sparkCont.textFile("/Users/sahilnagpal/Downloads/ml-100k/u.data")
movieID = lines.map(lambda x:x.split()[1])
count = movieID.countByValue()

sortedresults = collections.OrderedDict(sorted(count.items()))
for keys , values in sortedresults.items():
    print(keys,"-->",values)