import findspark
findspark.init()

from pyspark import SparkContext , SparkConf
sparkconf = SparkConf().setMaster("local[*]").setAppName("test")
sparkcont = SparkContext(conf=sparkconf)
sparkcont.setLogLevel("WARN")

julydata = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/nasa_19950701.tsv")
augustdata = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/spark-python/inputFiles/nasa_19950801.tsv")

headerclearjuly = julydata.filter(lambda line : "logname" not in line)
headerclearaugust = augustdata.filter(lambda line : "logname" not in line)

newData = headerclearjuly.union(headerclearaugust)
sampledata = newData.sample(withReplacement=True,fraction=0.1)
sampledata.coalesce(1).saveAsTextFile("/Users/sahilnagpal/PycharmProjects/spark-python/output/testoutput")


