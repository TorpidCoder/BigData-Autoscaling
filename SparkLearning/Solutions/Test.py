import findspark
findspark.init()

from pyspark import SparkContext , SparkConf
sparkconf = SparkConf().setAppName("Test App").setMaster("local[*]")
sparkcont = SparkContext(conf=sparkconf)
sparkcont.setLogLevel("ERROR")




words = ["one", "two", "two", "three", "three", "three"]
wordsPairRdd = sparkcont.parallelize(words).map(lambda word: (word, 1))

wordCountsWithReduceByKey = wordsPairRdd \
    .reduceByKey(lambda x, y: x + y) \
    .collect()
print("wordCountsWithReduceByKey: {}".format(list(wordCountsWithReduceByKey)))

wordCountsWithGroupByKey = wordsPairRdd \
    .groupByKey() \
    .mapValues(len) \
    .collect()
print("wordCountsWithGroupByKey: {}".format(list(wordCountsWithGroupByKey)))

