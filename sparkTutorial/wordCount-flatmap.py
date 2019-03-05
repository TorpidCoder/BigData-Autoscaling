__author__ = "ResearchInMotion"

from pyspark import SparkContext, SparkConf

sc = SparkConf().setMaster("local")
st = SparkContext(conf=sc)
logs = st.setLogLevel("ERROR")

file = st.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/Book.txt")
word = file.flatMap(lambda x : x.split())
wordCount = word.countByValue()




for word , count in wordCount.items():
    cleanword = word.encode('ascii','ignore')
    if(cleanword):
        print(cleanword.decode() + " " + str(count))