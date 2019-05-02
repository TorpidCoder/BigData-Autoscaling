__author__ = "ResearchInMotion"

from pyspark import SparkConf,SparkContext
from operator import add
import os
import shutil
choice = input("Please enter your choice : ")
removeDir = input("Either say YES or NO")

output_directory = "/Users/sahilnagpal/PycharmProjects/sparkTutorial/Results/wordCountResult/"

if(os.path.exists(output_directory)==True):
    print("Directory already present")

    if(removeDir=="YES"):

        shutil.rmtree(output_directory)
    else:
        print("Directory already present")
else:
    print("Creating a new Directory")

if(choice=="Yes"):

    sparkconf =SparkConf().setAppName("wordCount").setMaster("local")
    sparkcont = SparkContext(conf=sparkconf)
    logs = sparkcont.setLogLevel("ERROR")

    file = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/data/sampleFile.txt")
    word = file.flatMap(lambda d : d.split(" ")).map(lambda x:(x,1)).reduceByKey(add)
    word.saveAsTextFile(output_directory)

elif(choice=="No"):
    print("Ok Not Running.........")

else:
    print("Wrong Input")