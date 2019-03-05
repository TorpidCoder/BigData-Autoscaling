__author__ = "ResearchInMotion"


from pyspark import SparkContext,SparkConf


sparkconf = SparkConf().setAppName("Minimum temperature").setMaster("local")
sparkcont = SparkContext(conf=sparkconf)

logs =sparkcont.setLogLevel("ALL")

def ParseLine(line):
    field = line.split(",")
    location = field[0]
    entityid= field[2]
    temp = float(field[3])*0.1 * (9.0 / 5.0) + 32.0
    return (location,entityid,temp)


file = sparkcont.textFile("/Users/sahilnagpal/PycharmProjects/sparkTutorial/1800.csv")
parseLine= file.map(ParseLine)
filteredField = parseLine.filter(lambda x : "TMIN" in x[1])
parseLines = filteredField.map(lambda x : (x[0], x[2]))
minTemps = parseLines.reduceByKey(lambda x,y : min(x,y))


output = minTemps.collect()

for vals in output:
    print(vals[0] + "\t {:.3f}F".format(vals[1]))



