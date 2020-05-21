__author__ = "ResearchInMotion"

import csv
import sys

inputfile = "/Users/sahilnagpal/PycharmProjects/BreakUpProject/whatsappchats.outputdata/actualoutput/part-00000"

with open("/Users/sahilnagpal/PycharmProjects/BreakUpProject/whatsappchats.outputdata/test.csv","w")  as f:
    writer=csv.writer(f, delimiter=",", lineterminator="\r\n")
    writer.writerows(inputfile)
