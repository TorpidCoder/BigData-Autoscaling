__author__ = "ResearchInMotion"

import pandas as pd

sparkdata = pd.read_csv("/Users/sahilnagpal/PycharmProjects/BreakUpProject/whatsappchats.outputdata/file.csv")
latlongdata = pd.read_csv("/Users/sahilnagpal/PycharmProjects/BreakUpProject/whatsappchats.inputdata/RealEstate.csv")
newdata = pd.merge(sparkdata,latlongdata,how='left')
print(newdata)