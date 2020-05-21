__author__ = "ResearchInMotion"

import folium
import json
import numpy as np
import vincent
import pandas as pd
from folium.plugins import MiniMap
from folium.plugins import MarkerCluster
from folium.plugins import Draw
from folium.plugins import HeatMap
from folium import plugins
from folium.plugins import MousePosition


delimeter = input("File Delimeter : ")
data = pd.read_csv("/Users/sahilnagpal/PycharmProjects/BreakUpProject/whatsappchats.inputdata/LatLongData.csv" , sep=delimeter)

latCol = input("Please enter the Latitude Column Name : ")
latRow = int(input("Index of Latitude Column in your file : "))

longCol = input("Please enter the Longitude Column Name : ")
lonRow = int(input("Index of Longitude Column in your file  : "))

popupCol = input("Please enter the popup Column Name : ")
popupRow = int(input("Index of the pop up : "))

#elevationRow = int(input("Index of Elevation Column in your file  : "))

# creating the map layer here
print("Creating the map here")
some_map = folium.Map(location=[data[latCol].mean(),data[longCol].mean()],zoom_start = 16)
minimap = MiniMap()   # creating the mini map here
draw = Draw()   # custom draw here



# cluster layer
mc = MarkerCluster()
mc.layer_name = 'Clusters'




for row in data.itertuples():
    popup = folium.Popup()
    #folium.Vega(scatter_chart,height=350,width=650).add_to(popup)
    mc.add_child(folium.Marker(location=[row[latRow],row[lonRow]],
                            popup=row[popupRow]))
    # mc.add_child(folium.Marker(location=[row[latRow], row[lonRow]],
    #                            popup=row[elevationRow]))


# mouse position
MousePosition().add_to(some_map)


draw.add_to(some_map)
some_map.add_child(mc)
some_map.add_child(minimap)
folium.TileLayer("stamentoner").add_to(some_map)
folium.TileLayer("openstreetmap").add_to(some_map)
folium.LayerControl().add_to(some_map)
some_map.save('/Users/sahilnagpal/PycharmProjects/BreakUpProject/whatsappchats.maps/firstmap.html')