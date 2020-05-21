__author__ = "ResearchInMotion"

import pandas as pd
import folium
from folium.plugins import MiniMap
from folium.plugins import MarkerCluster
from folium.plugins import MousePosition
from folium.plugins import Draw

df = pd.read_csv("/Users/sahilnagpal/PycharmProjects/BreakUpProject/whatsappchats.inputdata/LatLongData.csv",sep=",")

map_center = [df["Lat"].mean(), df["Long"].mean()]
mainmap = folium.Map(location=map_center, zoom_start=6)

# adding the mini map
minimap = MiniMap()

# adding the Cluster layer
mc = MarkerCluster()
mc.layer_name = 'Clusters'

# mouse position
MousePosition().add_to(mainmap)


# custom draw here
draw = Draw()

# tooltip
tooltip = 'Click me to get Average Price!'

for i, row in df[["Lat", "Long", "Price"]].dropna().iterrows():
    position = (row["Lat"], row["Long"])
    popups = folium.Popup()
    folium.Marker(position , popup=row["Price"] , tooltip=tooltip , icon=folium.Icon(color='red', icon='cloud')).add_to(mainmap)


# adding the child in the main map
mainmap.add_child(minimap)
draw.add_to(mainmap)
mainmap.add_child(mc)
folium.TileLayer("stamentoner").add_to(mainmap)
folium.TileLayer("openstreetmap").add_to(mainmap)
folium.LayerControl().add_to(mainmap)

mainmap.save("/Users/sahilnagpal/PycharmProjects/BreakUpProject/whatsappchats.maps/samplemap.html")