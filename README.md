# Aidetic-Data-Engineer---Assessment-Pyspark-2024
**Assignment based on Pyspark**

**Tasks:**

1. Load the dataset into a PySpark DataFrame.
2. Convert the Date and Time columns into a timestamp column named Timestamp.
3. Filter the dataset to include only earthquakes with a magnitude greater than 5.0.
4. Calculate the average depth and magnitude of earthquakes for each earthquake type.
5. Implement a UDF to categorize the earthquakes into levels (e.g., Low, Moderate, High) based on their magnitudes.
6. Calculate the distance of each earthquake from a reference location (e.g., (0, 0)).
7. Visualize the geographical distribution of earthquakes on a world map using appropriate libraries (e.g., Basemap or Folium).
8. Please include the final csv in the repository.

**Dataset Definition:**
1. Use the earthquake dataset with the following columns:
2. Date (string): Date of the earthquake.
3. Time (string): Time of the earthquake.
4. Latitude (float): Latitude of the earthquake location.
5. Longitude (float): Longitude of the earthquake location.
6. Type (string): Type of earthquake.
7. Depth (float): Depth of the earthquake.
8. Magnitude (float): Magnitude of the earthquake.

# Importing rquired libraries

import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import to_timestamp, udf, col, concat
from pyspark.sql.types import StringType, FloatType
import math

sc = SparkContext.getOrCreate()
spar = SparkSession(sc)

1. Load the  dataset into a PySpark DataFrame.

data = spark.read.csv("database.csv", inferSchmea = True, header = True)

2.Convert the Date and Time columns into a timestamp column named Timestamp.

df = data.withColumn("Timestamp", to_timestamp(concat(col("Date"), col("Time")), "dd/MM/yyyyHH:mm:ss"))

3.Filter the dataset to include only earthquakes with a magnitude greater than 5.0. 

df_filter = df.filter(col("Magnitude")>5)
df_filter.select("Magnitude").show(5)

4.Calculate the average depth and magnitude of earthquakes for each earthquake type.


avg_depth_magnitude = df.groupby("Type").agg({"Depth": "avg", "Magnitude": "avg"})
avg_depth_magnitude.show()


5.Implement a UDF to categorize the earthquakes into levels (e.g., Low, Moderate, High) based on their magnitudes.

def categorize_magnitude(magnitude):
    if magnitude < 5.0:
        return "Low"
    elif magnitude >= 5.0 and magnitude < 6.0:
        return "Moderate"
    else:
        return "High"

categorize_magnitude_udf = udf(categorize_magnitude, StringType())
df = df.withColumn("Magnitude Level", categorize_magnitude_udf(df["Magnitude"]))
df.select("Magnitude Level", "Magnitude").show(5)

6.Calculate the distance of each earthquake from a reference location (e.g., (0, 0)).

ref_latitude = 0.0
ref_longitude = 0.0



def calculate_distance(lat, lon):
//Convert latitude and longitude from degrees to radians
    lat_rad = math.radians(lat)
    lon_rad = math.radians(lon)
    ref_lat_rad = math.radians(ref_latitude)
    ref_lon_rad = math.radians(ref_longitude)
    
//Haversine formula
    dlon = lon_rad - ref_lon_rad
    dlat = lat_rad - ref_lat_rad
    a = math.sin(dlat / 2)**2 + math.cos(lat_rad) * math.cos(ref_lat_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = 6371 * c  # Radius of Earth in kilometers
    
    return distance

calculate_distance_udf = udf(calculate_distance, FloatType())
df = df.withColumn("Distance from Reference", calculate_distance_udf(df["Latitude"], df["Longitude"]))
df.select("Distance from Reference").show(5)

import folium
map = folium.Map(location=[0, 0], zoom_start=1)
//Add markers for each earthquake

for row in data.collect():
    lat, lon, mag = row["Latitude"], row["Longitude"], row["Magnitude"]
    folium.CircleMarker(
        location=[lat, lon],
        radius=mag * 2,  # Adjust radius based on magnitude for better visualization
        color='red',
        fill=True,
        fill_color='red'
    ).add_to(map)

map
