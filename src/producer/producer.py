import time
import json
import pandas as pd
import geopandas as gpd
from confluent_kafka import Producer

PARQUET_FILE = "data/yellow_tripdata_2024-01.parquet"
SHAPEFILE = "data/taxi_zones/taxi_zones.shp"
KAFKA_BROKER = "localhost:19092"
TOPIC_NAME = "ride_requests"
SPEED_MULTIPLIER = 100  # 100x speed (1 hour of real time = 36 seconds)

# Load and Map the Geographic Zones
print("Loading shapefile and computing centroids")
gdf = gpd.read_file(SHAPEFILE)
centroids = gdf.geometry.centroid
centroids_gps = centroids.to_crs(epsg=4326)
gdf['longitude'] = centroids_gps.x
gdf['latitude'] = centroids_gps.y
zone_lookup = gdf.set_index('LocationID')[['latitude', 'longitude']].to_dict('index')

# Load and Clean the Parquet Data
print("Loading and cleaning Parquet data")
df = pd.read_parquet(PARQUET_FILE).head(100000) # read a chunk for the stream 

# Apply cleaning like in notebook
df['passenger_count'] = df['passenger_count'].fillna(1)
df = df[(df['tpep_pickup_datetime'] >= '2024-01-01') & (df['tpep_pickup_datetime'] < '2024-02-01')]
df = df[(df['trip_distance'] > 0) & (df['fare_amount'] >= 0)]
df = df[df['tpep_dropoff_datetime'] > df['tpep_pickup_datetime']]

# Sort strictly by pickup time so the stream flows chronologically
df = df.sort_values(by="tpep_pickup_datetime")
print(f"ready to stream {len(df)}")

# Kafka producer setup 
producer = Producer({
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'ride-hailing-producer',
    'linger.ms': 10, # Wait up to 10ms to batch messages together for higher throughput
})

def delivery_report(err, msg):
    """Callback triggered upon message delivery success or failure."""
    if err is not None:
        print(f"Delivery failed: {err}")

# Stream the data
print(f"starting stream at {SPEED_MULTIPLIER}x speed")
previous_time = None
messages_sent = 0

for index, row in df.iterrows():
    pickup_time = row['tpep_pickup_datetime']
    
    # Pace the loop to simulate real-time flow
    if previous_time is not None:
        time_diff = (pickup_time - previous_time).total_seconds()
        if time_diff > 0:
            time.sleep(time_diff / SPEED_MULTIPLIER)
            
    previous_time = pickup_time

    pu_zone = row['PULocationID']
    do_zone = row['DOLocationID']
    
    # skip if the zone ID is invalid
    if pu_zone not in zone_lookup or do_zone not in zone_lookup:
        continue

    # build the JSON payload
    event = {
        "vendor_id": int(row['VendorID']),
        "pickup_datetime": pickup_time.isoformat(), # 24/7 change into datetime.utcnow().isoformat()
        "pickup_lat": zone_lookup[pu_zone]['latitude'],
        "pickup_lon": zone_lookup[pu_zone]['longitude'],
        "dropoff_lat": zone_lookup[do_zone]['latitude'],
        "dropoff_lon": zone_lookup[do_zone]['longitude'],
        "passenger_count": int(row['passenger_count']),
        "trip_distance": float(row['trip_distance']),
        "total_amount": float(row['total_amount'])
    }
    
    # Produce the event
    producer.produce(
        TOPIC_NAME, 
        key=str(pu_zone).encode('utf-8'), 
        value=json.dumps(event).encode('utf-8'),
        callback=delivery_report
    )
    
    # poll handles the delivery callbacks
    producer.poll(0)
    
    messages_sent += 1
    if messages_sent % 1000 == 0:
        print(f"Sent {messages_sent} events. current stream time: {pickup_time}")

# Wait for any outstanding messages to be delivered before exiting
producer.flush()
print("stream finished")