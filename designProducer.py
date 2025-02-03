from google.cloud import pubsub_v1
import glob
import json
import os
import time
import csv

# Search for the JSON service account key and set the credentials
files = glob.glob("*.json")
if not files:
    raise FileNotFoundError("No JSON key file found. Make sure your service account key is in the directory.")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Set the project_id and topic_name
project_id = "phrasal-bonus-449202-e8"
topic_name = "design2-csvFiles"

# Create a publisher and get the topic path
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Publishing messages to {topic_path}...")

# Read CSV and publish messages
keysSaved = False
keys = []
row_id = 1  # Start ID at 1

with open("Labels.csv", newline="") as csvfile:
    reader = csv.reader(csvfile, delimiter=",", quotechar="|")
    for row in reader:

        # Save the keys (header row)
        if not keysSaved:
            keys = row  # Store the column headers
            keysSaved = True
            continue

        # Create a dictionary mapping column names to values
        rowDict = dict(zip(keys, row))

        # Convert values to match SQL table data types and add ID
        formatted_data = {
            "ID": row_id,  # Auto-generated unique ID
            "timestamp": int(rowDict["Timestamp"]),
            "car1x": float(rowDict["Car1_Location_X"]),
            "car1y": int(rowDict["Car1_Location_Y"]),
            "car1z": float(rowDict["Car1_Location_Z"]),
            "car2x": float(rowDict["Car2_Location_X"]),
            "car2y": int(rowDict["Car2_Location_Y"]),
            "car2z": float(rowDict["Car2_Location_Z"]),
            "imgview": rowDict["Occluded_Image_view"],
            "carview": rowDict["Occluding_Car_view"],
            "groundview": rowDict["Ground_Truth_View"],
            "pedxtopleft": int(rowDict["pedestrianLocationX_TopLeft"]),
            "pedytopleft": int(rowDict["pedestrianLocationY_TopLeft"]),
            "pedxbottomright": int(rowDict["pedestrianLocationX_BottomRight"]),
            "pedybottomright": int(rowDict["pedestrianLocationY_BottomRight"]),
        }

        # **Double-encoded JSON format**
        message = json.dumps(formatted_data).encode("utf-8")

        try:
            future = publisher.publish(topic_path, message)
            future.result()  # Ensure successful publishing
            print(f"Published successfully: {formatted_data}")
        except Exception as e:
            print(f"Failed to publish: {e}")

        row_id += 1  # Increment ID for next row

    time.sleep(0.5)  # Wait for 0.5 seconds
    print("All messages have been published successfully!")
