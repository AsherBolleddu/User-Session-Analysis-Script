# Imports
import json
from collections import defaultdict

import requests

import config


# A sorting key that sorts the events by visitorId and time
def sorting_key(event):
    visitor = event["visitorId"]
    time = event["timestamp"]
    return visitor, time


# Headers for the API request, as well as the JSON format to send the data
headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {config.API_KEY}'
}

# Get the data from the sourceURL
response = requests.get(config.sourceUrl)

# Parse the JSON data into a dictionary
data = response.json()

# Sort the events by visitorId, then the timestamp
events = sorted(data["events"], key=sorting_key)

# Create a dictionary to store sessions by user using defaultdict (this will default to an empty list for any key
# that hasn't been set yet). If visitor is a key that hasn't been added to sessionsByUser yet, the defaultdict will
# automatically assign it an empty list value. This eliminates the need for a check
sessionsByUser = defaultdict(list)

# Process each event to group them into sessions
for event in events:
    visitor = event["visitorId"]

    # Check if the current visitor has no sessions, or if the time gap between the current and last event of the last
    # session is greater than ten minutes. If it is start a new session
    if not sessionsByUser[visitor] or event["timestamp"] - sessionsByUser[visitor][-1]["endTime"] > config.TEN_MINUTES:
        sessionsByUser[visitor].append({
            "duration": 0,
            "pages": [],
            "startTime": event["timestamp"],
            # Temporary end time of session, will be updated and deleted. Used to calculate duration
            "endTime": event["timestamp"]
        })

    # Add the URL to the pages list of the current session
    sessionsByUser[visitor][-1]["pages"].append(event["url"])

    # Update the end time of the current session
    sessionsByUser[visitor][-1]["endTime"] = event["timestamp"]

# Calculate the duration of the session
for visitor, sessions in sessionsByUser.items():
    for session in sessions:
        session["duration"] = session["endTime"] - session["startTime"]
        del session["endTime"]

# Convert the data into JSON format
jsonData = json.dumps({"sessionsByUser": sessionsByUser}, indent=4)

# Send the JSON data to the target URL
response = requests.post(config.targetUrl, data=jsonData, headers=headers)
