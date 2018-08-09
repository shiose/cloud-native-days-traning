from google.cloud import storage
from google.cloud import pubsub_v1
import json
import os
import re

def gcs2pubsub(data, context):
	print('Bucket: {}'.format(data['bucket']))
	print('File: {}'.format(data['name']))

	project_id = os.environ['GCLOUD_PROJECT']
	print('ProjectID: {}'.format(project_id))

	# read from cloud storage by cloud functions' default service account
	storage_client = storage.Client()
	bucket = storage_client.bucket(data['bucket'])
	file = bucket.get_blob(data['name'])
	contents = file.download_as_string()

	print('Contents: {}'.format(contents))

    # convert from json string to object
	array = json.loads(contents)

    # initialize pubsub publisher
	publisher = pubsub_v1.PublisherClient()
	tp = 'projects/{project_id}/topics/{topic}'.format(
			project_id=project_id,
			topic='days-2018',  # Set this to something appropriate.
		)
	print('Topic: {}'.format(tp))

    # publish to pub/sub and convert data
	for game in array:
		location = game["location"]
		fifa_id = game["fifa_id"]

		team = game["home_team"]
		country = team["country"]
		for ev in game["home_team_events"]:
			time = re.search("\\d+", ev["time"]).group(0)
			dest = json.dumps({
				"location": location,
				"fifa_id": fifa_id,
				"country": country,
				"type_of_event": ev["type_of_event"],
				"player": ev["player"],
				"time": int(time)
			})
			print("Publish: " + dest)
			publisher.publish(tp, dest.encode('utf-8'))

		team = game["away_team"]
		country = team["country"]
		for ev in game["away_team_events"]:
			time = re.search("\\d+", ev["time"]).group(0)
			dest = json.dumps({
				"location": location,
				"fifa_id": fifa_id,
				"country": country,
				"type_of_event": ev["type_of_event"],
				"player": ev["player"],
				"time": int(time)
			})
			print("Publish: " + dest)
			publisher.publish(tp, dest.encode('utf-8'))
