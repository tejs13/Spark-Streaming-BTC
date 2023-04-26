import time

from google.cloud import pubsub_v1
import json

# Set the project ID and topic name
project_id = "btc-spark-streaming"
topic_name = "projects/btc-spark-streaming/topics/btc-transactions"


# Set the path to the service account key file
service_account_key_file = 'D:/Dataproc/btc-spark-streaming-5b497a9c7926.json'

# Create a Publisher client instance
publisher = pubsub_v1.PublisherClient.from_service_account_file(service_account_key_file)


# Define the message payload source (in this example, a list of messages)
messages = [
    {'name': 'John', 'age': 30, 'city': 'New York'},
    {'name': 'Jane', 'age': 25, 'city': 'San Francisco'},
    {'name': 'Bob', 'age': 40, 'city': 'Chicago'}
]
# Continuously publish messages to the topic
while True:
    for message in messages:
        payload = json.dumps(message).encode('utf-8')
        future = publisher.publish(topic_name, data=payload)
        result = future.result()
        print('Published message ID:', result)
    time.sleep(5)  # Wait 5 seconds before publishing the next batch of messages
