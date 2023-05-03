from google.cloud.pubsublite.cloudpubsub import PublisherClient
from google.cloud.pubsublite.types import (
    CloudRegion,
    CloudZone,
    MessageMetadata,
    TopicPath,
)
from google.oauth2 import service_account

# TODO(developer):
project_number = 1050341177353
cloud_region = "us-central1"
zone_id = "a"
topic_id = "btc-trans"
regional = False


# Set the path to the service account key file
service_account_key_file = 'D:/Dataproc/btc-spark-streaming-5b497a9c7926.json'


credentials = service_account.Credentials.from_service_account_file(service_account_key_file)


if regional:
    location = CloudRegion(cloud_region)
else:
    location = CloudZone(CloudRegion(cloud_region), zone_id)

topic_path = TopicPath(project_number, location, topic_id)

# PublisherClient() must be used in a `with` block or have __enter__() called before use.
with PublisherClient(credentials=credentials) as publisher_client:
    data = "Hello world!"
    api_future = publisher_client.publish(topic_path, data.encode("utf-8"))
    # result() blocks. To resolve API futures asynchronously, use add_done_callback().
    message_id = api_future.result()
    message_metadata = MessageMetadata.decode(message_id)
    print(
        f"Published a message to {topic_path} with partition {message_metadata.partition.value} and offset {message_metadata.cursor.offset}."
    )

