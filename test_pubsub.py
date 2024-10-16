from google.cloud import pubsub_v1

# Initialize Pub/Sub client using service account
publisher = pubsub_v1.PublisherClient()

# Your Google Cloud project ID
project_id = "cbd3354-435500"

# Get the list of topics in the project
project_path = f"projects/{project_id}"

print("Listing Pub/Sub topics:")

# List topics
for topic in publisher.list_topics(request={"project": project_path}):
    print(topic.name)
