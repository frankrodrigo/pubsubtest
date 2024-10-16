from flask import Flask, jsonify
from google.cloud import pubsub_v1
import pyodbc

app = Flask(__name__)

# Pub/Sub Configuration
project_id = "cbd3354-435500"
subscription_id = "my-sub"

# SQL Server Configuration
server = '162.222.181.70'
database = 'db1'
username = 'dbuser1'
password = 'dbuser1'
driver = '{ODBC Driver 17 for SQL Server}'


# Connect to SQL Server
def connect_to_db():
    connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    return pyodbc.connect(connection_string)


# Function to process Pub/Sub messages
def callback(message):
    print(f"Received message: {message.data}")

    try:
        # Decode message
        message_data = message.data.decode('utf-8')

        # Insert message into SQL Server database
        connection = connect_to_db()
        cursor = connection.cursor()

        # Query to insert message into the pubsub_messages table
        query = "INSERT INTO pubsub_messages (message_body) VALUES (?)"
        cursor.execute(query, message_data)
        connection.commit()

        print("Message inserted into the database")
        message.ack()  # Acknowledge the message after processing

    except Exception as e:
        print(f"Failed to insert into database: {e}")
        message.nack()  # If there’s an issue, don’t acknowledge the message


# Route to start Pub/Sub subscription
@app.route('/start-listening', methods=['GET'])
def start_listening():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    # Subscribe to the topic and call the callback function for each message
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...")

    try:
        streaming_pull_future.result(timeout=300)  # Listen for messages for 30 seconds
    except TimeoutError:
        streaming_pull_future.cancel()  # Stop listening after timeout

    return jsonify({"status": "Listening to Pub/Sub messages"})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
