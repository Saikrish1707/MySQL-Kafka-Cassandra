import argparse
from uuid import uuid4
from six.moves import input
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
#from confluent_kafka.schema_registry import *
import pandas as pd
from typing import List
import mysql.connector
import time
from datetime import datetime


# Define the Kafka producer configuration
API_KEY = 'JEHC652DZTIXYQ2E'
API_SECRET_KEY = 'YaV3ndjXUe7o5WBFs9weoJvzdbPUqfkwb5oipqTN2YotY3KkztaU6vT3ShaEOulp'
BOOTSTRAP_SERVER = 'pkc-xrnwx.asia-south2.gcp.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
ENDPOINT_SCHEMA_URL = 'https://psrc-3w372.australia-southeast1.gcp.confluent.cloud'
SCHEMA_REGISTRY_API_KEY = '6DMM4W3BZ434JUB5'
SCHEMA_REGISTRY_API_SECRET = '4O3E7Ypg/CuJmtO3mKaG90eH33nBar+2l5nB1TjxqJglXpPz6mU5XJJuh+bMkt38'


# Connect to MySQL database
mysql_conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="7066",
    database="kafka_demo"
)


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf
    
def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }
    
class random_data:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        self.record=record
   
    @staticmethod
    def dict_to_random_data(data:dict,ctx):
        return random_data(record=data)

    def __str__(self):
        return f"{self.record}"
        
def random_to_dict(random:random_data, ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """
    # User._address must not be serialized; omit from dict
    return random_data.record
    
    
def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))



def main(topic):
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    # subjects = schema_registry_client.get_subjects()
    # print(subjects)
    subject = topic+'-value'

    schema = schema_registry_client.get_latest_version(subject)
    schema_str=schema.schema.schema_str

    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client)
    
    # Define a variable to store the last processed timestamp
    last_processed_timestamp = None

    producer = Producer(sasl_conf())

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    #while True:
        # Serve on_delivery callbacks from previous calls to produce()
    producer.poll(0.0)

    try:
        while True:
            # Get cursor to execute MySQL queries
            mysql_cursor = mysql_conn.cursor(dictionary=True)
    
            # Build the MySQL query to fetch new/updated records since the last checkpoint
            if last_processed_timestamp:
                mysql_query = f"""SELECT * FROM DAILY_LOAD WHERE UPDATED_AT > '{last_processed_timestamp}'"""
            else:
                mysql_query = """SELECT * FROM DAILY_LOAD"""
    
            # Execute MySQL query to fetch data
            mysql_cursor.execute(mysql_query,)
    
            # Fetch all rows from MySQL result set
            mysql_rows = mysql_cursor.fetchall()
    
            for row in mysql_rows:
                print(row)
                # Extract the timestamp from the current row
                updated_timestamp = row['UPDATED_AT']  # Adjust this based on your column name
                created_timestamp = row['CREATED_AT']
                updated_datetime = updated_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                created_datetime = created_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                row['UPDATED_AT'] = updated_datetime
                row['CREATED_AT'] = created_datetime
                # Convert row to a comma-separated string
                kafka_msg = ','.join(map(str, row))
    
                # Produce the Kafka message to the topic
                producer.produce(topic=topic,
                            key=string_serializer(str(uuid4())),
                            value=json_serializer(row, SerializationContext(topic,MessageField.VALUE)),
                            on_delivery=delivery_report)
    
                # Update the last processed timestamp to the latest value
                if not last_processed_timestamp or updated_datetime > last_processed_timestamp:
                    last_processed_timestamp = updated_datetime
    
            # Flush records after producing all messages in this batch
            producer.flush()
    
            # Sleep for a while to avoid constant polling
            time.sleep(60)  # Sleep for 60 seconds (adjust as needed)

    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass

    print("\nFlushing records...")
    producer.flush()
 
main("mysql_cassandra")