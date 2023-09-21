import argparse
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime



# Configure Cassandra connection
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(contact_points=['127.0.0.1'], auth_provider=auth_provider)
session = cluster.connect('mysql_cassandra')

API_KEY = 'JEHC652DZTIXYQ2E'
API_SECRET_KEY = 'YaV3ndjXUe7o5WBFs9weoJvzdbPUqfkwb5oipqTN2YotY3KkztaU6vT3ShaEOulp'
BOOTSTRAP_SERVER = 'pkc-xrnwx.asia-south2.gcp.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
ENDPOINT_SCHEMA_URL = 'https://psrc-3w372.australia-southeast1.gcp.confluent.cloud'
SCHEMA_REGISTRY_API_KEY = '6DMM4W3BZ434JUB5'
SCHEMA_REGISTRY_API_SECRET = '4O3E7Ypg/CuJmtO3mKaG90eH33nBar+2l5nB1TjxqJglXpPz6mU5XJJuh+bMkt38'


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
        
        
def main(topic):
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    
    # subjects = schema_registry_client.get_subjects()
    # print(subjects)
    subject = topic+'-value'

    schema = schema_registry_client.get_latest_version(subject)
    schema_str=schema.schema.schema_str

    json_deserializer = JSONDeserializer(schema_str)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "latest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            raw_data = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if raw_data is not None:
                print("User record {}: Value: {}\n".format(msg.key(), raw_data))
                query = f"""INSERT INTO dump_from_mysql (A, B, C, CREATED_AT, D, E, UPDATED_AT) 
                    VALUES ('{raw_data['A']}', '{raw_data['B']}', '{raw_data['C']}', '{raw_data['CREATED_AT']}', '{raw_data['D']}', '{raw_data['E']}', '{raw_data['UPDATED_AT']}')"""
                session.execute(query)
                #consumer.commit(asynchronous=False)
        except KeyboardInterrupt:
            break
    consumer.close()

main("mysql_cassandra")

cluster.shutdown()
