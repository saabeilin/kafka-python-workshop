import time
from pprint import pprint

from confluent_kafka import avro
from confluent_kafka.cimpl import Message
from kafkian import Producer
from kafkian.serde.avroserdebase import AvroRecord
from kafkian.serde.serialization import AvroSerializer, SubjectNameStrategy

from kafkian_example import config

PRODUCER_CONFIG = {
    'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS
}


def delivery_success_callback(msg: Message):
    pprint({
        'topic': msg.topic(),
        'partition': msg.partition(),
        'timestamp': msg.timestamp(),
        'key': msg.key(),
        'value': msg.value(),
    })


producer = Producer(
    PRODUCER_CONFIG,
    delivery_success_callback=delivery_success_callback,
    key_serializer=AvroSerializer(config.SCHEMA_REGISTRY_URL,
                                  subject_name_strategy=SubjectNameStrategy.RecordNameStrategy),
    value_serializer=AvroSerializer(config.SCHEMA_REGISTRY_URL,
                                    subject_name_strategy=SubjectNameStrategy.RecordNameStrategy),
)

key_schema_str = """
{
    "namespace": "net.treqster.locations",
    "name": "LocationKey",
    "type": "record",
    "fields" : [
        {
            "name" : "userId",
            "type" : ["null", "string"]
        },
        {
            "name" : "deviceId",
            "type" : "string"
        }
    ]
}
"""

value_schema_str = """
{
   "namespace": "net.treqster.locations",
   "name": "LocationUpdated",
   "type": "record",
   "fields" : [
     {
       "name" : "userId",
       "type" : ["null", "string"]
     },
     {
       "name" : "deviceId",
       "type" : "string"
     },
     {
       "name" : "latitude",
       "type" : "float"
     },
     {
       "name" : "longitude",
       "type" : "float"
     },
     {
        "name": "time",
        "type": {
            "type": "long",
            "logicalType": "timestamp-millis"
        }
     },

     {
       "name" : "altitude",
       "type" : ["null", "float"],
       "default": null
     },
     {
       "name" : "bearing",
       "type" : ["null", "float"],
       "default": null
     },
     {
       "name" : "speed",
       "type" : ["null", "float"],
       "default": null
     },
     {
       "name" : "accuracy",
       "type" : ["null", "float"],
       "default": null
     },
     {
       "name" : "provider",
       "type" : ["null", "string"],
       "default": null
     }

   ]
}
"""


class LocationUpdated(AvroRecord):
    _schema = avro.loads(value_schema_str)


class LocationKey(AvroRecord):
    _schema = avro.loads(key_schema_str)


def produce_location_updated(location: dict):
    message = LocationUpdated(location)

    key = LocationKey(dict(
        userId=str(location['userId']),
        deviceId=location['deviceId']
    ))

    producer.produce('location.events', key, message, sync=True)


location = {
    'latitude': 59.91,
    'longitude': 30.25,
    'altitude': 10.1,
    'speed': 10.2,
    'bearing': 135.3,
    'accuracy': 0.45,
    'provider': 'glonass',
    'userId': 'some-user-id-1',
    'deviceId': 'yellow-submarine-1'
}

for i in range(10):
    location['latitude'] += 0.001
    location['longitude'] += 0.1
    location['time'] = int(time.time())
    produce_location_updated(location)
