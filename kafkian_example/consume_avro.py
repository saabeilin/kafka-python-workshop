import logging
from pprint import pprint

from kafkian import Consumer
from kafkian.message import Message
from kafkian.serde.deserialization import AvroDeserializer

from kafkian_example import config

logger = logging.getLogger(__name__)

CONSUMER_CONFIG = {
    'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS,
    'auto.offset.reset': 'earliest',
    'group.id': 'debug'
}


def repr_message(message: Message):
    return {
        'topic': message.topic,
        'partition': message.partition,
        'timestamp': message.timestamp,
        'key': message.key,
        'key_class': message.key.schema.fullname,
        'value': message.value,
        'value_class': message.value.schema.fullname
    }


def handle(message: Message):
    message_rep = repr_message(message)
    logger.info("Received message", extra=message_rep)
    pprint(message_rep)

    if message.value.schema.fullname == 'net.treqster.locations.LocationUpdated':
        # handle_location_updated(message.value)
        pass
    else:
        logger.warning("Message not handled", extra=message_rep)


def handler():
    consumer = Consumer(
        CONSUMER_CONFIG,
        topics=['location.events'],
        key_deserializer=AvroDeserializer(schema_registry_url=config.SCHEMA_REGISTRY_URL),
        value_deserializer=AvroDeserializer(schema_registry_url=config.SCHEMA_REGISTRY_URL),
    )

    for message in consumer:
        handle(message)
        consumer.commit()


if __name__ == '__main__':
    handler()
