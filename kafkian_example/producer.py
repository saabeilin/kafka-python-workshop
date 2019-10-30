from pprint import pprint

from confluent_kafka.cimpl import Message
from kafkian import Producer

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
    delivery_success_callback=delivery_success_callback
)
