import uuid
from pprint import pprint

from kafkian import Consumer
from kafkian.message import Message

from kafkian_example import config

CONSUMER_CONFIG = {
    'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS,
    'default.topic.config': {
        'auto.offset.reset': 'earliest',
    },
    'group.id': str(uuid.uuid4())
}

consumer = Consumer(
    CONSUMER_CONFIG,
    topics=["test"],
)


def repr_message(message: Message):
    return {
        'topic': message.topic,
        'partition': message.partition,
        'timestamp': message.timestamp,
        'key': message.key,
        'value': message.value,
    }
