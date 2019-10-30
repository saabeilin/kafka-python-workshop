from pprint import pprint

from kafkian_example.consumer import consumer, repr_message

try:
    for message in consumer:
        pprint(repr_message(message))
except KeyboardInterrupt as e:
    pprint(consumer.commit(True))
