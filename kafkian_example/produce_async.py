import time
import uuid

from kafkian_example.producer import producer

try:
    while True:
        print(producer.produce(
            topic="test",
            key=str(uuid.uuid4()),
            value="test",
            sync=False
        ))
        time.sleep(1)
except:
    producer.flush()
