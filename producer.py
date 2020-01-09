from pykafka import KafkaClient
import sys
import random
import scipy.stats
import json
import time

color = ["red", "blue", "green", "yellow"]
shape = ["triangle", "circle", "rectangle"]
lognorm = scipy.stats.lognorm(1)


def generate_dict():
    d = dict()
    d["color"] = color[random.randint(0, 3)]
    d["shape"] = shape[random.randint(0, 2)]
    d["size"] = lognorm.rvs(1)[0]
    return d


if __name__ == '__main__':
    client = KafkaClient('localhost:9092')
    topic = client.topics['test']

    with topic.get_sync_producer() as producer:
        frequency = 100
        try:
            while True:
                d = generate_dict()
                msg_str = json.dumps(d)

                producer.produce(bytes(msg_str, encoding="utf-8"),
                                 partition_key=b'0')
                
                time.sleep(1./frequency)
        except KeyboardInterrupt:
            sys.stderr.write('[Aborted by user]\n')
        finally:
            producer.stop()
