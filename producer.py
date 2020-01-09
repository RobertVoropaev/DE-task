#!/usr/bin/env python

import sys, random, scipy.stats, json, time
from pykafka import KafkaClient

if __name__ == '__main__':
    client = KafkaClient('localhost:9092')
    topic = client.topics['test']
    p = topic.get_sync_producer(delivery_reports=True)

    d = dict()
    frequency = 100

    color = ["red", "blue", "green", "yellow"]
    shape = ["triangle", "circle", "rectangle"]
    lognorm = scipy.stats.lognorm(1)

    try:
        while True:
            d.clear()

            d["color"] = color[random.randint(0, 3)]
            d["shape"] = shape[random.randint(0, 2)]
            d["size"] = lognorm.rvs(1)[0]

            msg_str = json.dumps(d)

            p.produce(bytes(msg_str, encoding="utf-8"), partition_key=b'0')

            time.sleep(1./frequency)

    except KeyboardInterrupt:
        sys.stderr.write('[Aborted by user]\n')
    finally:
        p.stop()
