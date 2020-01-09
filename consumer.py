#!/usr/bin/env python

from pykafka import KafkaClient
import sys, json


if __name__ == '__main__':
    client = KafkaClient('localhost:9092')
    topic = client.topics['test']

    try:
        while True:
            msg = c.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())
            else:
                sys.stderr.write('Message received from %s[%d] with offset %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))
                print(msg.value())

    except KeyboardInterrupt:
        sys.stderr.write('[Aborted by user]\n')

    finally:
        c.close()