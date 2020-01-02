#!/usr/bin/env python

from confluent_kafka import Consumer, KafkaException
import sys, json


if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.stderr.write('Usage: %s <bootstrap-brokers> <topic>\n' % sys.argv[0])
        sys.exit(1)

    broker = sys.argv[1]
    topic = sys.argv[2]

    conf = {
        'bootstrap.servers': broker,
        'group.id': 0,
    }

    c = Consumer(conf)

    c.subscribe([topic])

    try:
        while True:
            msg = c.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())
            else:
                sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                                 (msg.topic(), msg.partition(), msg.offset(),
                                  str(msg.key())))
                print(msg.value())

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        c.close()