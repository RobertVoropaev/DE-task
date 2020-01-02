#!/usr/bin/env python

from confluent_kafka import Producer
import sys, random, scipy.stats, json, time


if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.stderr.write('Usage: %s <bootstrap-brokers> <topic>\n' % sys.argv[0])
        sys.exit(1)

    broker = sys.argv[1]
    topic = sys.argv[2]

    conf = {'bootstrap.servers': broker}

    p = Producer(conf)

    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))

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

            p.produce(topic=topic, value=msg_str, partition=0, callback=delivery_callback)

            time.sleep(1./frequency)
            p.poll(0)

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        sys.stderr.write('Waiting for %d deliveries\n' % len(p))
        p.flush()
