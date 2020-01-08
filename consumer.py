#!/usr/bin/env python

from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
from pyspark import SparkContext

import sys, json


if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.stderr.write('Usage: %s <bootstrap-brokers> <topic>\n' % sys.argv[0])
        sys.exit(1)

    broker = sys.argv[1]
    topic = sys.argv[2]

    sc = SparkContext(appName="StreamingConsumer")
    ssc = StreamingContext(sc, batchDuration=1)

    rdd = KafkaUtils.createStream(ssc, groupId='0', topics={topic:0}, kafkaParams={'bootstrap.servers': broker})
    rdd.pprint()

    ssc.start()
    ssc.awaitTermination()

