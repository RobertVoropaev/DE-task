from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import sys, json

if __name__ == '__main__':
    broker = "localhost:9092"
    topic = "test"
    
    sc = SparkContext(appName="StreamingConsumer", master="local")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, batchDuration=1)

    rdd = KafkaUtils.createDirectStream(ssc, [topic],
                                        kafkaParams={'metadata.broker.list': broker})
    rdd.pprint()

    ssc.start()
    ssc.awaitTermination()

    ssc.stop(stopSparkContext=True, stopGraceFully=True)

