from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


from datetime import datetime
import sys
import json
import scipy.stats
import numpy as np


def createContext(host, topic, checkpoint):
    conf = SparkConf().setAppName("StreamingConsumer").setMaster("local[*]")
    
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    
    ssc = StreamingContext(sc, batchDuration=1)
    ssc.checkpoint(checkpoint)

    rdd = KafkaUtils.createDirectStream(ssc, [topic],
                                        kafkaParams={'metadata.broker.list': host})
    rdd3 = rddProcessing(rdd)
    rdd3.pprint()
    
    return ssc


def rddProcessing(rdd):
    rdd = rdd.map(lambda data: data[1])
    rdd = rdd.map(lambda data: json.loads(data))

    rdd1 = rdd.map(lambda data: (data['shape'], data['color'])) \
                .groupByKey().mapValues(list) \
                .map(lambda data: (data[0], scipy.stats.mode(data[1]).mode[0], len(data[1])))

    rdd2 = rdd.map(lambda data: (data['shape'], data['size'])) \
                .groupByKey().mapValues(list) \
                .map(lambda data: (data[0], np.quantile(data[1], 0.1)))
    
    rdd3 = rdd1.join(rdd2) \
                .map(lambda data: (datetime.now().strftime("%y-%m-%d %H:%M:%S"), \
                                    data[0], data[1][0], data[1][1])) 
    return rdd1
   

if __name__ == '__main__':
    host = "localhost:9092"
    topic = "test"
    checkpoint = "checkpoint/checkpoint0"

    ssc = StreamingContext.getOrCreate(checkpoint,
                                    lambda: createContext(host, topic, checkpoint))
    ssc.start()
    try:
        ssc.awaitTermination()
    except KeyboardInterrupt:
        sys.stderr.write('[Aborted by user]\n')
    finally:
        ssc.stop(stopSparkContext=True, stopGraceFully=True)

