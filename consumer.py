from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import sys, json
import scipy.stats
import numpy as np
from datetime import datetime

if __name__ == '__main__':
    broker = "localhost:9092"
    topic = "test"
    
    sc = SparkContext(appName="StreamingConsumer", master="local")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, batchDuration=1)

    rdd = KafkaUtils.createDirectStream(ssc, [topic],
                                        kafkaParams={'metadata.broker.list': broker,
                                        'startingOffsets':'earliest'})
    rdd = rdd.map(lambda data: data[1])
    rdd = rdd.map(lambda data: json.loads(data))

    rdd1 = rdd.map(lambda data: (data['shape'], data['color'])) \
                .groupByKey().mapValues(list) \
                .map(lambda data: (data[0], scipy.stats.mode(data[1]).mode[0]))
    

    rdd2 = rdd.map(lambda data: (data['shape'], data['size'])) \
                .groupByKey().mapValues(list) \
                .map(lambda data: (data[0], np.quantile(data[1], 0.1)))
    
    rdd3 = rdd1.join(rdd2) \
                .map(lambda data: (datetime.now().strftime("%y-%m-%d %H:%M:%S"), \
                                    data[0], data[1][0], data[1][1])) 
    


    rdd3.pprint()


    ssc.start()
    ssc.awaitTermination()

    ssc.stop(stopSparkContext=True, stopGraceFully=True)

