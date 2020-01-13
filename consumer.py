from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import pyspark_cassandra

from datetime import datetime
import scipy.stats as st
import numpy as np
import sys
import json


def createContext(kafka_host, kafka_topic,
                  cassandra_host, cassandra_port, checkpoint):

    conf = SparkConf().setAppName("StreamingConsumer") \
                      .setMaster("local[*]") \
                      .set("spark.cassandra.connection.host", cassandra_host) \
                      .set("spark.cassandra.connection.port", cassandra_port) 
    
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    ssc = StreamingContext(sc, batchDuration=10)
    ssc.checkpoint(checkpoint)

    dStream = KafkaUtils.createDirectStream(ssc, [kafka_topic],
                            kafkaParams={'metadata.broker.list': kafka_host})
    
    procDStream = rddProcessing(dStream)
    procDStream.pprint()
    
    procDStream.foreachRDD(lambda rdd: rdd.saveToCassandra(keyspace="spark",
                                                           table="data")

    return ssc


def rddProcessing(rdd):
    rdd_dict = rdd.map(lambda data: json.loads(data[1]))

    shape_color = rdd_dict.map(lambda data: (data['shape'], data['color'])) \
                          .groupByKey().mapValues(list) \
                          .map(lambda data: (data[0], st.mode(data[1]).mode[0]))

    shape_size = rdd_dict.map(lambda data: (data['shape'], data['size'])) \
                         .groupByKey().mapValues(list) \
                         .map(lambda data: (data[0], np.quantile(data[1], 0.1)))
    
    rdd_join = shape_color.join(shape_size) \
                          .map(lambda data: (datetime.now(), data[0],
                                             str(data[1][0]), float(data[1][1])))

    return rdd_join
   

if __name__ == '__main__':
    if len(sys.argv) != 5:
        sys.stderr.write("Usage: %s <kafka_host:port> <kafka_topic> " 
                         "<cassandra_host:port> <checkpoint_dir> \n" % sys.argv[0])
        sys.exit(1)

    kafka_host = sys.argv[1]
    kafka_topic = sys.argv[2]

    cassandra_arg =  sys.argv[3].split(":")
    cassandra_host = cassandra_arg[0]
    cassandra_port = cassandra_arg[1]
    
    checkpoint = sys.argv[4]

    ssc = StreamingContext.getOrCreate(checkpoint,
                          lambda: createContext(kafka_host, kafka_topic,
                                    cassandra_host, cassandra_port, checkpoint))

    ssc.start()
    try:
        ssc.awaitTermination()
    except KeyboardInterrupt:
        sys.stderr.write('[Aborted by user]\n')
    finally:
        ssc.stop(stopSparkContext=True, stopGraceFully=True)

