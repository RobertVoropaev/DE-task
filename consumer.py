from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SparkSession

from datetime import datetime
import sys
import json
import scipy.stats
import numpy as np


def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']


def createContext(host, topic, checkpoint):
    conf = SparkConf().setAppName("StreamingConsumer").setMaster("local[*]")
    
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    

    ssc = StreamingContext(sc, batchDuration=10)
    ssc.checkpoint(checkpoint)

    dStream = KafkaUtils.createDirectStream(ssc, [topic],
                                        kafkaParams={'metadata.broker.list': host})
    #TdStream.pprint()
    dStream.foreachRDD(process)

    return ssc


def process(rdd):
    print("=========== s ===========")
    if rdd.isEmpty():
        return
    rdd3 = rddProcessing(rdd)

    spark = getSparkSessionInstance(rdd.context.getConf())
    rowRdd = rdd3.map(lambda row: Row(shape=row[0], color=row[1], quantile=row[2]))
    
    df = spark.createDataFrame(rowRdd)
    df.createOrReplaceTempView("DataTable")
    dfs = spark.sql('select * from DataTable')
    dfs.show()



def rddProcessing(rdd):
    rdd = rdd.map(lambda data: data[1])
    rdd = rdd.map(lambda data: json.loads(data))

    rdd1 = rdd.map(lambda data: (data['shape'], data['color'])) \
                .groupByKey().mapValues(list) \
                .map(lambda data: (data[0], scipy.stats.mode(data[1]).mode[0], ))

    rdd2 = rdd.map(lambda data: (data['shape'], data['size'])) \
                .groupByKey().mapValues(list) \
                .map(lambda data: (data[0], np.quantile(data[1], 0.1)))
    
    rdd3 = rdd1.join(rdd2) \
                .map(lambda data: (data[0], str(data[1][0]), float(data[1][1]))) 
    return rdd3
   

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

