from pykafka import KafkaClient
import sys
import random
import scipy.stats
import json
import time

class Data:
    def __init__(self):
        self.color = ["red", "blue", "green", "yellow"]
        self.shape = ["triangle", "circle", "rectangle"]
        self.lognorm = scipy.stats.lognorm(1)

    def generate_dict(self):
        d = dict()
        d["color"] = self.color[random.randint(0, 3)]
        d["shape"] = self.shape[random.randint(0, 2)]
        d["size"] = self.lognorm.rvs(1)[0]
        return d


if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.stderr.write('Usage: %s <bootstrap-server> <topic>\n' % sys.argv[0])
        sys.exit(1)

    kafka_host = sys.argv[1]
    kafka_topic =topic = sys.argv[2]
    
    client = KafkaClient(kafka_host)
    topic = client.topics[kafka_topic]

    with topic.get_sync_producer(delivery_reports=True) as producer:
        data = Data()
        frequency = 100 #частота отправки сообщений
        try:
            while True:
                d = data.generate_dict()
                msg_str = json.dumps(d)

                report = producer.produce(bytes(msg_str, encoding="utf-8"))
                print("Message sent to partition {} with offset {}" \
                                .format(report.partition_id, report.offset))
    
                time.sleep(1./frequency)
        except KeyboardInterrupt:
            sys.stderr.write('[Aborted by user]\n')
        finally:
            producer.stop()
