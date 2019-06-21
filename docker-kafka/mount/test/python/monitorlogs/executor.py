#!/usr/bin/env python

from kafka import KafkaProducer
import json
from watcher import Watcher
from watchdog.observers import Observer


producer_kafka = KafkaProducer(bootstrap_servers = 'kafka-1:9092',value_serializer = lambda m: json.dumps(m).encode('ascii'))
kafka_topic1 = 'json_topic_big'
kafka_topic2 = 'json_topic_small'

if __name__ == '__main__':
    w = Watcher(Observer(), "/test/python", kafka_topic1, kafka_topic2, producer_kafka)
    w.run()
